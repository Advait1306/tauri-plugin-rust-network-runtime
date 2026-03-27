use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use futures_util::StreamExt;
use reqwest::{
  header::{HeaderMap, HeaderName, HeaderValue, ACCEPT},
  redirect::Policy,
  Client,
};
use tauri::async_runtime;
use tokio::sync::mpsc;
use tokio::time::sleep;
use url::Url;

use crate::{
  cookies::CookieStoreHandle,
  models::{
    EventSourceConnectRequest, EventSourceConnectResponse, EventSourceEvent, EventSourceEventKind,
    HeaderEntry,
  },
  Error, Result,
};

#[derive(Clone)]
pub struct EventSourceClient {
  client: Client,
  cookies: CookieStoreHandle,
  next_id: Arc<AtomicU64>,
  connections: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<EventSourceControl>>>>,
}

impl EventSourceClient {
  pub fn new(cookies: CookieStoreHandle) -> Result<Self> {
    let client = Client::builder()
      .cookie_provider(cookies.provider())
      .redirect(Policy::limited(10))
      .build()?;

    Ok(Self {
      client,
      cookies,
      next_id: Arc::new(AtomicU64::new(1)),
      connections: Arc::new(Mutex::new(HashMap::new())),
    })
  }

  pub async fn connect(
    &self,
    request: EventSourceConnectRequest,
    sink: Option<Arc<dyn Fn(EventSourceEvent) + Send + Sync>>,
  ) -> Result<EventSourceConnectResponse> {
    let source_id = self.next_id.fetch_add(1, Ordering::Relaxed);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    self
      .connections
      .lock()
      .expect("event source registry poisoned")
      .insert(source_id, control_tx);

    let client = self.client.clone();
    let cookies = self.cookies.clone();
    let connections = self.connections.clone();
    async_runtime::spawn(run_event_source(
      source_id,
      client,
      cookies,
      request,
      control_rx,
      sink,
      connections,
    ));

    Ok(EventSourceConnectResponse { source_id })
  }

  pub fn close(&self, source_id: u64) -> Result<()> {
    let sender = self
      .connections
      .lock()
      .expect("event source registry poisoned")
      .remove(&source_id)
      .ok_or_else(|| Error::from("unknown event source id"))?;
    sender
      .send(EventSourceControl::Close)
      .map_err(|_| Error::from("event source closed"))
  }
}

enum EventSourceControl {
  Close,
}

async fn run_event_source(
  source_id: u64,
  client: Client,
  cookies: CookieStoreHandle,
  request: EventSourceConnectRequest,
  mut control_rx: mpsc::UnboundedReceiver<EventSourceControl>,
  sink: Option<Arc<dyn Fn(EventSourceEvent) + Send + Sync>>,
  connections: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<EventSourceControl>>>>,
) {
  let emit = |event: EventSourceEvent| {
    if let Some(sink) = sink.as_ref() {
      sink(event.clone());
    }
  };

  let mut retry_ms = request.retry_ms.unwrap_or(3_000);
  let mut last_event_id = request.last_event_id.clone();

  loop {
    let url = match Url::parse(&request.url) {
      Ok(url) => url,
      Err(error) => {
        emit(EventSourceEvent {
          source_id,
          kind: EventSourceEventKind::Error,
          event: None,
          data: Some(error.to_string()),
          last_event_id: last_event_id.clone(),
          retry_ms: Some(retry_ms),
        });
        break;
      }
    };

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    if let Some(cookie_header) = cookies.cookie_header(&url) {
      headers.insert(
        HeaderName::from_static("cookie"),
        HeaderValue::from_str(&cookie_header).unwrap_or_else(|_| HeaderValue::from_static("")),
      );
    }
    if let Some(event_id) = &last_event_id {
      headers.insert(
        HeaderName::from_static("last-event-id"),
        HeaderValue::from_str(event_id).unwrap_or_else(|_| HeaderValue::from_static("")),
      );
    }
    for HeaderEntry { name, value } in &request.headers {
      if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_str(value),
      ) {
        headers.insert(name, value);
      }
    }

    let response = match client.get(url.as_str()).headers(headers).send().await {
      Ok(response) => response,
      Err(error) => {
        emit(EventSourceEvent {
          source_id,
          kind: EventSourceEventKind::Error,
          event: None,
          data: Some(error.to_string()),
          last_event_id: last_event_id.clone(),
          retry_ms: Some(retry_ms),
        });
        if wait_for_reconnect(&mut control_rx, retry_ms).await {
          break;
        } else {
          continue;
        }
      }
    };

    cookies.persist().ok();
    emit(EventSourceEvent {
      source_id,
      kind: EventSourceEventKind::Open,
      event: None,
      data: None,
      last_event_id: last_event_id.clone(),
      retry_ms: Some(retry_ms),
    });

    let mut parser = SseParser::default();
    let mut stream = response.bytes_stream();
    loop {
      tokio::select! {
        control = control_rx.recv() => {
          if matches!(control, Some(EventSourceControl::Close)) {
            connections.lock().ok().and_then(|mut registry| registry.remove(&source_id));
            emit(EventSourceEvent {
              source_id,
              kind: EventSourceEventKind::Close,
              event: None,
              data: None,
              last_event_id: last_event_id.clone(),
              retry_ms: Some(retry_ms),
            });
            return;
          }
        }
        chunk = stream.next() => {
          match chunk {
            Some(Ok(bytes)) => {
              if let Some(parsed) = parser.push(&bytes) {
                if let Some(value) = parsed.retry_ms {
                  retry_ms = value;
                }
                if let Some(event_id) = parsed.last_event_id.clone() {
                  last_event_id = Some(event_id);
                }
                emit(EventSourceEvent {
                  source_id,
                  kind: if parsed.event.as_deref().unwrap_or("message") == "message" {
                    EventSourceEventKind::Message
                  } else {
                    EventSourceEventKind::Named
                  },
                  event: parsed.event,
                  data: Some(parsed.data),
                  last_event_id: last_event_id.clone(),
                  retry_ms: Some(retry_ms),
                });
              }
            }
            Some(Err(error)) => {
              emit(EventSourceEvent {
                source_id,
                kind: EventSourceEventKind::Error,
                event: None,
                data: Some(error.to_string()),
                last_event_id: last_event_id.clone(),
                retry_ms: Some(retry_ms),
              });
              break;
            }
            None => break,
          }
        }
      }
    }

    if wait_for_reconnect(&mut control_rx, retry_ms).await {
      break;
    } else {
      continue;
    }
  }

  connections.lock().ok().and_then(|mut registry| registry.remove(&source_id));
}

async fn wait_for_reconnect(
  control_rx: &mut mpsc::UnboundedReceiver<EventSourceControl>,
  retry_ms: u64,
) -> bool {
  tokio::select! {
    control = control_rx.recv() => matches!(control, Some(EventSourceControl::Close)),
    _ = sleep(Duration::from_millis(retry_ms)) => false,
  }
}

#[derive(Default)]
struct SseParser {
  buffer: Vec<u8>,
  data_lines: Vec<String>,
  event: Option<String>,
  last_event_id: Option<String>,
  retry_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct ParsedEvent {
  data: String,
  event: Option<String>,
  last_event_id: Option<String>,
  retry_ms: Option<u64>,
}

impl SseParser {
  fn push(&mut self, bytes: &[u8]) -> Option<ParsedEvent> {
    self.buffer.extend_from_slice(bytes);
    let mut produced = None;
    while let Some(position) = find_event_separator(&self.buffer) {
      let chunk = self.buffer.drain(..position).collect::<Vec<_>>();
      if let Some(parsed) = self.parse_chunk(&chunk) {
        produced = Some(parsed);
      }
    }
    produced
  }

  fn parse_chunk(&mut self, chunk: &[u8]) -> Option<ParsedEvent> {
    let text = String::from_utf8_lossy(chunk).replace("\r\n", "\n");
    for line in text.lines() {
      let line = line.trim_end_matches('\r');
      if line.is_empty() {
        continue;
      }
      if let Some(rest) = line.strip_prefix("data:") {
        self.data_lines.push(rest.trim_start().to_string());
        continue;
      }
      if let Some(rest) = line.strip_prefix("event:") {
        self.event = Some(rest.trim_start().to_string());
        continue;
      }
      if let Some(rest) = line.strip_prefix("id:") {
        self.last_event_id = Some(rest.trim_start().to_string());
        continue;
      }
      if let Some(rest) = line.strip_prefix("retry:") {
        self.retry_ms = rest.trim().parse().ok();
      }
    }

    if self.data_lines.is_empty() && self.event.is_none() {
      return None;
    }

    let data = self.data_lines.join("\n");
    let event = self.event.take();
    let last_event_id = self.last_event_id.clone();
    let retry_ms = self.retry_ms.take();
    self.data_lines.clear();
    Some(ParsedEvent {
      data,
      event,
      last_event_id,
      retry_ms,
    })
  }
}

fn find_event_separator(buffer: &[u8]) -> Option<usize> {
  buffer
    .windows(4)
    .position(|window| window == b"\r\n\r\n")
    .map(|position| position + 4)
    .or_else(|| buffer.windows(2).position(|window| window == b"\n\n").map(|position| position + 2))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_retry_field() {
    let mut parser = SseParser::default();
    let parsed = parser.push(b"retry: 1500\n\ndata: hello\n\n").unwrap();
    assert_eq!(parsed.retry_ms, Some(1500));
  }
}
