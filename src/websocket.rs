use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
  },
};

use futures_util::{SinkExt, StreamExt};
use reqwest::header::{HeaderName, HeaderValue};
use tauri::async_runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::{
  connect_async,
  tungstenite::{client::IntoClientRequest, protocol::Message},
};
use url::Url;

use crate::{
  cookies::CookieStoreHandle,
  models::{
    HeaderEntry, SocketMessage, WebSocketConnectRequest, WebSocketEvent, WebSocketEventKind,
    WebSocketSendRequest,
  },
  Error, Result,
};

fn cookie_url_for_websocket(url: &Url) -> Url {
  let mut cookie_url = url.clone();
  match cookie_url.scheme() {
    "ws" => {
      let _ = cookie_url.set_scheme("http");
    }
    "wss" => {
      let _ = cookie_url.set_scheme("https");
    }
    _ => {}
  }
  cookie_url
}

#[derive(Clone)]
pub struct WebSocketClient {
  cookies: CookieStoreHandle,
  next_id: Arc<AtomicU64>,
  connections: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<OutboundMessage>>>>,
}

impl WebSocketClient {
  pub fn new(cookies: CookieStoreHandle) -> Self {
    Self {
      cookies,
      next_id: Arc::new(AtomicU64::new(1)),
      connections: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  pub async fn connect(&self, request: WebSocketConnectRequest) -> Result<WebSocketConnection> {
    self.connect_with_sink(request, None).await
  }

  pub async fn connect_with_sink(
    &self,
    request: WebSocketConnectRequest,
    sink: Option<Arc<dyn Fn(WebSocketEvent) + Send + Sync>>,
  ) -> Result<WebSocketConnection> {
    let socket_id = self.next_id.fetch_add(1, Ordering::Relaxed);
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    self
      .connections
      .lock()
      .expect("socket registry poisoned")
      .insert(socket_id, command_tx.clone());

    let cookies = self.cookies.clone();
    let connections = self.connections.clone();
    async_runtime::spawn(run_socket(
      socket_id,
      request,
      cookies,
      command_rx,
      event_tx,
      sink,
      connections,
    ));

    Ok(WebSocketConnection {
      socket_id,
      command_tx,
      event_rx,
    })
  }

  pub fn send(&self, request: WebSocketSendRequest) -> Result<()> {
    let sender = self
      .connections
      .lock()
      .expect("socket registry poisoned")
      .get(&request.socket_id)
      .cloned()
      .ok_or_else(|| Error::from("unknown websocket socket id"))?;

    let message = match request.message {
      SocketMessage::Text { value } => OutboundMessage::Text(value),
      SocketMessage::Binary { value } => OutboundMessage::Binary(value),
      SocketMessage::Close { code, reason } => OutboundMessage::Close { code, reason },
    };

    sender
      .send(message)
      .map_err(|_| Error::from("websocket connection closed"))
  }

  pub fn close(&self, socket_id: u64, code: Option<u16>, reason: Option<String>) -> Result<()> {
    self.send(WebSocketSendRequest {
      socket_id,
      message: SocketMessage::Close { code, reason },
    })
  }
}

pub struct WebSocketConnection {
  socket_id: u64,
  command_tx: mpsc::UnboundedSender<OutboundMessage>,
  event_rx: mpsc::UnboundedReceiver<WebSocketEvent>,
}

impl WebSocketConnection {
  pub fn id(&self) -> u64 {
    self.socket_id
  }

  pub fn send_text(&self, value: impl Into<String>) -> Result<()> {
    self
      .command_tx
      .send(OutboundMessage::Text(value.into()))
      .map_err(|_| Error::from("websocket connection closed"))
  }

  pub fn send_binary(&self, value: impl Into<Vec<u8>>) -> Result<()> {
    self
      .command_tx
      .send(OutboundMessage::Binary(value.into()))
      .map_err(|_| Error::from("websocket connection closed"))
  }

  pub fn close(&self, code: Option<u16>, reason: Option<String>) -> Result<()> {
    self
      .command_tx
      .send(OutboundMessage::Close { code, reason })
      .map_err(|_| Error::from("websocket connection closed"))
  }

  pub async fn next_event(&mut self) -> Option<WebSocketEvent> {
    self.event_rx.recv().await
  }
}

enum OutboundMessage {
  Text(String),
  Binary(Vec<u8>),
  Close {
    code: Option<u16>,
    reason: Option<String>,
  },
}

async fn run_socket(
  socket_id: u64,
  connect_request: WebSocketConnectRequest,
  cookies: CookieStoreHandle,
  mut command_rx: mpsc::UnboundedReceiver<OutboundMessage>,
  event_tx: mpsc::UnboundedSender<WebSocketEvent>,
  sink: Option<Arc<dyn Fn(WebSocketEvent) + Send + Sync>>,
  connections: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<OutboundMessage>>>>,
) {
  let emit = |event: WebSocketEvent| {
    if let Some(sink) = sink.as_ref() {
      sink(event.clone());
    }
    let _ = event_tx.send(event);
  };

  let url = match Url::parse(&connect_request.url) {
    Ok(url) => url,
    Err(error) => {
      emit(WebSocketEvent {
        socket_id,
        kind: WebSocketEventKind::Error,
        data: None,
        text: Some(error.to_string()),
        code: None,
        reason: None,
        was_clean: None,
        protocol: None,
      });
      connections.lock().ok().and_then(|mut registry| registry.remove(&socket_id));
      return;
    }
  };
  let cookie_url = cookie_url_for_websocket(&url);

  let mut client_request = match url.as_str().into_client_request() {
    Ok(request) => request,
    Err(error) => {
      emit(WebSocketEvent {
        socket_id,
        kind: WebSocketEventKind::Error,
        data: None,
        text: Some(error.to_string()),
        code: None,
        reason: None,
        was_clean: None,
        protocol: None,
      });
      connections.lock().ok().and_then(|mut registry| registry.remove(&socket_id));
      return;
    }
  };

  let headers = client_request.headers_mut();
  for HeaderEntry { name, value } in connect_request.headers {
    if let (Ok(name), Ok(value)) = (
      HeaderName::from_bytes(name.as_bytes()),
      HeaderValue::from_str(&value),
    ) {
      headers.insert(name, value);
    }
  }
  if !connect_request.protocols.is_empty() {
    if let Ok(value) = HeaderValue::from_str(&connect_request.protocols.join(", ")) {
      headers.insert("Sec-WebSocket-Protocol", value);
    }
  }
  if let Some(cookie_header) = cookies.cookie_header(&cookie_url) {
    if let Ok(value) = HeaderValue::from_str(&cookie_header) {
      headers.insert("Cookie", value);
    }
  }

  let (socket, response) = match connect_async(client_request).await {
    Ok(result) => result,
    Err(error) => {
      emit(WebSocketEvent {
        socket_id,
        kind: WebSocketEventKind::Error,
        data: None,
        text: Some(error.to_string()),
        code: None,
        reason: None,
        was_clean: None,
        protocol: None,
      });
      emit(WebSocketEvent {
        socket_id,
        kind: WebSocketEventKind::Close,
        data: None,
        text: None,
        code: None,
        reason: Some(error.to_string()),
        was_clean: Some(false),
        protocol: None,
      });
      connections.lock().ok().and_then(|mut registry| registry.remove(&socket_id));
      return;
    }
  };

  cookies.store_response_headers(&cookie_url, response.headers());
  let protocol = response
    .headers()
    .get("Sec-WebSocket-Protocol")
    .and_then(|value| value.to_str().ok())
    .map(|value| value.to_string());

  emit(WebSocketEvent {
    socket_id,
    kind: WebSocketEventKind::Open,
    data: None,
    text: None,
    code: None,
    reason: None,
    was_clean: None,
    protocol: protocol.clone(),
  });

  let (mut sink_stream, mut stream) = socket.split();
  loop {
    tokio::select! {
      command = command_rx.recv() => {
        match command {
          Some(OutboundMessage::Text(value)) => {
            let _ = sink_stream.send(Message::Text(value.into())).await;
          }
          Some(OutboundMessage::Binary(value)) => {
            let _ = sink_stream.send(Message::Binary(value.into())).await;
          }
          Some(OutboundMessage::Close { code, reason }) => {
            let frame = code.map(|code| tokio_tungstenite::tungstenite::protocol::CloseFrame {
              code: code.into(),
              reason: reason.unwrap_or_default().into(),
            });
            let _ = sink_stream.send(Message::Close(frame)).await;
            break;
          }
          None => break,
        }
      }
      message = stream.next() => {
        match message {
          Some(Ok(Message::Text(text))) => {
            emit(WebSocketEvent {
              socket_id,
              kind: WebSocketEventKind::Message,
              data: None,
              text: Some(text.to_string()),
              code: None,
              reason: None,
              was_clean: None,
              protocol: protocol.clone(),
            });
          }
          Some(Ok(Message::Binary(data))) => {
            emit(WebSocketEvent {
              socket_id,
              kind: WebSocketEventKind::Message,
              data: Some(data.to_vec()),
              text: None,
              code: None,
              reason: None,
              was_clean: None,
              protocol: protocol.clone(),
            });
          }
          Some(Ok(Message::Close(frame))) => {
            emit(WebSocketEvent {
              socket_id,
              kind: WebSocketEventKind::Close,
              data: None,
              text: None,
              code: frame.as_ref().map(|frame| frame.code.into()),
              reason: frame.as_ref().map(|frame| frame.reason.to_string()),
              was_clean: Some(true),
              protocol: protocol.clone(),
            });
            break;
          }
          Some(Ok(Message::Ping(payload))) => {
            let _ = sink_stream.send(Message::Pong(payload)).await;
          }
          Some(Ok(Message::Pong(_))) => {}
          Some(Ok(Message::Frame(_))) => {}
          Some(Err(error)) => {
            emit(WebSocketEvent {
              socket_id,
              kind: WebSocketEventKind::Error,
              data: None,
              text: Some(error.to_string()),
              code: None,
              reason: None,
              was_clean: None,
              protocol: protocol.clone(),
            });
            emit(WebSocketEvent {
              socket_id,
              kind: WebSocketEventKind::Close,
              data: None,
              text: None,
              code: None,
              reason: Some(error.to_string()),
              was_clean: Some(false),
              protocol: protocol.clone(),
            });
            break;
          }
          None => break,
        }
      }
    }
  }

  connections.lock().ok().and_then(|mut registry| registry.remove(&socket_id));
  cookies.persist().ok();
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn outbound_message_can_be_built() {
    let message = OutboundMessage::Text("hello".to_string());
    match message {
      OutboundMessage::Text(value) => assert_eq!(value, "hello"),
      _ => panic!("unexpected message"),
    }
  }
}
