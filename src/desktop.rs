use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use serde::de::DeserializeOwned;
use tauri::{
  async_runtime::JoinHandle,
  plugin::PluginApi,
  AppHandle, Emitter, Runtime,
};
use tokio::sync::oneshot;

use crate::{
  client::{CookieStoreHandle, HttpClient, WebSocketClient},
  http::RequestBuilder,
  models::{
    EventSourceConnectRequest, EventSourceConnectResponse, EventSourceEvent, HttpRequest,
    HttpResponse, WebSocketCloseRequest, WebSocketConnectRequest, WebSocketConnectResponse,
    WebSocketEvent, WebSocketSendRequest,
  },
  sse::EventSourceClient,
  websocket::WebSocketConnection,
  Error, Result,
};

pub fn init<R: Runtime, C: DeserializeOwned>(
  app: &AppHandle<R>,
  _api: PluginApi<R, C>,
) -> Result<RustNetworkRuntime<R>> {
  let cookies = CookieStoreHandle::from_app(app)?;
  let http = HttpClient::new(cookies.clone())?;
  let websocket = WebSocketClient::new(cookies.clone());
  let event_source = EventSourceClient::new(cookies.clone())?;

  Ok(RustNetworkRuntime {
    app: app.clone(),
    cookies,
    http,
    websocket,
    event_source,
    pending_http: Arc::new(Mutex::new(HashMap::new())),
  })
}

/// Access to the rust-network-runtime APIs.
pub struct RustNetworkRuntime<R: Runtime> {
  app: AppHandle<R>,
  cookies: CookieStoreHandle,
  http: HttpClient,
  websocket: WebSocketClient,
  event_source: EventSourceClient,
  pending_http: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl<R: Runtime> RustNetworkRuntime<R> {
  pub fn cookie_store(&self) -> CookieStoreHandle {
    self.cookies.clone()
  }

  pub fn http_client(&self) -> HttpClient {
    self.http.clone()
  }

  pub fn websocket_client(&self) -> WebSocketClient {
    self.websocket.clone()
  }

  pub fn http_builder(
    &self,
    method: impl Into<reqwest::Method>,
    url: impl Into<String>,
  ) -> RequestBuilder {
    self.http.request(method, url)
  }

  pub async fn http_request(&self, request: HttpRequest) -> Result<HttpResponse> {
    if let Some(request_id) = request.request_id.clone() {
      let (tx, rx) = oneshot::channel();
      let client = self.http.clone();
      let pending_http = self.pending_http.clone();
      let request_id_for_task = request_id.clone();
      let handle = tauri::async_runtime::spawn(async move {
        let result = client.execute(request).await;
        let _ = tx.send(result);
        pending_http
          .lock()
          .ok()
          .and_then(|mut registry| registry.remove(&request_id_for_task));
      });

      self
        .pending_http
        .lock()
        .expect("pending http registry poisoned")
        .insert(request_id, handle);

      return rx.await.unwrap_or_else(|_| Err(Error::from("request aborted")));
    }

    self.http.execute(request).await
  }

  pub fn abort_http_request(&self, request_id: &str) -> bool {
    if let Some(handle) = self
      .pending_http
      .lock()
      .expect("pending http registry poisoned")
      .remove(request_id)
    {
      handle.abort();
      true
    } else {
      false
    }
  }

  pub fn websocket_send(&self, request: WebSocketSendRequest) -> Result<()> {
    self.websocket.send(request)
  }

  pub fn websocket_close(&self, request: WebSocketCloseRequest) -> Result<()> {
    self
      .websocket
      .close(request.socket_id, request.code, request.reason)
  }

  pub async fn websocket_connect(
    &self,
    request: WebSocketConnectRequest,
  ) -> Result<WebSocketConnectResponse> {
    let app = self.app.clone();
    self
      .websocket
      .connect_with_sink(
        request,
        Some(Arc::new(move |event: WebSocketEvent| {
          let _ = app.emit("rust-network-runtime://websocket", event);
        })),
      )
      .await
      .map(|connection: WebSocketConnection| WebSocketConnectResponse {
        socket_id: connection.id(),
        protocol: None,
      })
  }

  pub async fn eventsource_connect(
    &self,
    request: EventSourceConnectRequest,
  ) -> Result<EventSourceConnectResponse> {
    let app = self.app.clone();
    self
      .event_source
      .connect(
        request,
        Some(Arc::new(move |event: EventSourceEvent| {
          let _ = app.emit("rust-network-runtime://eventsource", event);
        })),
      )
      .await
  }

  pub fn eventsource_close(&self, source_id: u64) -> Result<()> {
    self.event_source.close(source_id)
  }

  pub fn send_beacon(&self, request: HttpRequest) -> bool {
    let client = self.http.clone();
    tauri::async_runtime::spawn(async move {
      let _ = client.execute(request).await;
    });
    true
  }
}
