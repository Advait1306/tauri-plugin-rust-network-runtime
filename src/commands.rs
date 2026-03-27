use tauri::{command, AppHandle, Runtime};

use crate::{
  models::{
    AbortHttpRequest, EventSourceCloseRequest, EventSourceConnectRequest,
    EventSourceConnectResponse, HttpRequest, HttpResponse, WebSocketCloseRequest,
    WebSocketConnectRequest, WebSocketConnectResponse, WebSocketSendRequest,
  },
  Result, RustNetworkRuntimeExt,
};

#[command]
pub(crate) async fn http_request<R: Runtime>(
  app: AppHandle<R>,
  request: HttpRequest,
) -> Result<HttpResponse> {
  app.rust_network_runtime().http_request(request).await
}

#[command]
pub(crate) fn abort_http_request<R: Runtime>(
  app: AppHandle<R>,
  payload: AbortHttpRequest,
) -> bool {
  app
    .rust_network_runtime()
    .abort_http_request(&payload.request_id)
}

#[command]
pub(crate) async fn websocket_connect<R: Runtime>(
  app: AppHandle<R>,
  request: WebSocketConnectRequest,
) -> Result<WebSocketConnectResponse> {
  app.rust_network_runtime().websocket_connect(request).await
}

#[command]
pub(crate) fn websocket_send<R: Runtime>(
  app: AppHandle<R>,
  request: WebSocketSendRequest,
) -> Result<()> {
  app.rust_network_runtime().websocket_send(request)
}

#[command]
pub(crate) fn websocket_close<R: Runtime>(
  app: AppHandle<R>,
  request: WebSocketCloseRequest,
) -> Result<()> {
  app.rust_network_runtime().websocket_close(request)
}

#[command]
pub(crate) async fn eventsource_connect<R: Runtime>(
  app: AppHandle<R>,
  request: EventSourceConnectRequest,
) -> Result<EventSourceConnectResponse> {
  app.rust_network_runtime().eventsource_connect(request).await
}

#[command]
pub(crate) fn eventsource_close<R: Runtime>(
  app: AppHandle<R>,
  request: EventSourceCloseRequest,
) -> Result<()> {
  app
    .rust_network_runtime()
    .eventsource_close(request.source_id)
}

#[command]
pub(crate) fn send_beacon<R: Runtime>(app: AppHandle<R>, request: HttpRequest) -> bool {
  app.rust_network_runtime().send_beacon(request)
}
