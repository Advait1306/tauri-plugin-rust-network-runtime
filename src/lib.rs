use tauri::{
  plugin::{Builder, TauriPlugin},
  Manager, Runtime,
};

pub use client::{
  CookieStoreHandle, HttpClient, RequestBuilder, WebSocketClient, WebSocketConnection,
};
pub use error::{Error, Result};
pub use models::*;

#[cfg(desktop)]
mod desktop;
#[cfg(mobile)]
mod mobile;

mod client;
mod commands;
mod cookies;
mod error;
mod http;
mod models;
mod sse;
mod websocket;

#[cfg(desktop)]
use desktop::RustNetworkRuntime;
#[cfg(mobile)]
use mobile::RustNetworkRuntime;

/// Extensions to [`tauri::App`], [`tauri::AppHandle`] and [`tauri::Window`] to access the plugin state.
pub trait RustNetworkRuntimeExt<R: Runtime> {
  fn rust_network_runtime(&self) -> &RustNetworkRuntime<R>;
}

impl<R: Runtime, T: Manager<R>> RustNetworkRuntimeExt<R> for T {
  fn rust_network_runtime(&self) -> &RustNetworkRuntime<R> {
    self.state::<RustNetworkRuntime<R>>().inner()
  }
}

/// Initializes the plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
  Builder::new("rust-network-runtime")
    .invoke_handler(tauri::generate_handler![
      commands::http_request,
      commands::abort_http_request,
      commands::websocket_connect,
      commands::websocket_send,
      commands::websocket_close,
      commands::eventsource_connect,
      commands::eventsource_close,
      commands::send_beacon,
    ])
    .setup(|app, api| {
      #[cfg(mobile)]
      let rust_network_runtime = mobile::init(app, api)?;
      #[cfg(desktop)]
      let rust_network_runtime = desktop::init(app, api)?;
      app.manage(rust_network_runtime);
      Ok(())
    })
    .build()
}
