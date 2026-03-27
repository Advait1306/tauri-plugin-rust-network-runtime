use serde::de::DeserializeOwned;
use tauri::{plugin::PluginApi, AppHandle, Runtime};

/// Desktop-only plugin stub for mobile targets.
pub struct RustNetworkRuntime<R: Runtime>(std::marker::PhantomData<R>);

pub fn init<R: Runtime, C: DeserializeOwned>(
  _app: &AppHandle<R>,
  _api: PluginApi<R, C>,
) -> crate::Result<RustNetworkRuntime<R>> {
  Err(crate::Error::from(
    "tauri-plugin-rust-network-runtime is desktop-only in v1",
  ))
}
