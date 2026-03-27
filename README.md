# Tauri Plugin rust_network_runtime

Desktop-only Tauri v2 plugin that replaces JS-visible runtime networking APIs with Rust-backed implementations while leaving page loading under Tauri/WebView control.

## What it patches

- `fetch`
- `WebSocket`
- `XMLHttpRequest` (async only)
- `EventSource`
- `navigator.sendBeacon`

All JS traffic and the exported Rust clients share one Rust-managed cookie jar backed by `reqwest_cookie_store` + `cookie_store`.

## Rust host usage

```rust
tauri::Builder::default()
  .plugin(tauri_plugin_rust_network_runtime::init())
```

The plugin crate re-exports:

- `HttpClient`
- `RequestBuilder`
- `WebSocketClient`
- `WebSocketConnection`
- `CookieStoreHandle`

## Guest JS usage

```ts
import { installNetworkRuntime } from 'tauri-plugin-rust-network-runtime-api'

installNetworkRuntime()
```

Call `installNetworkRuntime()` before your auth, sync, analytics, or data clients initialize.

Use `restoreNativeNetworkRuntime()` to undo the patching and restore native browser objects.

## Example app

`examples/tauri-app` now includes a runnable demo that:

- starts an in-process Rust HTTP server inside the Tauri app
- installs the guest runtime shims before the frontend mounts
- exercises `fetch`, `XMLHttpRequest`, `WebSocket`, `EventSource`, and `navigator.sendBeacon`
- renders the observed request and response payloads in the UI

Run it with:

```bash
cd examples/tauri-app
npm install
npm run build
cargo tauri build --debug
```
