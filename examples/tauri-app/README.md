# Example App

This example demonstrates the plugin in action instead of just installing it.

## What it does

- Installs `installNetworkRuntime()` before the UI mounts.
- Starts a Rust HTTP server inside the Tauri app on `127.0.0.1` with an ephemeral port.
- Exposes that server origin to the frontend through a Tauri command.
- Exercises `fetch`, `XMLHttpRequest`, `WebSocket`, `EventSource`, and `navigator.sendBeacon`.
- Shows the observed request and response payloads in the UI.

## Run it

Use the built app path rather than `tauri dev`, because the example patches `WebSocket` and `EventSource`, which conflicts with Vite HMR.

```bash
npm install
npm run build
cargo tauri build --debug
open src-tauri/target/debug/bundle/macos/tauri-app.app
```

From the UI, use the request buttons to repeat the demo traffic.
