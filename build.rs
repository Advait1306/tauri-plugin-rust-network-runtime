const COMMANDS: &[&str] = &[
  "abort_http_request",
  "eventsource_close",
  "eventsource_connect",
  "http_request",
  "send_beacon",
  "websocket_close",
  "websocket_connect",
  "websocket_send",
];

fn main() {
  tauri_plugin::Builder::new(COMMANDS)
    .android_path("android")
    .ios_path("ios")
    .build();
}
