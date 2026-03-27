use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderEntry {
  pub name: String,
  pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpRequest {
  pub url: String,
  pub method: String,
  #[serde(default)]
  pub headers: Vec<HeaderEntry>,
  pub body: Option<BodyData>,
  pub timeout_ms: Option<u64>,
  pub allow_redirects: Option<bool>,
  pub request_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpResponse {
  pub status: u16,
  pub status_text: String,
  pub url: String,
  #[serde(default)]
  pub headers: Vec<HeaderEntry>,
  #[serde(default)]
  pub body: Vec<u8>,
  pub redirected: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BodyData {
  Text {
    value: String,
    content_type: Option<String>,
  },
  Base64 {
    value: String,
    content_type: Option<String>,
  },
  Bytes {
    value: Vec<u8>,
    content_type: Option<String>,
  },
  Json {
    value: serde_json::Value,
  },
  FormUrlEncoded {
    #[serde(default)]
    fields: Vec<FormField>,
  },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
  pub name: String,
  pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AbortHttpRequest {
  pub request_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketConnectRequest {
  pub url: String,
  #[serde(default)]
  pub protocols: Vec<String>,
  #[serde(default)]
  pub headers: Vec<HeaderEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketConnectResponse {
  pub socket_id: u64,
  pub protocol: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketSendRequest {
  pub socket_id: u64,
  pub message: SocketMessage,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SocketMessage {
  Text {
    value: String,
  },
  Binary {
    value: Vec<u8>,
  },
  Close {
    code: Option<u16>,
    reason: Option<String>,
  },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketCloseRequest {
  pub socket_id: u64,
  pub code: Option<u16>,
  pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketEvent {
  pub socket_id: u64,
  pub kind: WebSocketEventKind,
  pub data: Option<Vec<u8>>,
  pub text: Option<String>,
  pub code: Option<u16>,
  pub reason: Option<String>,
  pub was_clean: Option<bool>,
  pub protocol: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WebSocketEventKind {
  Open,
  Message,
  Error,
  Close,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSourceConnectRequest {
  pub url: String,
  #[serde(default)]
  pub headers: Vec<HeaderEntry>,
  pub with_credentials: Option<bool>,
  pub last_event_id: Option<String>,
  pub retry_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSourceConnectResponse {
  pub source_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSourceCloseRequest {
  pub source_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSourceEvent {
  pub source_id: u64,
  pub kind: EventSourceEventKind,
  pub event: Option<String>,
  pub data: Option<String>,
  pub last_event_id: Option<String>,
  pub retry_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EventSourceEventKind {
  Open,
  Message,
  Error,
  Close,
  Named,
}
