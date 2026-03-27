use serde::{ser::Serializer, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] std::io::Error),
  #[error(transparent)]
  Reqwest(#[from] reqwest::Error),
  #[error(transparent)]
  Json(#[from] serde_json::Error),
  #[error(transparent)]
  Url(#[from] url::ParseError),
  #[error(transparent)]
  Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
  #[error("{0}")]
  Message(String),
  #[cfg(mobile)]
  #[error(transparent)]
  PluginInvoke(#[from] tauri::plugin::mobile::PluginInvokeError),
}

impl Serialize for Error {
  fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_str(self.to_string().as_ref())
  }
}

impl From<&str> for Error {
  fn from(value: &str) -> Self {
    Self::Message(value.to_string())
  }
}

impl From<String> for Error {
  fn from(value: String) -> Self {
    Self::Message(value)
  }
}
