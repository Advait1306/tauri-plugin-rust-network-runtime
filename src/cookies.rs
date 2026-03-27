use std::{
  fs::{self, File},
  io::{BufReader, BufWriter},
  path::{Path, PathBuf},
  sync::Arc,
};

use cookie_store::{serde::json, CookieStore, RawCookie};
use reqwest::header::{HeaderMap, SET_COOKIE};
use reqwest_cookie_store::CookieStoreMutex;
use tauri::{AppHandle, Error as TauriError, Manager, Runtime};
use url::Url;

use crate::{Error, Result};

#[derive(Clone)]
pub struct CookieStoreHandle {
  store: Arc<CookieStoreMutex>,
  path: Arc<PathBuf>,
}

impl CookieStoreHandle {
  pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
    let path = path.into();
    let store = if path.exists() {
      let file = File::open(&path)?;
      let reader = BufReader::new(file);
      json::load(reader).unwrap_or_default()
    } else {
      CookieStore::default()
    };

    Ok(Self {
      store: Arc::new(CookieStoreMutex::new(store)),
      path: Arc::new(path),
    })
  }

  pub fn from_app<R: Runtime>(app: &AppHandle<R>) -> Result<Self> {
    let mut path = app
      .path()
      .app_data_dir()
      .map_err(|error: TauriError| Error::from(error.to_string()))?;
    fs::create_dir_all(&path)?;
    path.push("rust-network-runtime.cookies.json");
    Self::new(path)
  }

  pub fn provider(&self) -> Arc<CookieStoreMutex> {
    self.store.clone()
  }

  pub fn cookie_header(&self, url: &Url) -> Option<String> {
    let store = self.store.lock().ok()?;
    let values = store
      .get_request_values(url)
      .map(|(name, value)| format!("{name}={value}"))
      .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.join("; "))
  }

  pub fn store_response_headers(&self, url: &Url, headers: &HeaderMap) {
    let raw_cookies = headers
      .get_all(SET_COOKIE)
      .iter()
      .filter_map(|value| value.to_str().ok())
      .filter_map(|value| RawCookie::parse(value.to_string()).ok());

    let mut store = self.store.lock().expect("cookie store poisoned");
    store.store_response_cookies(raw_cookies, url);
  }

  pub fn persist(&self) -> Result<()> {
    let store = self.store.lock().expect("cookie store poisoned");
    if let Some(parent) = Path::new(self.path.as_ref()).parent() {
      fs::create_dir_all(parent)?;
    }
    let file = File::create(self.path.as_ref())?;
    let mut writer = BufWriter::new(file);
    json::save(&store, &mut writer).map_err(|error| Error::from(error.to_string()))
  }

  pub fn path(&self) -> &Path {
    self.path.as_ref().as_path()
  }
}
