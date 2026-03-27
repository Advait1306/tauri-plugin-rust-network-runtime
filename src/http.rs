use std::time::Duration;

use reqwest::{
  header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE},
  redirect::Policy,
  Client,
  Method,
};
use url::Url;

use crate::{
  cookies::CookieStoreHandle,
  models::{BodyData, HeaderEntry, HttpRequest, HttpResponse},
  Error, Result,
};

#[derive(Clone)]
pub struct HttpClient {
  client: Client,
  no_redirect_client: Client,
  cookies: CookieStoreHandle,
}

impl HttpClient {
  pub fn new(cookies: CookieStoreHandle) -> Result<Self> {
    let client = Client::builder()
      .cookie_provider(cookies.provider())
      .redirect(Policy::limited(10))
      .build()?;
    let no_redirect_client = Client::builder()
      .cookie_provider(cookies.provider())
      .redirect(Policy::none())
      .build()?;

    Ok(Self {
      client,
      no_redirect_client,
      cookies,
    })
  }

  pub fn request(&self, method: impl Into<Method>, url: impl Into<String>) -> RequestBuilder {
    RequestBuilder {
      client: self.clone(),
      request: HttpRequest {
        method: method.into().to_string(),
        url: url.into(),
        headers: Vec::new(),
        body: None,
        timeout_ms: None,
        allow_redirects: None,
        request_id: None,
      },
    }
  }

  pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse> {
    let url = Url::parse(&request.url)?;
    let method = Method::from_bytes(request.method.as_bytes()).map_err(|error| Error::from(error.to_string()))?;
    let client = if request.allow_redirects == Some(false) {
      &self.no_redirect_client
    } else {
      &self.client
    };

    let mut builder = client.request(method, url.as_str());

    if let Some(timeout_ms) = request.timeout_ms {
      builder = builder.timeout(Duration::from_millis(timeout_ms));
    }

    let mut headers = HeaderMap::new();
    for header in request.headers {
      let name = HeaderName::from_bytes(header.name.as_bytes())
        .map_err(|error| Error::from(error.to_string()))?;
      let value = HeaderValue::from_str(&header.value)
        .map_err(|error| Error::from(error.to_string()))?;
      headers.append(name, value);
    }

    let body = request.body;
    let body = match body {
      Some(BodyData::Text { value, content_type }) => {
        if let Some(content_type) = content_type.or_else(|| infer_content_type(&headers)) {
          headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&content_type).map_err(|error| Error::from(error.to_string()))?,
          );
        }
        Some(value.into_bytes())
      }
      Some(BodyData::Base64 { value, content_type }) => {
        if let Some(content_type) = content_type.or_else(|| infer_content_type(&headers)) {
          headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&content_type).map_err(|error| Error::from(error.to_string()))?,
          );
        }
        Some(
          {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
              .decode(value.as_bytes())
              .map_err(|error| Error::from(error.to_string()))?
          },
        )
      }
      Some(BodyData::Bytes { value, content_type }) => {
        if let Some(content_type) = content_type.or_else(|| infer_content_type(&headers)) {
          headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&content_type).map_err(|error| Error::from(error.to_string()))?,
          );
        }
        Some(value)
      }
      Some(BodyData::Json { value }) => {
        headers.insert(
          CONTENT_TYPE,
          HeaderValue::from_static("application/json; charset=utf-8"),
        );
        Some(serde_json::to_vec(&value)?)
      }
      Some(BodyData::FormUrlEncoded { fields }) => {
        headers.insert(
          CONTENT_TYPE,
          HeaderValue::from_static("application/x-www-form-urlencoded; charset=utf-8"),
        );
        let body = fields
          .into_iter()
          .map(|field| {
            format!(
              "{}={}",
              url::form_urlencoded::byte_serialize(field.name.as_bytes()).collect::<String>(),
              url::form_urlencoded::byte_serialize(field.value.as_bytes()).collect::<String>()
            )
          })
          .collect::<Vec<_>>()
          .join("&");
        Some(body.into_bytes())
      }
      None => None,
    };

    builder = builder.headers(headers);
    if let Some(body) = body {
      builder = builder.body(body);
    }

    let response = builder.send().await?;
    let status = response.status();
    let status_text = status.canonical_reason().unwrap_or("").to_string();
    let url = response.url().to_string();
    let redirected = response.url().as_str() != request.url;
    let headers = response
      .headers()
      .iter()
      .map(|(name, value)| HeaderEntry {
        name: name.to_string(),
        value: value.to_str().unwrap_or_default().to_string(),
      })
      .collect::<Vec<_>>();
    let body = response.bytes().await?.to_vec();

    self.cookies.persist()?;

    Ok(HttpResponse {
      status: status.as_u16(),
      status_text,
      url,
      headers,
      body,
      redirected,
    })
  }
}

fn infer_content_type(headers: &HeaderMap) -> Option<String> {
  headers
    .get(CONTENT_TYPE)
    .and_then(|value| value.to_str().ok())
    .map(|value| value.to_string())
}

#[derive(Clone)]
pub struct RequestBuilder {
  client: HttpClient,
  request: HttpRequest,
}

impl RequestBuilder {
  pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
    self.request.headers.push(HeaderEntry {
      name: name.into(),
      value: value.into(),
    });
    self
  }

  pub fn body(mut self, body: BodyData) -> Self {
    self.request.body = Some(body);
    self
  }

  pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
    self.request.timeout_ms = Some(timeout_ms);
    self
  }

  pub fn allow_redirects(mut self, allow_redirects: bool) -> Self {
    self.request.allow_redirects = Some(allow_redirects);
    self
  }

  pub async fn send(self) -> Result<HttpResponse> {
    self.client.execute(self.request).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn infer_content_type_reads_header() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    assert_eq!(infer_content_type(&headers), Some("text/plain".to_string()));
  }
}
