use std::time::Duration;

use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Method, RequestBuilder, Response};
use serde_json::Value;

use crate::error::parse_api_error;
use crate::{Error, JsonObject, Result};

pub(crate) const DEFAULT_BASE_URL: &str = "https://api.hot.dev";

const USER_AGENT_VALUE: &str = concat!("hot-sdk-rust/", env!("CARGO_PKG_VERSION"));

// JSON requests retry on 429 when the server supplies retry_after.
const MAX_RETRIES: u32 = 2;
const MAX_RETRY_AFTER_SECONDS: u64 = 30;

// Mirrors Python's urllib quote(safe=""): everything but unreserved characters.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Percent-encodes a single path segment, including any "/".
pub(crate) fn enc(segment: &str) -> String {
    utf8_percent_encode(segment, SEGMENT).to_string()
}

/// Percent-encodes each segment of a slash-separated path, keeping the slashes.
pub(crate) fn enc_path(path: &str) -> String {
    path.split('/').map(enc).collect::<Vec<_>>().join("/")
}

pub(crate) struct Transport {
    token: String,
    base_url: String,
    api_base_url: String,
    client: reqwest::Client,
    timeout: Option<Duration>,
}

impl Transport {
    pub(crate) fn new(
        token: String,
        base_url: String,
        timeout: Option<Duration>,
        client: Option<reqwest::Client>,
    ) -> Transport {
        let base_url = base_url.trim_end_matches('/').to_string();
        Transport {
            token,
            api_base_url: format!("{base_url}/v1"),
            base_url,
            client: client.unwrap_or_default(),
            timeout,
        }
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns an authorized request builder for a path relative to /v1.
    pub(crate) fn request_builder(&self, method: Method, path: &str) -> RequestBuilder {
        let mut auth = HeaderValue::try_from(format!("Bearer {}", self.token))
            .unwrap_or(HeaderValue::from_static(""));
        auth.set_sensitive(true);
        self.client
            .request(method, format!("{}{}", self.api_base_url, path))
            .header(AUTHORIZATION, auth)
            .header(USER_AGENT, USER_AGENT_VALUE)
    }

    /// Sends a request and maps non-2xx responses to [`ApiError`]. The
    /// response body is left unread for the caller.
    pub(crate) async fn execute(&self, builder: RequestBuilder) -> Result<Response> {
        let response = builder.send().await?;
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            let headers = response.headers().clone();
            let text = response.text().await.unwrap_or_default();
            return Err(Error::Api(parse_api_error(
                status.as_u16(),
                &text,
                &headers,
            )));
        }
        Ok(response)
    }

    /// Calls a JSON endpoint and returns the response envelope. Empty bodies
    /// become `{"data": null, "meta": {}}`; non-object JSON is wrapped the
    /// same way.
    pub(crate) async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let mut attempts: u32 = 0;
        let response = loop {
            let mut builder = self
                .request_builder(method.clone(), path)
                .header(ACCEPT, "application/json");
            if let Some(timeout) = self.timeout {
                builder = builder.timeout(timeout);
            }
            if !query.is_empty() {
                builder = builder.query(query);
            }
            if let Some(body) = body {
                builder = builder.header(CONTENT_TYPE, "application/json").json(body);
            }

            let response = builder.send().await?;
            let status = response.status();
            if status.is_client_error() || status.is_server_error() {
                let headers = response.headers().clone();
                let text = response.text().await.unwrap_or_default();
                let error = parse_api_error(status.as_u16(), &text, &headers);
                if status.as_u16() == 429 && attempts < MAX_RETRIES {
                    if let Some(retry_after) = error.retry_after {
                        if retry_after > 0 {
                            attempts += 1;
                            tokio::time::sleep(Duration::from_secs(
                                retry_after.min(MAX_RETRY_AFTER_SECONDS),
                            ))
                            .await;
                            continue;
                        }
                    }
                }
                return Err(Error::Api(error));
            }
            break response;
        };
        let text = response.text().await?;
        if text.is_empty() {
            return Ok(empty_envelope());
        }
        match serde_json::from_str::<Value>(&text)? {
            Value::Object(envelope) => Ok(envelope),
            other => {
                let mut envelope = empty_envelope();
                envelope.insert("data".to_string(), other);
                Ok(envelope)
            }
        }
    }
}

fn empty_envelope() -> JsonObject {
    let mut envelope = JsonObject::new();
    envelope.insert("data".to_string(), Value::Null);
    envelope.insert("meta".to_string(), Value::Object(JsonObject::new()));
    envelope
}

/// Extracts the envelope's "data" member when it is an object.
pub(crate) fn data_object(mut envelope: JsonObject) -> JsonObject {
    match envelope.remove("data") {
        Some(Value::Object(object)) => object,
        _ => JsonObject::new(),
    }
}

/// Decodes a raw JSON response and returns its "data" object.
pub(crate) async fn decode_data_response(response: Response) -> Result<JsonObject> {
    let text = response.text().await?;
    let envelope = match serde_json::from_str::<Value>(&text)? {
        Value::Object(envelope) => envelope,
        _ => JsonObject::new(),
    };
    Ok(data_object(envelope))
}
