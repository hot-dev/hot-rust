use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use futures_util::StreamExt;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::header::{
    HeaderMap, HeaderValue, InvalidHeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT,
};
use reqwest::{Method, RequestBuilder, Response};
use serde_json::Value;

use crate::error::{parse_api_error, ApiError};
use crate::streaming::{backoff, jitter};
use crate::{Error, JsonObject, Result};

pub(crate) const DEFAULT_BASE_URL: &str = "https://api.hot.dev";

/// Bounds each JSON request unless the builder sets its own timeout.
pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

const USER_AGENT_VALUE: &str = concat!("hot-sdk-rust/", env!("CARGO_PKG_VERSION"));

// JSON requests retry on 429 when the server supplies retry_after, on
// POST 503 event_publish_in_progress, and idempotent ones also on 502, 503
// and 504.
const MAX_RETRIES: u32 = 2;
const MAX_RETRY_AFTER_SECONDS: u64 = 30;
const MAX_UNAVAILABLE_RETRY_AFTER_SECONDS: u64 = 10;

/// Cap raw bytes read from non-2xx error bodies to bound memory on unexpected
/// upstream responses.
const MAX_ERROR_BODY_READ_BYTES: usize = 64 * 1024;

// Mirrors Python's urllib quote(safe=""): everything but unreserved characters.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Validates and percent-encodes a single path segment, including any "/".
/// Rejects empty and dot (`"."`, `".."`) segments before any request is sent.
pub(crate) fn enc_segment(segment: &str) -> Result<String> {
    if segment.is_empty() || segment == "." || segment == ".." {
        return Err(Error::Protocol(format!(
            "invalid path segment: {segment:?}"
        )));
    }
    Ok(utf8_percent_encode(segment, SEGMENT).to_string())
}

/// Validates and percent-encodes each segment of a slash-separated path,
/// keeping the slashes. Strips a single leading `/` and rejects empty paths or
/// any `""`, `"."`, or `".."` segment.
pub(crate) fn enc_path(path: &str) -> Result<String> {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    if trimmed.is_empty() {
        return Err(Error::Protocol("file path must not be empty".into()));
    }
    let encoded: Result<Vec<String>> = trimmed.split('/').map(enc_segment).collect();
    Ok(encoded?.join("/"))
}

/// Normalizes a configured base URL by trimming whitespace, trailing slashes,
/// and any trailing `/v1` suffix.
pub(crate) fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    trimmed
        .strip_suffix("/v1")
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string()
}

/// Normalizes a `/v1`-relative API path so it always starts with a single `/`.
pub(crate) fn normalize_api_path(path: &str) -> Cow<'_, str> {
    if path.starts_with('/') && !path.starts_with("//") {
        Cow::Borrowed(path)
    } else {
        Cow::Owned(format!("/{}", path.trim_start_matches('/')))
    }
}

fn is_https_url(url: &str) -> bool {
    url.len() >= 8 && url[..8].eq_ignore_ascii_case("https://")
}

/// The Authorization header for a token. Surrounding whitespace, such as the
/// newline a token file or secret store adds, is removed. Empty or
/// whitespace-only tokens are rejected.
pub(crate) fn authorization(token: &str) -> std::result::Result<HeaderValue, InvalidHeaderValue> {
    let token = token.trim();
    if token.is_empty() {
        return Err(HeaderValue::from_bytes(b"\n").unwrap_err());
    }
    let mut value = HeaderValue::try_from(format!("Bearer {token}"))?;
    value.set_sensitive(true);
    Ok(value)
}

/// Maps a non-2xx response to an [`ApiError`]; `path` names the request.
pub(crate) fn api_error(status: u16, text: &str, headers: &HeaderMap, path: &str) -> ApiError {
    parse_api_error(status, text, headers).with_request_path(path)
}

/// Reads at most [`MAX_ERROR_BODY_READ_BYTES`] from an error response body.
pub(crate) async fn read_error_body(response: Response) -> String {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        let remaining = MAX_ERROR_BODY_READ_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            break;
        }
        let take = chunk.len().min(remaining);
        body.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            break;
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

fn jittered_retry_after(seconds: u64) -> Duration {
    let capped = seconds.min(MAX_RETRY_AFTER_SECONDS);
    let base = Duration::from_secs(capped);
    let max_jitter = Duration::from_secs_f64((base.as_secs_f64() * 0.25).min(1.0));
    base.saturating_add(max_jitter.mul_f64(jitter()))
        .min(Duration::from_secs(MAX_RETRY_AFTER_SECONDS))
}

fn retry_delay(
    method: &Method,
    error: &ApiError,
    attempt: u32,
    timeout: Duration,
) -> Option<Duration> {
    let idempotent = matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE
    );
    let delay = match (error.status_code, error.retry_after) {
        (429, Some(0)) => Some(backoff(attempt, None)),
        (429, Some(seconds)) => Some(jittered_retry_after(seconds)),
        (503, retry_after)
            if *method == Method::POST
                && error.code.as_deref() == Some("event_publish_in_progress") =>
        {
            Some(match retry_after {
                Some(seconds) if seconds > 0 => jittered_retry_after(seconds),
                _ => backoff(attempt, None),
            })
        }
        (502..=504, retry_after) if idempotent => Some(backoff(
            attempt,
            retry_after.map(|seconds| seconds.min(MAX_UNAVAILABLE_RETRY_AFTER_SECONDS)),
        )),
        _ => None,
    };
    delay.map(|d| d.min(timeout))
}

pub(crate) struct Transport {
    // None when the token is empty or not a valid header value: authenticated
    // requests then fail before they are sent instead of going out without
    // credentials.
    authorization: Option<HeaderValue>,
    base_url: String,
    api_base_url: String,
    client: reqwest::Client,
    timeout: Duration,
}

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transport")
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}

impl Transport {
    pub(crate) fn new(
        authorization: Option<HeaderValue>,
        base_url: String,
        timeout: Option<Duration>,
        client: Option<reqwest::Client>,
    ) -> Transport {
        let base_url = normalize_base_url(&base_url);
        let client = client.unwrap_or_else(|| {
            reqwest::Client::builder()
                .https_only(is_https_url(&base_url))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default()
        });
        Transport {
            authorization,
            api_base_url: format!("{base_url}/v1"),
            base_url,
            client,
            timeout: timeout.unwrap_or(DEFAULT_TIMEOUT),
        }
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns an authorized request builder for a path relative to /v1.
    pub(crate) fn request_builder(&self, method: Method, path: &str) -> RequestBuilder {
        let builder = self
            .client
            .request(method, format!("{}{}", self.api_base_url, path))
            .header(USER_AGENT, USER_AGENT_VALUE);
        match &self.authorization {
            Some(authorization) => builder.header(AUTHORIZATION, authorization.clone()),
            // An invalid header value makes reqwest fail the request with a
            // builder error when it is sent.
            None => builder.header(AUTHORIZATION, "\n"),
        }
    }

    /// Sends a request and maps non-2xx responses to [`ApiError`]. The
    /// response body is left unread for the caller.
    pub(crate) async fn execute(&self, builder: RequestBuilder) -> Result<Response> {
        let response = builder.send().await?;
        let status = response.status();
        if !status.is_success() {
            let headers = response.headers().clone();
            let path = response.url().path().to_string();
            let text = read_error_body(response).await;
            return Err(Error::Api(api_error(
                status.as_u16(),
                &text,
                &headers,
                &path,
            )));
        }
        Ok(response)
    }

    /// Calls `GET {base_url}/status` without sending an `Authorization` header
    /// and returns the unwrapped JSON response value.
    pub(crate) async fn status(&self) -> Result<Value> {
        let response = self
            .client
            .request(Method::GET, format!("{}/status", self.base_url))
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header(ACCEPT, "application/json")
            .timeout(self.timeout)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let headers = response.headers().clone();
            let text = read_error_body(response).await;
            return Err(Error::Api(api_error(
                status.as_u16(),
                &text,
                &headers,
                "/status",
            )));
        }
        let text = response.text().await?;
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str::<Value>(&text)?)
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
        let normalized_path = normalize_api_path(path);
        let mut attempts: u32 = 0;
        let response = loop {
            let mut builder = self
                .request_builder(method.clone(), &normalized_path)
                .header(ACCEPT, "application/json")
                .timeout(self.timeout);
            if !query.is_empty() {
                builder = builder.query(query);
            }
            if let Some(body) = body {
                builder = builder.header(CONTENT_TYPE, "application/json").json(body);
            }

            let response = builder.send().await?;
            let status = response.status();
            if !status.is_success() {
                let headers = response.headers().clone();
                let text = read_error_body(response).await;
                let error = api_error(status.as_u16(), &text, &headers, &normalized_path);
                if attempts < MAX_RETRIES {
                    if let Some(delay) = retry_delay(&method, &error, attempts + 1, self.timeout) {
                        attempts += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
                return Err(Error::Api(error));
            }
            break response;
        };
        let text = response.text().await?;
        if text.trim().is_empty() {
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

/// Decodes a JSON response text and returns its `"data"` object, treating an
/// empty (for example HTTP 204) body as an empty object.
pub(crate) fn decode_data_text(text: &str) -> Result<JsonObject> {
    if text.trim().is_empty() {
        return Ok(JsonObject::new());
    }
    let envelope = match serde_json::from_str::<Value>(text)? {
        Value::Object(envelope) => envelope,
        _ => JsonObject::new(),
    };
    Ok(data_object(envelope))
}

/// Decodes a raw JSON response and returns its "data" object.
pub(crate) async fn decode_data_response(response: Response) -> Result<JsonObject> {
    let text = response.text().await?;
    decode_data_text(&text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enc_segment_rejects_empty_and_dot_segments() {
        assert!(matches!(enc_segment(""), Err(Error::Protocol(_))));
        assert!(matches!(enc_segment("."), Err(Error::Protocol(_))));
        assert!(matches!(enc_segment(".."), Err(Error::Protocol(_))));
        assert_eq!(enc_segment("a/b c").unwrap(), "a%2Fb%20c");
        assert_eq!(enc_segment("a.b").unwrap(), "a.b");
        assert_eq!(enc_segment("...").unwrap(), "...");
    }

    #[test]
    fn enc_path_strips_leading_slash_and_rejects_bad_segments() {
        assert_eq!(enc_path("/dir/file.txt").unwrap(), "dir/file.txt");
        assert_eq!(enc_path("dir/a b.txt").unwrap(), "dir/a%20b.txt");
        for bad in [
            "", "/", "//", "../x", "a/../b", "a/./b", "a//b", "a/", ".", "..",
        ] {
            assert!(
                matches!(enc_path(bad), Err(Error::Protocol(_))),
                "expected error for {bad:?}"
            );
        }
    }

    #[test]
    fn authorization_rejects_empty_whitespace_and_newlines() {
        assert!(authorization("").is_err());
        assert!(authorization("   \t\n  ").is_err());
        assert!(authorization("a\nb").is_err());
        let header = authorization("  tok_123\n").unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer tok_123");
        assert!(header.is_sensitive());
    }

    #[test]
    fn base_url_and_api_path_normalization() {
        assert_eq!(
            normalize_base_url("https://api.hot.dev"),
            "https://api.hot.dev"
        );
        assert_eq!(
            normalize_base_url("https://api.hot.dev/"),
            "https://api.hot.dev"
        );
        assert_eq!(
            normalize_base_url("https://api.hot.dev///"),
            "https://api.hot.dev"
        );
        assert_eq!(
            normalize_base_url("https://api.hot.dev/v1"),
            "https://api.hot.dev"
        );
        assert_eq!(
            normalize_base_url("https://api.hot.dev/v1/"),
            "https://api.hot.dev"
        );
        assert_eq!(
            normalize_base_url("https://proxy.example/hot/v1/"),
            "https://proxy.example/hot"
        );
        assert_eq!(
            normalize_base_url("https://proxy.example/myv1"),
            "https://proxy.example/myv1"
        );

        assert_eq!(normalize_api_path("/projects"), "/projects");
        assert_eq!(normalize_api_path("projects"), "/projects");
        assert_eq!(normalize_api_path("//projects"), "/projects");
        assert_eq!(normalize_api_path(""), "/");
    }

    #[test]
    fn decode_data_text_handles_empty_204_body() {
        assert!(decode_data_text("").unwrap().is_empty());
        assert!(decode_data_text("   \n ").unwrap().is_empty());
        let obj = decode_data_text(r#"{"data":{"id":"f1"}}"#).unwrap();
        assert_eq!(obj["id"], "f1");
        assert!(matches!(decode_data_text("not json"), Err(Error::Json(_))));
    }

    #[test]
    fn retry_delay_policies_and_timeout_bounding() {
        let timeout = Duration::from_secs(60);

        let err_429_none = ApiError {
            status_code: 429,
            message: "rate limited".into(),
            code: Some("rate_limited".into()),
            request_id: None,
            retry_after: None,
            error: None,
        };
        assert_eq!(retry_delay(&Method::GET, &err_429_none, 1, timeout), None);

        let err_429_zero = ApiError {
            retry_after: Some(0),
            ..err_429_none.clone()
        };
        let d0 = retry_delay(&Method::GET, &err_429_zero, 1, timeout).unwrap();
        assert!(d0 >= Duration::from_millis(250) && d0 <= Duration::from_millis(500));

        let err_429_two = ApiError {
            retry_after: Some(2),
            ..err_429_none.clone()
        };
        let d2 = retry_delay(&Method::GET, &err_429_two, 1, timeout).unwrap();
        assert!(d2 >= Duration::from_secs(2) && d2 <= Duration::from_millis(2500));

        // Bounded by client timeout.
        let bounded =
            retry_delay(&Method::GET, &err_429_two, 1, Duration::from_millis(200)).unwrap();
        assert_eq!(bounded, Duration::from_millis(200));

        // POST 503 event_publish_in_progress retries honoring retry_after capped at 30s.
        let err_publish = ApiError {
            status_code: 503,
            message: "in progress".into(),
            code: Some("event_publish_in_progress".into()),
            request_id: None,
            retry_after: Some(90),
            error: None,
        };
        assert_eq!(
            retry_delay(&Method::POST, &err_publish, 1, timeout),
            Some(Duration::from_secs(30))
        );

        let err_publish_unset = ApiError {
            retry_after: None,
            ..err_publish.clone()
        };
        let d_unset = retry_delay(&Method::POST, &err_publish_unset, 1, timeout).unwrap();
        assert!(d_unset >= Duration::from_millis(250) && d_unset <= Duration::from_millis(500));

        // Plain POST 503 and POST 409 event_id_conflict are NOT retried.
        let err_plain_503 = ApiError {
            code: None,
            ..err_publish
        };
        assert_eq!(retry_delay(&Method::POST, &err_plain_503, 1, timeout), None);
        let err_409 = ApiError {
            status_code: 409,
            message: "conflict".into(),
            code: Some("event_id_conflict".into()),
            request_id: None,
            retry_after: None,
            error: None,
        };
        assert_eq!(retry_delay(&Method::POST, &err_409, 1, timeout), None);
    }

    #[test]
    fn transport_debug_redacts_authorization() {
        let transport = Transport::new(
            authorization("hot_secret_value_123").ok(),
            DEFAULT_BASE_URL.to_string(),
            None,
            None,
        );
        let debug = format!("{transport:?}");
        assert!(!debug.contains("hot_secret_value_123"), "{debug}");
        assert!(debug.contains("[REDACTED]"), "{debug}");
    }
}
