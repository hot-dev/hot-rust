use std::fmt;

use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::JsonObject;

/// The error type for SDK operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Hot API returned a non-2xx response.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// The HTTP request itself failed.
    #[error("transport error: {0}")]
    Http(#[from] reqwest::Error),
    /// A response body was not valid JSON.
    #[error("invalid JSON response: {0}")]
    Json(#[from] serde_json::Error),
    /// A run ended with `run:fail` or `run:cancel`; carries the run's result
    /// message.
    #[error("{0}")]
    RunFailed(String),
    /// A waited run reached a failed or cancelled terminal state.
    #[error("{message}")]
    RunWaitFailed { message: String, run: JsonObject },
    /// A task reached a failed, cancelled, or timed-out terminal state.
    #[error("{message}")]
    TaskFailed { message: String, task: JsonObject },
    /// Timed out waiting for a run result.
    #[error("timeout waiting for run result")]
    Timeout,
    /// Timed out waiting for a task result.
    #[error("timeout waiting for task")]
    TaskTimeout,
    /// The stream ended before the expected event arrived.
    #[error("{0}")]
    Protocol(String),
}

/// Structured error for non-2xx Hot API responses.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status_code: u16,
    pub message: String,
    pub code: Option<String>,
    pub request_id: Option<String>,
    /// Server-suggested retry delay in seconds, from the error body or
    /// Retry-After header.
    pub retry_after: Option<u64>,
    /// The raw "error" object from the response body, if any.
    pub error: Option<JsonObject>,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (status {})", self.message, self.status_code)
    }
}

impl std::error::Error for ApiError {}

pub(crate) fn parse_api_error(status_code: u16, text: &str, headers: &HeaderMap) -> ApiError {
    let parsed: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    let error_object = parsed.get("error").and_then(Value::as_object);

    match error_object {
        Some(error) => {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("Hot API error ({status_code})"));
            ApiError {
                status_code,
                message,
                code: stringified(error.get("code")),
                request_id: stringified(error.get("request_id")),
                retry_after: retry_after(error.get("retry_after"), headers),
                error: Some(error.clone()),
            }
        }
        None => {
            let message = if text.is_empty() {
                format!("Hot API error ({status_code})")
            } else {
                text.to_string()
            };
            ApiError {
                status_code,
                message,
                code: None,
                request_id: None,
                retry_after: retry_after(None, headers),
                error: None,
            }
        }
    }
}

fn stringified(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    }
}

fn retry_after(value: Option<&Value>, headers: &HeaderMap) -> Option<u64> {
    if let Some(seconds) = value.and_then(Value::as_u64) {
        return Some(seconds);
    }
    headers
        .get("retry-after")
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.parse().ok())
}

/// The kind of API key a route accepts. Org API keys call only `/v1/orgs`
/// and `/v1/api-keys`; environment keys, sessions, and service keys call
/// every other route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyScope {
    /// An org API key.
    Org,
    /// An environment API key (or a session or service key it issued).
    Env,
}

impl KeyScope {
    /// The wire name: `"org"` or `"env"`.
    pub fn as_str(self) -> &'static str {
        match self {
            KeyScope::Org => "org",
            KeyScope::Env => "env",
        }
    }

    /// The scope a request path needs; `path` may include the `/v1` prefix.
    pub(crate) fn for_path(path: &str) -> KeyScope {
        let path = path.find("/v1/").map_or(path, |index| &path[index + 3..]);
        if path.starts_with("/orgs") || path.starts_with("/api-keys") {
            KeyScope::Org
        } else {
            KeyScope::Env
        }
    }

    fn message(self) -> &'static str {
        match self {
            KeyScope::Org => concat!(
                "This call needs an org API key; environment keys, sessions ",
                "and service keys call runtime routes"
            ),
            KeyScope::Env => concat!(
                "This call needs an environment API key; org API keys only call ",
                "/v1/orgs and /v1/api-keys"
            ),
        }
    }
}

impl fmt::Display for KeyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

const KEY_SCOPE_MISMATCH: &str = "key_scope_mismatch";

impl ApiError {
    /// For a `403 key_scope_mismatch` response, the kind of key the call
    /// needs: the client's credential is the other kind.
    pub fn required_scope(&self) -> Option<KeyScope> {
        if self.code.as_deref() != Some(KEY_SCOPE_MISMATCH) {
            return None;
        }
        if self.message.contains("needs an org API key") {
            Some(KeyScope::Org)
        } else if self.message.contains("needs an environment API key") {
            Some(KeyScope::Env)
        } else {
            None
        }
    }

    /// Names the needed key in `message` for a key-scope mismatch; the
    /// server's own message stays in `error`.
    pub(crate) fn with_request_path(mut self, path: &str) -> ApiError {
        if self.code.as_deref() == Some(KEY_SCOPE_MISMATCH) {
            self.message = KeyScope::for_path(path).message().to_string();
        }
        self
    }
}

impl Error {
    /// For a `403 key_scope_mismatch` API error, the kind of key the call
    /// needs. See [`ApiError::required_scope`].
    pub fn required_scope(&self) -> Option<KeyScope> {
        match self {
            Error::Api(error) => error.required_scope(),
            _ => None,
        }
    }

    /// Durable waiters stop on these instead of reconnecting: every 4xx except
    /// 408 and 429, a request that could not be built, and run or task
    /// outcomes. Transport failures, 408, 429, 5xx, and early stream ends are
    /// transient.
    pub(crate) fn is_permanent(&self) -> bool {
        match self {
            Error::Api(error) => {
                (400..500).contains(&error.status_code) && !matches!(error.status_code, 408 | 429)
            }
            Error::Http(error) => error.is_builder(),
            Error::Json(_) | Error::Protocol(_) => false,
            Error::RunFailed(_)
            | Error::RunWaitFailed { .. }
            | Error::TaskFailed { .. }
            | Error::Timeout
            | Error::TaskTimeout => true,
        }
    }

    /// The server's suggested retry delay in seconds, if any.
    pub(crate) fn retry_after(&self) -> Option<u64> {
        match self {
            Error::Api(error) => error.retry_after,
            _ => None,
        }
    }
}
