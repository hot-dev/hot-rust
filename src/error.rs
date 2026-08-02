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
