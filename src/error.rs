use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::JsonObject;

/// Maximum bytes retained in [`ApiError::message`] for non-JSON error bodies.
const MAX_RAW_ERROR_BODY_BYTES: usize = 4096;

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
    RunWaitFailed {
        /// Human-readable error message extracted from the run result.
        message: String,
        /// The final durable run snapshot.
        run: JsonObject,
    },
    /// A task reached a failed, cancelled, or timed-out terminal state.
    #[error("{message}")]
    TaskFailed {
        /// Human-readable error message extracted from the task result.
        message: String,
        /// The final durable task snapshot.
        task: JsonObject,
    },
    /// Timed out waiting for a run result.
    #[error("timeout waiting for run result")]
    Timeout,
    /// Timed out waiting for a task result.
    #[error("timeout waiting for task")]
    TaskTimeout,
    /// A protocol or local validation error (for example an invalid path
    /// segment, an oversized SSE frame, or a stream ending before the expected
    /// event arrived).
    #[error("{0}")]
    Protocol(String),
}

/// Structured error for non-2xx Hot API responses.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// HTTP status code returned by the server.
    pub status_code: u16,
    /// Human-readable error message.
    pub message: String,
    /// Machine-readable Hot API error code, if present.
    pub code: Option<String>,
    /// Server request identifier for support and tracing, if present.
    pub request_id: Option<String>,
    /// Server-suggested retry delay in seconds, from the error body or
    /// `Retry-After` header.
    pub retry_after: Option<u64>,
    /// The raw `"error"` object from the response body, if any.
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
    let now_secs = current_unix_secs();

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
                retry_after: parse_retry_after(error.get("retry_after"), headers, now_secs),
                error: Some(error.clone()),
            }
        }
        None => {
            let message = if text.is_empty() {
                format!("Hot API error ({status_code})")
            } else if text.len() > MAX_RAW_ERROR_BODY_BYTES {
                format!("{}...", truncate_utf8(text, MAX_RAW_ERROR_BODY_BYTES))
            } else {
                text.to_string()
            };
            ApiError {
                status_code,
                message,
                code: None,
                request_id: None,
                retry_after: parse_retry_after(None, headers, now_secs),
                error: None,
            }
        }
    }
}

fn truncate_utf8(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn stringified(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    }
}

fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs().min(i64::MAX as u64) as i64)
}

fn parse_retry_after(value: Option<&Value>, headers: &HeaderMap, now_secs: i64) -> Option<u64> {
    if let Some(seconds) = value.and_then(|v| parse_retry_after_value(v, now_secs)) {
        return Some(seconds);
    }
    headers
        .get("retry-after")
        .and_then(|header| header.to_str().ok())
        .and_then(|header| parse_retry_after_str(header, now_secs))
}

fn parse_retry_after_value(value: &Value, now_secs: i64) -> Option<u64> {
    if let Some(seconds) = value.as_u64() {
        return Some(seconds);
    }
    if let Some(seconds) = value.as_f64() {
        return ceil_non_negative_seconds(seconds);
    }
    if let Some(text) = value.as_str() {
        return parse_retry_after_str(text, now_secs);
    }
    None
}

fn parse_retry_after_str(raw: &str, now_secs: i64) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(seconds);
    }
    if let Ok(seconds) = trimmed.parse::<f64>() {
        return ceil_non_negative_seconds(seconds);
    }
    parse_http_date_secs(trimmed).map(|target| {
        if target > now_secs {
            (target - now_secs) as u64
        } else {
            0
        }
    })
}

fn ceil_non_negative_seconds(seconds: f64) -> Option<u64> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let ceiled = seconds.ceil();
    if ceiled >= u64::MAX as f64 {
        Some(u64::MAX)
    } else {
        Some(ceiled as u64)
    }
}

/// Parses an RFC 9110 §5.6.7 HTTP-date (`IMF-fixdate`, RFC 850, or `asctime`)
/// into UTC Unix seconds.
fn parse_http_date_secs(input: &str) -> Option<i64> {
    let input = input.trim();
    if let Some((_, rest)) = input.split_once(',') {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() == 5 && parts[4] == "GMT" {
            // IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT"
            let day: u32 = parts[0].parse().ok()?;
            let month = parse_month(parts[1])?;
            let year: i64 = parts[2].parse().ok()?;
            let (hour, min, sec) = parse_hms(parts[3])?;
            return civil_to_unix_secs(year, month, day, hour, min, sec);
        }
        if parts.len() == 3 && parts[2] == "GMT" {
            // RFC 850: "Sunday, 06-Nov-94 08:49:37 GMT"
            let mut date_parts = parts[0].split('-');
            let day: u32 = date_parts.next()?.parse().ok()?;
            let month = parse_month(date_parts.next()?)?;
            let year_str = date_parts.next()?;
            if date_parts.next().is_some() || year_str.len() != 2 {
                return None;
            }
            let yy: i64 = year_str.parse().ok()?;
            let year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
            let (hour, min, sec) = parse_hms(parts[1])?;
            return civil_to_unix_secs(year, month, day, hour, min, sec);
        }
        return None;
    }

    // asctime: "Sun Nov  6 08:49:37 1994"
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.len() == 5 {
        let month = parse_month(parts[1])?;
        let day: u32 = parts[2].parse().ok()?;
        let (hour, min, sec) = parse_hms(parts[3])?;
        let year: i64 = parts[4].parse().ok()?;
        return civil_to_unix_secs(year, month, day, hour, min, sec);
    }
    None
}

fn parse_month(name: &str) -> Option<u32> {
    match name {
        "Jan" => Some(1),
        "Feb" => Some(2),
        "Mar" => Some(3),
        "Apr" => Some(4),
        "May" => Some(5),
        "Jun" => Some(6),
        "Jul" => Some(7),
        "Aug" => Some(8),
        "Sep" => Some(9),
        "Oct" => Some(10),
        "Nov" => Some(11),
        "Dec" => Some(12),
        _ => None,
    }
}

fn parse_hms(hms: &str) -> Option<(u32, u32, u32)> {
    let mut parts = hms.split(':');
    let hour: u32 = parts.next()?.parse().ok()?;
    let min: u32 = parts.next()?.parse().ok()?;
    let sec: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    Some((hour, min, sec.min(59)))
}

fn civil_to_unix_secs(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: u32,
) -> Option<i64> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u64;
    let m = month as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + (doe as i64) - 719_468;
    days.checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3600 + i64::from(min) * 60 + i64::from(sec))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
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
        let path = path.split('?').next().unwrap_or(path);
        let path = path.find("/v1/").map_or(path, |index| &path[index + 3..]);
        let first_segment = path.strip_prefix('/').unwrap_or(path).split('/').next();
        if matches!(first_segment, Some("orgs" | "api-keys")) {
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

    /// Durable waiters stop on these instead of reconnecting: 3xx redirects,
    /// every 4xx except 408 and 429, a request that could not be built,
    /// invalid JSON responses, and run or task outcomes. Transport failures,
    /// 408, 429, 5xx, and early stream ends are transient.
    pub(crate) fn is_permanent(&self) -> bool {
        match self {
            Error::Api(error) => {
                (300..500).contains(&error.status_code) && !matches!(error.status_code, 408 | 429)
            }
            Error::Http(error) => error.is_builder(),
            Error::Protocol(_) => false,
            Error::Json(_)
            | Error::RunFailed(_)
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

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;
    use serde_json::json;

    use super::*;

    #[test]
    fn key_scope_checks_segment_boundaries() {
        assert_eq!(KeyScope::for_path("/orgs"), KeyScope::Org);
        assert_eq!(KeyScope::for_path("/orgs/o1"), KeyScope::Org);
        assert_eq!(KeyScope::for_path("/v1/orgs?limit=10"), KeyScope::Org);
        assert_eq!(KeyScope::for_path("/api-keys"), KeyScope::Org);
        assert_eq!(KeyScope::for_path("/v1/api-keys/k1"), KeyScope::Org);
        assert_eq!(KeyScope::for_path("/orgs-foo"), KeyScope::Env);
        assert_eq!(KeyScope::for_path("/v1/orgs-foo/bar"), KeyScope::Env);
        assert_eq!(KeyScope::for_path("/api-keys-v2"), KeyScope::Env);
        assert_eq!(KeyScope::for_path("/v1/org/usage"), KeyScope::Env);
        assert_eq!(KeyScope::for_path("/v1/env"), KeyScope::Env);
    }

    #[test]
    fn error_permanence_classification() {
        let redirect = Error::Api(parse_api_error(302, "Found", &HeaderMap::new()));
        assert!(redirect.is_permanent());
        let bad_req = Error::Api(parse_api_error(400, "", &HeaderMap::new()));
        assert!(bad_req.is_permanent());
        let timeout_408 = Error::Api(parse_api_error(408, "", &HeaderMap::new()));
        assert!(!timeout_408.is_permanent());
        let rate_limit = Error::Api(parse_api_error(429, "", &HeaderMap::new()));
        assert!(!rate_limit.is_permanent());
        let server_err = Error::Api(parse_api_error(503, "", &HeaderMap::new()));
        assert!(!server_err.is_permanent());

        let json_err = Error::Json(serde_json::from_str::<Value>("not json").unwrap_err());
        assert!(json_err.is_permanent());
        assert!(!Error::Protocol("early end".into()).is_permanent());
    }

    #[test]
    fn non_json_error_body_is_capped_on_char_boundary() {
        let body = "é".repeat(MAX_RAW_ERROR_BODY_BYTES); // 2 bytes per char
        let error = parse_api_error(502, &body, &HeaderMap::new());
        assert!(error.message.ends_with("..."));
        assert!(error.message.len() <= MAX_RAW_ERROR_BODY_BYTES + 3);
    }

    #[test]
    fn retry_after_handles_floats_whitespace_and_http_dates() {
        let headers = HeaderMap::new();
        assert_eq!(
            parse_retry_after(Some(&json!(0.2)), &headers, 1000),
            Some(1)
        );
        assert_eq!(
            parse_retry_after(Some(&json!(1.5)), &headers, 1000),
            Some(2)
        );
        assert_eq!(
            parse_retry_after(Some(&json!(0.0)), &headers, 1000),
            Some(0)
        );
        assert_eq!(parse_retry_after(Some(&json!(-1.0)), &headers, 1000), None);
        assert_eq!(
            parse_retry_after(Some(&json!(" 7 ")), &headers, 1000),
            Some(7)
        );

        let mut trimmed_headers = HeaderMap::new();
        trimmed_headers.insert("retry-after", HeaderValue::from_static("  12  "));
        assert_eq!(parse_retry_after(None, &trimmed_headers, 1000), Some(12));

        // Wed, 21 Oct 2015 07:28:00 GMT == 1445412480
        assert_eq!(
            parse_http_date_secs("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(1_445_412_480)
        );
        // Sun, 06 Nov 1994 08:49:37 GMT == 784111777
        assert_eq!(
            parse_http_date_secs("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777)
        );
        assert_eq!(
            parse_http_date_secs("Sunday, 06-Nov-94 08:49:37 GMT"),
            Some(784_111_777)
        );
        assert_eq!(
            parse_http_date_secs("Sun Nov  6 08:49:37 1994"),
            Some(784_111_777)
        );

        let mut date_headers = HeaderMap::new();
        date_headers.insert(
            "retry-after",
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert_eq!(
            parse_retry_after(None, &date_headers, 1_445_412_470),
            Some(10)
        );
        // Past HTTP-date resolves to 0 seconds.
        assert_eq!(
            parse_retry_after(None, &date_headers, 1_445_412_500),
            Some(0)
        );
    }
}
