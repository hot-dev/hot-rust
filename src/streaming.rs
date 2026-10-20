use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::header::ACCEPT;
use reqwest::Method;
use serde_json::Value;

use crate::error::parse_api_error;
use crate::sse::consume_sse_blocks;
use crate::transport::{enc, Transport};
use crate::{Error, JsonObject, StreamEvent};

/// A stream of Hot run-stream events.
pub type EventStream = Pin<Box<dyn Stream<Item = crate::Result<StreamEvent>> + Send>>;

/// Convenience accessors for [`StreamEvent`] wire-format fields.
pub trait StreamEventExt {
    /// The event's "type" field, or "" when absent.
    fn event_type(&self) -> &str;
    /// The event's "run" object, when present.
    fn run(&self) -> Option<&JsonObject>;
    /// run.run_id, when present.
    fn run_id(&self) -> Option<&str>;
}

impl StreamEventExt for StreamEvent {
    fn event_type(&self) -> &str {
        self.get("type").and_then(Value::as_str).unwrap_or("")
    }

    fn run(&self) -> Option<&JsonObject> {
        self.get("run").and_then(Value::as_object)
    }

    fn run_id(&self) -> Option<&str> {
        self.run()?.get("run_id")?.as_str()
    }
}

/// Opens an SSE request and yields decoded events. The stream ends after
/// yielding an error.
pub(crate) fn iter_stream(
    transport: Arc<Transport>,
    method: Method,
    path: String,
    body: Option<Value>,
    query: Vec<(String, String)>,
) -> EventStream {
    Box::pin(async_stream::stream! {
        let mut builder = transport
            .request_builder(method, &path)
            .header(ACCEPT, "text/event-stream");
        if !query.is_empty() {
            builder = builder.query(&query);
        }
        if let Some(body) = &body {
            builder = builder.json(body);
        }

        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                yield Err(Error::Http(error));
                return;
            }
        };
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            let headers = response.headers().clone();
            let text = response.text().await.unwrap_or_default();
            yield Err(Error::Api(parse_api_error(status.as_u16(), &text, &headers)));
            return;
        }

        // Bytes are decoded through a byte buffer so multi-byte UTF-8
        // sequences split across chunks survive intact.
        let mut pending: Vec<u8> = Vec::new();
        let mut buffer = String::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    yield Err(Error::Http(error));
                    return;
                }
            };
            pending.extend_from_slice(&chunk);
            let valid_len = match std::str::from_utf8(&pending) {
                Ok(_) => pending.len(),
                Err(error) => error.valid_up_to(),
            };
            buffer.push_str(std::str::from_utf8(&pending[..valid_len]).unwrap_or(""));
            pending.drain(..valid_len);

            let (events, rest) = consume_sse_blocks(&buffer);
            buffer = rest;
            for event in events {
                yield Ok(event);
            }
        }

        buffer.push_str("\n\n");
        let (events, _) = consume_sse_blocks(&buffer);
        for event in events {
            yield Ok(event);
        }
    })
}

pub(crate) async fn wait_for_run_result(
    transport: Arc<Transport>,
    stream_id: &str,
    event_id: &str,
    timeout: Duration,
    mut on_chunk: Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<JsonObject> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut current_run_id: Option<String> = None;
    for attempts in 0..=5 {
        let mut source = iter_stream(
            transport.clone(),
            Method::GET,
            format!("/streams/{}/subscribe", enc(stream_id)),
            None,
            Vec::new(),
        );
        loop {
            let item = match tokio::time::timeout_at(deadline, source.next()).await {
                Ok(item) => item,
                Err(_) => return Err(Error::Timeout),
            };
            let Some(item) = item else { break };
            match item {
                Ok(event) => {
                    if let Some(run) =
                        handle_run_event(&event, event_id, &mut current_run_id, &mut on_chunk)?
                    {
                        return Ok(run);
                    }
                }
                Err(_error) if attempts < 5 => break,
                Err(error) => return Err(error),
            }
        }
        if attempts == 5 {
            return Err(Error::Protocol("stream ended before run completed".into()));
        }
    }
    unreachable!()
}

pub(crate) async fn wait_for_call_result(
    transport: Arc<Transport>,
    body: JsonObject,
    timeout: Duration,
    mut on_chunk: Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<JsonObject> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut stream_id: Option<String> = None;
    let mut event_id: Option<String> = None;
    let mut current_run_id: Option<String> = None;

    for attempts in 0..=5 {
        let (method, path, request_body) = match stream_id.as_deref() {
            None => (
                Method::POST,
                "/streams/subscribe-with-event".to_string(),
                Some(Value::Object(body.clone())),
            ),
            Some(stream_id) => (
                Method::GET,
                format!("/streams/{}/subscribe", enc(stream_id)),
                None,
            ),
        };
        let mut source = iter_stream(transport.clone(), method, path, request_body, Vec::new());
        loop {
            let item = match tokio::time::timeout_at(deadline, source.next()).await {
                Ok(item) => item,
                Err(_) => return Err(Error::Timeout),
            };
            let Some(item) = item else { break };
            let event = match item {
                Ok(event) => event,
                Err(_error) if stream_id.is_some() && attempts < 5 => break,
                Err(error) => return Err(error),
            };
            if event.event_type() == "event:published" {
                stream_id = event
                    .get("stream_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                event_id = event
                    .get("event_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            if let Some(event_id) = event_id.as_deref() {
                if let Some(run) =
                    handle_run_event(&event, event_id, &mut current_run_id, &mut on_chunk)?
                {
                    return Ok(run);
                }
            }
        }
        if stream_id.is_none() || event_id.is_none() {
            return Err(Error::Protocol(
                "stream ended before event:published was received".into(),
            ));
        }
        if attempts == 5 {
            return Err(Error::Protocol("stream ended before run completed".into()));
        }
    }
    unreachable!()
}

pub(crate) async fn wait_for_task(
    transport: Arc<Transport>,
    task_id: &str,
    timeout: Duration,
    max_attempts: u32,
) -> crate::Result<JsonObject> {
    let deadline = tokio::time::Instant::now() + timeout;
    for attempts in 0..=max_attempts {
        let mut source = iter_stream(
            transport.clone(),
            Method::GET,
            format!("/tasks/{}/subscribe", enc(task_id)),
            None,
            Vec::new(),
        );
        loop {
            let item = match tokio::time::timeout_at(deadline, source.next()).await {
                Ok(item) => item,
                Err(_) => return Err(Error::TaskTimeout),
            };
            let Some(item) = item else { break };
            let event = match item {
                Ok(event) => event,
                Err(_error) if attempts < max_attempts => break,
                Err(error) => return Err(error),
            };
            if event.event_type() != "task:update" {
                continue;
            }
            let Some(task) = event.get("task").and_then(Value::as_object) else {
                continue;
            };
            match task.get("status").and_then(Value::as_str) {
                Some("completed") => return Ok(task.clone()),
                Some("failed" | "cancelled" | "timed_out") => {
                    let status = task
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("failed");
                    let message = result_message(task.get("result"))
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("task {status}"));
                    return Err(Error::TaskFailed {
                        message,
                        task: task.clone(),
                    });
                }
                _ => {}
            }
        }
        if attempts == max_attempts {
            return Err(Error::Protocol(
                "task subscription ended before the task completed".into(),
            ));
        }
    }
    unreachable!()
}

pub(crate) async fn wait_for_run(
    transport: Arc<Transport>,
    run_id: &str,
    timeout: Duration,
    max_attempts: u32,
) -> crate::Result<JsonObject> {
    let deadline = tokio::time::Instant::now() + timeout;
    for attempts in 0..=max_attempts {
        let mut source = iter_stream(
            transport.clone(),
            Method::GET,
            format!("/runs/{}/subscribe", enc(run_id)),
            None,
            Vec::new(),
        );
        loop {
            let item = match tokio::time::timeout_at(deadline, source.next()).await {
                Ok(item) => item,
                Err(_) => return Err(Error::Timeout),
            };
            let Some(item) = item else { break };
            let event = match item {
                Ok(event) => event,
                Err(_error) if attempts < max_attempts => break,
                Err(error) => return Err(error),
            };
            if event.event_type() != "run:update" {
                continue;
            }
            let Some(run) = event.get("run").and_then(Value::as_object) else {
                continue;
            };
            match run.get("status").and_then(Value::as_str) {
                Some("succeeded") => return Ok(run.clone()),
                Some(status @ ("failed" | "cancelled")) => {
                    let message = run_result_message(run, &format!("run {status}"));
                    return Err(Error::RunWaitFailed {
                        message,
                        run: run.clone(),
                    });
                }
                _ => {}
            }
        }
        if attempts == max_attempts {
            return Err(Error::Protocol(
                "run subscription ended before the run completed".into(),
            ));
        }
    }
    unreachable!()
}

fn handle_run_event(
    event: &StreamEvent,
    event_id: &str,
    current_run_id: &mut Option<String>,
    on_chunk: &mut Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<Option<JsonObject>> {
    match event.event_type() {
        "run:start" => {
            if event_id_of_run(event.run()) == Some(event_id) {
                *current_run_id = event.run_id().map(str::to_string);
            }
        }
        "stream:data" => {
            let matches_run = current_run_id.as_deref().is_some_and(|current| {
                event.get("run_id").and_then(Value::as_str) == Some(current)
            });
            if matches_run {
                if let Some(on_chunk) = on_chunk.as_deref_mut() {
                    if let Some(text) = event
                        .get("payload")
                        .and_then(Value::as_object)
                        .and_then(|payload| payload.get("text"))
                        .and_then(Value::as_str)
                    {
                        on_chunk(text);
                    }
                }
            }
        }
        event_type @ ("run:stop" | "run:fail" | "run:cancel") => {
            let Some(run) = event.run() else {
                return Ok(None);
            };
            if event_id_of_run(Some(run)) != Some(event_id) {
                return Ok(None);
            }
            match event_type {
                "run:fail" => return Err(Error::RunFailed(run_result_message(run, "run failed"))),
                "run:cancel" => {
                    return Err(Error::RunFailed(run_result_message(run, "run cancelled")))
                }
                _ => return Ok(Some(run.clone())),
            }
        }
        _ => {}
    }
    Ok(None)
}

pub(crate) fn event_id_of_run(run: Option<&JsonObject>) -> Option<&str> {
    run?.get("event_id")?.as_str()
}

/// Digs a human-readable message out of a run result value.
fn result_message(value: Option<&Value>) -> Option<&str> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text),
        Value::Object(object) => ["$err", "$val", "error", "message", "msg", "reason", "err"]
            .iter()
            .find_map(|key| result_message(object.get(*key))),
        _ => None,
    }
}

pub(crate) fn run_result_message(run: &JsonObject, fallback: &str) -> String {
    if let Some(message) = result_message(run.get("result")) {
        return message.to_string();
    }
    match run.get("status").and_then(Value::as_str) {
        Some(status) if !status.is_empty() => format!("{fallback} ({status})"),
        _ => fallback.to_string(),
    }
}

/// Unwraps a `{"$ok": ...}` run result.
pub(crate) fn extract_run_result(result: Option<Value>) -> Value {
    match result {
        Some(Value::Object(mut object)) if object.contains_key("$ok") => {
            object.remove("$ok").unwrap_or(Value::Null)
        }
        Some(other) => other,
        None => Value::Null,
    }
}
