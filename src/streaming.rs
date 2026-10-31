use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::header::ACCEPT;
use reqwest::Method;
use serde_json::Value;

use tokio::time::Instant;

use crate::sse::consume_sse_blocks;
use crate::transport::{api_error, enc, Transport};
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
    open_stream(transport, method, path, body, query, None)
}

/// [`iter_stream`] for durable helpers: when `idle` is set, a connection that
/// sends nothing (not even a keepalive comment) for that long ends with a
/// transient [`Error::Protocol`], so a dead socket reconnects.
pub(crate) fn open_stream(
    transport: Arc<Transport>,
    method: Method,
    path: String,
    body: Option<Value>,
    query: Vec<(String, String)>,
    idle: Option<Duration>,
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

        let sent = match idle {
            Some(idle) => match tokio::time::timeout(idle, builder.send()).await {
                Ok(sent) => sent,
                Err(_) => {
                    yield Err(idle_error(idle));
                    return;
                }
            },
            None => builder.send().await,
        };
        let response = match sent {
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
            yield Err(Error::Api(api_error(status.as_u16(), &text, &headers, &path)));
            return;
        }

        // Bytes are decoded through a byte buffer so multi-byte UTF-8
        // sequences split across chunks survive intact.
        let mut pending: Vec<u8> = Vec::new();
        let mut buffer = String::new();
        let mut chunks = response.bytes_stream();
        loop {
            let next = match idle {
                Some(idle) => match tokio::time::timeout(idle, chunks.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        yield Err(idle_error(idle));
                        return;
                    }
                },
                None => chunks.next().await,
            };
            let Some(chunk) = next else { break };
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

fn idle_error(idle: Duration) -> Error {
    Error::Protocol(format!("stream sent nothing for {} s", idle.as_secs()))
}

/// Durable helpers treat a connection silent for this long as dead. Hot
/// sends a keepalive comment every 15 s.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Consecutive failed connections the run-result helpers tolerate.
const MAX_RECONNECTS: u32 = 5;

const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// The delay before consecutive failure `failures` (1-based): half of
/// `min(10 s, 500 ms * 2^(failures-1))` plus a random part of the other half,
/// and at least the server's `retry_after` seconds.
pub(crate) fn backoff(failures: u32, retry_after: Option<u64>) -> Duration {
    let doubled = BACKOFF_BASE.saturating_mul(1u32 << failures.saturating_sub(1).min(16));
    let half = doubled.min(BACKOFF_MAX) / 2;
    let delay = half + half.mul_f64(jitter());
    match retry_after {
        Some(seconds) => delay.max(Duration::from_secs(seconds).min(MAX_RETRY_AFTER)),
        None => delay,
    }
}

/// A uniform value in [0, 1), from the standard library's randomly keyed
/// hasher, so jitter needs no extra dependency.
fn jitter() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    );
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// What a connection delivered before it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    Nothing,
    Events,
    /// It ended with `stream:complete`, the server's 5-minute rollover.
    Rollover,
}

impl Progress {
    pub(crate) fn after(self, event: &StreamEvent) -> Progress {
        if event.event_type() == "stream:complete" {
            Progress::Rollover
        } else if self == Progress::Nothing {
            Progress::Events
        } else {
            self
        }
    }
}

/// The reconnect policy every durable helper shares. Permanent errors stop
/// at once. A connection that delivered events resets the failure count, and
/// a clean rollover (`stream:complete`) reconnects without delay. Otherwise
/// each consecutive failure waits with jittered exponential backoff,
/// honouring `retry_after`, until `max_attempts` reconnects have failed in a
/// row or the deadline would pass.
pub(crate) struct Reconnect {
    max_attempts: u32,
    failures: u32,
    deadline: Option<Instant>,
}

impl Reconnect {
    pub(crate) fn new(max_attempts: u32, deadline: Option<Instant>) -> Reconnect {
        Reconnect {
            max_attempts,
            failures: 0,
            deadline,
        }
    }

    /// Decides what follows a connection that ended with `error` (None for a
    /// clean end) after `progress`. Returns after any backoff when the caller
    /// should reconnect; otherwise returns the error to report: `error`, or
    /// `ended()` for a clean end, or `timeout()` when the backoff would pass
    /// the deadline.
    pub(crate) async fn next(
        &mut self,
        progress: Progress,
        error: Option<Error>,
        ended: impl FnOnce() -> Error,
        timeout: impl FnOnce() -> Error,
    ) -> crate::Result<()> {
        if error.as_ref().is_some_and(Error::is_permanent) {
            return Err(error.unwrap_or_else(ended));
        }
        if progress != Progress::Nothing {
            self.failures = 0;
        }
        if progress == Progress::Rollover && error.is_none() {
            return Ok(());
        }
        self.failures += 1;
        if self.failures > self.max_attempts {
            return Err(error.unwrap_or_else(ended));
        }
        let delay = backoff(self.failures, error.as_ref().and_then(Error::retry_after));
        if let Some(deadline) = self.deadline {
            if Instant::now() + delay >= deadline {
                return Err(timeout());
            }
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

/// How one connection of a durable helper ended.
enum Connection<T> {
    /// The helper's result arrived.
    Done(T),
    /// The connection ended without a result.
    Ended {
        progress: Progress,
        error: Option<Error>,
    },
}

/// What a durable helper does with the events of its connections.
trait Waiter {
    type Output;

    /// Handles one event; `Some` ends the wait with that result.
    fn event(&mut self, event: &StreamEvent) -> crate::Result<Option<Self::Output>>;

    /// When set, [`Waiter::wake`] runs at this instant (or at the deadline,
    /// if sooner) unless an event arrives first.
    fn wake_at(&self) -> Option<Instant> {
        None
    }

    fn wake(&mut self) -> crate::Result<Option<Self::Output>> {
        Ok(None)
    }
}

/// A waiter over durable snapshots (`run:update`, `task:update`).
struct Snapshots<F>(F);

impl<F> Waiter for Snapshots<F>
where
    F: FnMut(&StreamEvent) -> crate::Result<Option<JsonObject>>,
{
    type Output = JsonObject;

    fn event(&mut self, event: &StreamEvent) -> crate::Result<Option<JsonObject>> {
        (self.0)(event)
    }
}

/// Feeds one connection's events to `waiter` until it returns a result, the
/// connection ends, or the deadline passes (`timeout()`).
async fn consume<W: Waiter>(
    mut source: EventStream,
    deadline: Instant,
    timeout: fn() -> Error,
    waiter: &mut W,
) -> crate::Result<Connection<W::Output>> {
    let mut progress = Progress::Nothing;
    loop {
        let wake = waiter.wake_at();
        let until = wake.map_or(deadline, |wake| wake.min(deadline));
        let item = match tokio::time::timeout_at(until, source.next()).await {
            Ok(item) => item,
            Err(_) => {
                if wake.is_some() {
                    if let Some(result) = waiter.wake()? {
                        return Ok(Connection::Done(result));
                    }
                    if Instant::now() < deadline {
                        continue;
                    }
                }
                return Err(timeout());
            }
        };
        match item {
            None => {
                return Ok(Connection::Ended {
                    progress,
                    error: None,
                })
            }
            Some(Err(error)) => {
                return Ok(Connection::Ended {
                    progress,
                    error: Some(error),
                })
            }
            Some(Ok(event)) => {
                progress = progress.after(&event);
                if let Some(result) = waiter.event(&event)? {
                    return Ok(Connection::Done(result));
                }
            }
        }
    }
}

fn stream_path(stream_id: &str) -> String {
    format!("/streams/{}/subscribe", enc(stream_id))
}

pub(crate) async fn wait_for_run_result(
    transport: Arc<Transport>,
    stream_id: &str,
    event_id: &str,
    timeout: Duration,
    on_chunk: Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<JsonObject> {
    let deadline = Instant::now() + timeout;
    let mut tracker = RunTracker::new(Some(event_id.to_string()), on_chunk);
    let mut reconnect = Reconnect::new(MAX_RECONNECTS, Some(deadline));
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            stream_path(stream_id),
            None,
            Vec::new(),
            Some(IDLE_TIMEOUT),
        );
        match consume(source, deadline, || Error::Timeout, &mut tracker).await? {
            Connection::Done(run) => return Ok(run),
            Connection::Ended { progress, error } => {
                reconnect
                    .next(
                        progress,
                        error,
                        || Error::Protocol("stream ended before run completed".into()),
                        || Error::Timeout,
                    )
                    .await?
            }
        }
    }
}

pub(crate) async fn wait_for_call_result(
    transport: Arc<Transport>,
    body: JsonObject,
    timeout: Duration,
    on_chunk: Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<JsonObject> {
    let deadline = Instant::now() + timeout;
    let mut tracker = RunTracker::new(None, on_chunk);
    let mut stream_id: Option<String> = None;
    let mut reconnect = Reconnect::new(MAX_RECONNECTS, Some(deadline));
    loop {
        let (method, path, request_body) = match stream_id.as_deref() {
            None => (
                Method::POST,
                "/streams/subscribe-with-event".to_string(),
                Some(Value::Object(body.clone())),
            ),
            Some(stream_id) => (Method::GET, stream_path(stream_id), None),
        };
        let source = open_stream(
            transport.clone(),
            method,
            path,
            request_body,
            Vec::new(),
            Some(IDLE_TIMEOUT),
        );
        let outcome = consume(source, deadline, || Error::Timeout, &mut tracker).await?;
        if stream_id.is_none() {
            stream_id = tracker.stream_id.clone();
        }
        match outcome {
            Connection::Done(run) => return Ok(run),
            // Publishing again would duplicate the event, so only a
            // connection that confirmed the publish is resumed.
            Connection::Ended {
                error: Some(error), ..
            } if stream_id.is_none() => return Err(error),
            Connection::Ended { .. } if stream_id.is_none() || !tracker.published() => {
                return Err(Error::Protocol(
                    "stream ended before event:published was received".into(),
                ))
            }
            Connection::Ended { progress, error } => {
                reconnect
                    .next(
                        progress,
                        error,
                        || Error::Protocol("stream ended before run completed".into()),
                        || Error::Timeout,
                    )
                    .await?
            }
        }
    }
}

pub(crate) async fn wait_for_task(
    transport: Arc<Transport>,
    task_id: &str,
    timeout: Duration,
    max_attempts: u32,
) -> crate::Result<JsonObject> {
    let deadline = Instant::now() + timeout;
    let mut reconnect = Reconnect::new(max_attempts, Some(deadline));
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            format!("/tasks/{}/subscribe", enc(task_id)),
            None,
            Vec::new(),
            Some(IDLE_TIMEOUT),
        );
        let mut waiter = Snapshots(|event: &StreamEvent| {
            if event.event_type() != "task:update" {
                return Ok(None);
            }
            let Some(task) = event.get("task").and_then(Value::as_object) else {
                return Ok(None);
            };
            match task.get("status").and_then(Value::as_str) {
                Some("completed") => Ok(Some(task.clone())),
                Some(status @ ("failed" | "cancelled" | "timed_out")) => {
                    let message = result_message(task.get("result"))
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("task {status}"));
                    Err(Error::TaskFailed {
                        message,
                        task: task.clone(),
                    })
                }
                _ => Ok(None),
            }
        });
        let outcome = consume(source, deadline, || Error::TaskTimeout, &mut waiter).await?;
        match outcome {
            Connection::Done(task) => return Ok(task),
            Connection::Ended { progress, error } => {
                reconnect
                    .next(
                        progress,
                        error,
                        || {
                            Error::Protocol(
                                "task subscription ended before the task completed".into(),
                            )
                        },
                        || Error::TaskTimeout,
                    )
                    .await?
            }
        }
    }
}

pub(crate) async fn wait_for_run(
    transport: Arc<Transport>,
    run_id: &str,
    timeout: Duration,
    max_attempts: u32,
) -> crate::Result<JsonObject> {
    let deadline = Instant::now() + timeout;
    let mut reconnect = Reconnect::new(max_attempts, Some(deadline));
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            format!("/runs/{}/subscribe", enc(run_id)),
            None,
            Vec::new(),
            Some(IDLE_TIMEOUT),
        );
        let mut waiter = Snapshots(|event: &StreamEvent| {
            if event.event_type() != "run:update" {
                return Ok(None);
            }
            let Some(run) = event.get("run").and_then(Value::as_object) else {
                return Ok(None);
            };
            match run.get("status").and_then(Value::as_str) {
                Some("succeeded") => Ok(Some(run.clone())),
                Some(status @ ("failed" | "cancelled")) => Err(Error::RunWaitFailed {
                    message: run_result_message(run, &format!("run {status}")),
                    run: run.clone(),
                }),
                _ => Ok(None),
            }
        });
        let outcome = consume(source, deadline, || Error::Timeout, &mut waiter).await?;
        match outcome {
            Connection::Done(run) => return Ok(run),
            Connection::Ended { progress, error } => {
                reconnect
                    .next(
                        progress,
                        error,
                        || {
                            Error::Protocol(
                                "run subscription ended before the run completed".into(),
                            )
                        },
                        || Error::Timeout,
                    )
                    .await?
            }
        }
    }
}

/// Text chunks buffered for runs not yet tied to the published event are
/// capped at this many bytes in total; later chunks for such runs are
/// dropped.
const MAX_BUFFERED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// How long a failed run whose retry was scheduled waits for that retry run
/// to appear before its failure is reported.
const RETRY_GRACE: Duration = Duration::from_secs(5);

/// A failure that stands unless a retry of the failed run appears.
struct PendingFailure {
    run_id: String,
    until: Instant,
    error: Error,
}

/// Ties runs on a stream to one published event and delivers their text
/// chunks in order. A run belongs to the event when its `event_id` matches or
/// its `origin_run_id` names a run that does (a retry gets a new event id).
/// `stream:data` can arrive before `run:start` (and `run:start` after
/// `run:stop`), so chunks of unknown runs wait until their run is tied.
///
/// A run with status `pending_retry` is not terminal, whatever the event:
/// the retry run carries the result. Once the retry is queued the server
/// marks the failed run `failed` and replays it that way, so a failure whose
/// `retry_attempt` shows a scheduled retry waits [`RETRY_GRACE`] for the
/// retry run before it is reported.
pub(crate) struct RunTracker<'a> {
    event_id: Option<String>,
    pub(crate) stream_id: Option<String>,
    // Tied runs and the highest retry_attempt seen for each.
    runs: HashMap<String, i64>,
    // Tied runs that a tied retry run names as its origin.
    retried: HashSet<String>,
    pending_failure: Option<PendingFailure>,
    // Run events whose origin run is not tied yet, by origin_run_id.
    orphans: HashMap<String, Vec<(String, JsonObject)>>,
    chunks: HashMap<String, Vec<String>>,
    chunk_bytes: usize,
    on_chunk: Option<&'a mut (dyn FnMut(&str) + Send)>,
}

impl<'a> RunTracker<'a> {
    pub(crate) fn new(
        event_id: Option<String>,
        on_chunk: Option<&'a mut (dyn FnMut(&str) + Send)>,
    ) -> RunTracker<'a> {
        RunTracker {
            event_id,
            stream_id: None,
            runs: HashMap::new(),
            retried: HashSet::new(),
            pending_failure: None,
            orphans: HashMap::new(),
            chunks: HashMap::new(),
            chunk_bytes: 0,
            on_chunk,
        }
    }

    pub(crate) fn published(&self) -> bool {
        self.event_id.is_some()
    }

    /// Handles one event. Returns the run once a tied run stops; a tied run
    /// that fails for good or is cancelled becomes [`Error::RunFailed`].
    pub(crate) fn handle(&mut self, event: &StreamEvent) -> crate::Result<Option<JsonObject>> {
        match event.event_type() {
            "event:published" => {
                if self.event_id.is_none() {
                    self.event_id = event
                        .get("event_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    self.stream_id = event
                        .get("stream_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                Ok(None)
            }
            "stream:data" => {
                let Some(run_id) = event.get("run_id").and_then(Value::as_str) else {
                    return Ok(None);
                };
                let Some(text) = event
                    .get("payload")
                    .and_then(Value::as_object)
                    .and_then(|payload| payload.get("text"))
                    .and_then(Value::as_str)
                else {
                    return Ok(None);
                };
                if self.runs.contains_key(run_id) {
                    if let Some(on_chunk) = self.on_chunk.as_deref_mut() {
                        on_chunk(text);
                    }
                } else if self.on_chunk.is_some()
                    && self.chunk_bytes + text.len() <= MAX_BUFFERED_CHUNK_BYTES
                {
                    self.chunk_bytes += text.len();
                    self.chunks
                        .entry(run_id.to_string())
                        .or_default()
                        .push(text.to_string());
                }
                Ok(None)
            }
            event_type @ ("run:start" | "run:stop" | "run:fail" | "run:cancel") => {
                let Some(run) = event.run() else {
                    return Ok(None);
                };
                self.run_event(event_type, run)
            }
            _ => Ok(None),
        }
    }

    /// When a failure is waiting for its retry, the instant it stands.
    pub(crate) fn wake_at(&self) -> Option<Instant> {
        self.pending_failure.as_ref().map(|pending| pending.until)
    }

    /// Reports a failure whose retry did not appear in time.
    pub(crate) fn expire(&mut self) -> crate::Result<Option<JsonObject>> {
        match self.pending_failure.take() {
            Some(pending) => Err(pending.error),
            None => Ok(None),
        }
    }

    /// True when the run is tied to the published event, by event id or
    /// through its origin run.
    fn ties(&self, run: &JsonObject) -> bool {
        let by_event = self
            .event_id
            .as_deref()
            .is_some_and(|event_id| event_id_of_run(Some(run)) == Some(event_id));
        by_event || origin_of(run).is_some_and(|origin| self.runs.contains_key(origin))
    }

    fn run_event(
        &mut self,
        event_type: &str,
        run: &JsonObject,
    ) -> crate::Result<Option<JsonObject>> {
        if !self.ties(run) {
            if let Some(origin) = origin_of(run) {
                self.orphans
                    .entry(origin.to_string())
                    .or_default()
                    .push((event_type.to_string(), run.clone()));
            }
            return Ok(None);
        }
        let run_id = run.get("run_id").and_then(Value::as_str).unwrap_or("");
        let attempt = retry_attempt(run);
        let mut orphans = Vec::new();
        match self.runs.get_mut(run_id) {
            Some(seen) => *seen = (*seen).max(attempt),
            None => {
                self.runs.insert(run_id.to_string(), attempt);
                if let Some(chunks) = self.chunks.remove(run_id) {
                    for text in chunks {
                        self.chunk_bytes -= text.len();
                        if let Some(on_chunk) = self.on_chunk.as_deref_mut() {
                            on_chunk(&text);
                        }
                    }
                }
                orphans = self.orphans.remove(run_id).unwrap_or_default();
                if !orphans.is_empty() {
                    // A retry of this run is already on the stream.
                    self.retried.insert(run_id.to_string());
                }
            }
        }
        if let Some(origin) = origin_of(run) {
            self.retried.insert(origin.to_string());
            if self
                .pending_failure
                .as_ref()
                .is_some_and(|pending| pending.run_id == origin)
            {
                self.pending_failure = None;
            }
        }
        if let Some(result) = self.outcome(event_type, run_id, run)? {
            return Ok(Some(result));
        }
        for (orphan_type, orphan) in orphans {
            if let Some(result) = self.run_event(&orphan_type, &orphan)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    /// The outcome of a run event for a tied run.
    fn outcome(
        &mut self,
        event_type: &str,
        run_id: &str,
        run: &JsonObject,
    ) -> crate::Result<Option<JsonObject>> {
        if run.get("status").and_then(Value::as_str) == Some("pending_retry") {
            return Ok(None);
        }
        match event_type {
            "run:stop" => Ok(Some(run.clone())),
            "run:cancel" => Err(Error::RunFailed(run_result_message(run, "run cancelled"))),
            "run:fail" if self.retried.contains(run_id) => Ok(None),
            "run:fail" => {
                let error = Error::RunFailed(run_result_message(run, "run failed"));
                // retry_attempt goes up when a retry is scheduled; a run's own
                // attempt number is its origin's (0 for the first attempt).
                let base = origin_of(run)
                    .and_then(|origin| self.runs.get(origin).copied())
                    .unwrap_or(0);
                if retry_attempt(run) > base {
                    if self.pending_failure.is_none() {
                        self.pending_failure = Some(PendingFailure {
                            run_id: run_id.to_string(),
                            until: Instant::now() + RETRY_GRACE,
                            error,
                        });
                    }
                    Ok(None)
                } else {
                    Err(error)
                }
            }
            _ => Ok(None),
        }
    }
}

impl Waiter for RunTracker<'_> {
    type Output = JsonObject;

    fn event(&mut self, event: &StreamEvent) -> crate::Result<Option<JsonObject>> {
        self.handle(event)
    }

    fn wake_at(&self) -> Option<Instant> {
        RunTracker::wake_at(self)
    }

    fn wake(&mut self) -> crate::Result<Option<JsonObject>> {
        self.expire()
    }
}

fn origin_of(run: &JsonObject) -> Option<&str> {
    run.get("origin_run_id").and_then(Value::as_str)
}

fn retry_attempt(run: &JsonObject) -> i64 {
    run.get("retry_attempt")
        .and_then(Value::as_i64)
        .unwrap_or(0)
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
