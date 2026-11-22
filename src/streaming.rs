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

use crate::sse::SseDecoder;
use crate::transport::{api_error, enc_segment, read_error_body, Transport};
use crate::{Error, JsonObject, Result, StreamEvent};

/// A stream of Hot run-stream events.
pub type EventStream = Pin<Box<dyn Stream<Item = crate::Result<StreamEvent>> + Send>>;

/// Convenience accessors for [`StreamEvent`] wire-format fields.
pub trait StreamEventExt {
    /// The event's "type" field, or "" when absent.
    fn event_type(&self) -> &str;
    /// The event's "run" object, when present.
    fn run(&self) -> Option<&JsonObject>;
    /// `run.run_id`, when present.
    fn run_id(&self) -> Option<&str>;
    /// Top-level `retry_run_id` on `run:fail` events when a retry run was
    /// scheduled.
    fn retry_run_id(&self) -> Option<&str> {
        None
    }
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

    fn retry_run_id(&self) -> Option<&str> {
        self.get("retry_run_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
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
        if !status.is_success() {
            let headers = response.headers().clone();
            let text = match idle {
                Some(idle) => tokio::time::timeout(idle, read_error_body(response))
                    .await
                    .unwrap_or_default(),
                None => read_error_body(response).await,
            };
            yield Err(Error::Api(api_error(status.as_u16(), &text, &headers, &path)));
            return;
        }

        let mut decoder = SseDecoder::new();
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
            match decoder.push(&chunk) {
                Ok(events) => {
                    for event in events {
                        yield Ok(event);
                    }
                }
                Err(error) => {
                    yield Err(error);
                    return;
                }
            }
        }

        match decoder.finish() {
            Ok(events) => {
                for event in events {
                    yield Ok(event);
                }
            }
            Err(error) => {
                yield Err(error);
            }
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

/// Consecutive `stream:complete` rollovers faster than this window are counted
/// toward rollover throttling so a broken server cannot trigger an infinite
/// tight reconnect loop.
const RAPID_ROLLOVER_WINDOW: Duration = Duration::from_secs(1);
const RAPID_ROLLOVER_BURST: u32 = 10;

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
pub(crate) fn jitter() -> f64 {
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
/// a clean rollover (`stream:complete`) reconnects without delay (subject to
/// deadline and rapid-rollover throttling). Otherwise each consecutive failure
/// waits with jittered exponential backoff, honouring `retry_after`, until
/// `max_attempts` reconnects have failed in a row or the deadline would pass.
pub(crate) struct Reconnect {
    max_attempts: u32,
    failures: u32,
    deadline: Option<Instant>,
    rapid_rollovers: u32,
    last_rollover: Option<Instant>,
}

impl Reconnect {
    pub(crate) fn new(max_attempts: u32, deadline: Option<Instant>) -> Reconnect {
        Reconnect {
            max_attempts,
            failures: 0,
            deadline,
            rapid_rollovers: 0,
            last_rollover: None,
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
            let now = Instant::now();
            if self.deadline.is_some_and(|deadline| now >= deadline) {
                return Err(timeout());
            }
            if self
                .last_rollover
                .is_some_and(|last| now.saturating_duration_since(last) < RAPID_ROLLOVER_WINDOW)
            {
                self.rapid_rollovers = self.rapid_rollovers.saturating_add(1);
            } else {
                self.rapid_rollovers = 1;
            }
            self.last_rollover = Some(now);
            if self.rapid_rollovers > RAPID_ROLLOVER_BURST {
                let delay = backoff(self.rapid_rollovers - RAPID_ROLLOVER_BURST, None);
                if let Some(deadline) = self.deadline {
                    match Instant::now().checked_add(delay) {
                        Some(wake) if wake < deadline => {}
                        _ => return Err(timeout()),
                    }
                }
                tokio::time::sleep(delay).await;
            }
            return Ok(());
        }
        self.rapid_rollovers = 0;
        self.last_rollover = None;
        self.failures += 1;
        if self.failures > self.max_attempts {
            return Err(error.unwrap_or_else(ended));
        }
        let delay = backoff(self.failures, error.as_ref().and_then(Error::retry_after));
        if let Some(deadline) = self.deadline {
            match Instant::now().checked_add(delay) {
                Some(wake) if wake < deadline => {}
                _ => return Err(timeout()),
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

    /// When set, [`Waiter::wake`] runs at this instant (if before the outer
    /// deadline) unless an event arrives first.
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
    deadline: Option<Instant>,
    timeout: fn() -> Error,
    waiter: &mut W,
) -> crate::Result<Connection<W::Output>> {
    let mut progress = Progress::Nothing;
    loop {
        let wake = waiter.wake_at();
        let (until, wake_fires) = match (wake, deadline) {
            (Some(wake), Some(deadline)) if wake < deadline => (Some(wake), true),
            (_, Some(deadline)) => (Some(deadline), false),
            (Some(wake), None) => (Some(wake), true),
            (None, None) => (None, false),
        };
        let item = match until {
            Some(until) => match tokio::time::timeout_at(until, source.next()).await {
                Ok(item) => item,
                Err(_) => {
                    if wake_fires {
                        if let Some(result) = waiter.wake()? {
                            return Ok(Connection::Done(result));
                        }
                        if deadline.is_none_or(|deadline| Instant::now() < deadline) {
                            continue;
                        }
                    }
                    return Err(timeout());
                }
            },
            None => source.next().await,
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

fn stream_path(stream_id: &str) -> Result<String> {
    Ok(format!("/streams/{}/subscribe", enc_segment(stream_id)?))
}

pub(crate) async fn wait_for_run_result(
    transport: Arc<Transport>,
    stream_id: &str,
    event_id: &str,
    timeout: Duration,
    on_chunk: Option<&mut (dyn FnMut(&str) + Send)>,
) -> crate::Result<JsonObject> {
    let path = stream_path(stream_id)?;
    let deadline = Instant::now().checked_add(timeout);
    let mut tracker = RunTracker::new(Some(event_id.to_string()), on_chunk);
    let mut reconnect = Reconnect::new(MAX_RECONNECTS, deadline);
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            path.clone(),
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
    let deadline = Instant::now().checked_add(timeout);
    let mut tracker = RunTracker::new(None, on_chunk);
    let mut stream_id: Option<String> = None;
    let mut reconnect = Reconnect::new(MAX_RECONNECTS, deadline);
    loop {
        let (method, path, request_body) = match stream_id.as_deref() {
            None => (
                Method::POST,
                "/streams/subscribe-with-event".to_string(),
                Some(Value::Object(body.clone())),
            ),
            Some(stream_id) => (Method::GET, stream_path(stream_id)?, None),
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
    let path = format!("/tasks/{}/subscribe", enc_segment(task_id)?);
    let deadline = Instant::now().checked_add(timeout);
    let mut reconnect = Reconnect::new(max_attempts, deadline);
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            path.clone(),
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
    let path = format!("/runs/{}/subscribe", enc_segment(run_id)?);
    let deadline = Instant::now().checked_add(timeout);
    let mut reconnect = Reconnect::new(max_attempts, deadline);
    loop {
        let source = open_stream(
            transport.clone(),
            Method::GET,
            path.clone(),
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
/// Maximum distinct unknown runs tracked in buffered chunks on shared streams.
const MAX_BUFFERED_CHUNK_RUNS: usize = 1024;
/// Maximum untied orphan run events retained on busy shared streams.
const MAX_ORPHAN_EVENTS: usize = 1024;

/// How long a failed run whose retry was scheduled waits for that retry run
/// to appear before its failure is reported.
const RETRY_GRACE: Duration = Duration::from_secs(5);

/// A failure that stands unless a retry of the failed run appears.
struct PendingFailure {
    run_id: String,
    until: Option<Instant>,
    error: Error,
}

/// A run tied to the published event.
struct TiedRun {
    run_type: Option<String>,
    // The highest retry_attempt seen.
    retry_attempt: i64,
    // The origin's retry_attempt when pre-tied via `retry_run_id`.
    base_retry_attempt: i64,
}

type OrphanEvent = (String, JsonObject, Option<String>);

/// Ties runs on a stream to one published event and delivers their text
/// chunks in order. A run belongs to the event when its `event_id` matches,
/// when it was named as a top-level `retry_run_id` on a tied `run:fail`, or
/// when it is a retry of a run that does: its `origin_run_id` names that run
/// and it has the same `run_type` (a retry gets a new event id). Task runs
/// also name the run that started them as their origin, but have `run_type`
/// `task`, so they are not taken for retries.
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
    runs: HashMap<String, TiedRun>,
    // Tied runs that a tied retry run names as its origin or `retry_run_id`.
    retried: HashSet<String>,
    pending_failure: Option<PendingFailure>,
    // Run events whose origin run is not tied yet, by origin_run_id.
    orphans: HashMap<String, Vec<OrphanEvent>>,
    orphan_count: usize,
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
            orphan_count: 0,
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
                    && !text.is_empty()
                    && self.chunk_bytes.saturating_add(text.len()) <= MAX_BUFFERED_CHUNK_BYTES
                    && (self.chunks.contains_key(run_id)
                        || self.chunks.len() < MAX_BUFFERED_CHUNK_RUNS)
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
                let retry_run_id = if event_type == "run:fail" {
                    event.retry_run_id()
                } else {
                    None
                };
                self.run_event(event_type, run, retry_run_id)
            }
            _ => Ok(None),
        }
    }

    /// When a failure is waiting for its retry, the instant it stands.
    pub(crate) fn wake_at(&self) -> Option<Instant> {
        self.pending_failure
            .as_ref()
            .and_then(|pending| pending.until)
    }

    /// Reports a failure whose retry did not appear in time.
    pub(crate) fn expire(&mut self) -> crate::Result<Option<JsonObject>> {
        match self.pending_failure.take() {
            Some(pending) => Err(pending.error),
            None => Ok(None),
        }
    }

    /// True when the run is tied to the published event, by run id (e.g. from
    /// a `run:fail`'s `retry_run_id`), by event id, or as a retry of a tied
    /// run.
    fn ties(&self, run: &JsonObject) -> bool {
        let by_id = run
            .get("run_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .is_some_and(|run_id| self.runs.contains_key(run_id));
        let by_event = self
            .event_id
            .as_deref()
            .is_some_and(|event_id| event_id_of_run(Some(run)) == Some(event_id));
        by_id
            || by_event
            || origin_of(run)
                .and_then(|origin| self.runs.get(origin))
                .is_some_and(|origin| origin.run_type.as_deref() == run_type(run))
    }

    fn flush_chunks(&mut self, run_id: &str) {
        if let Some(chunks) = self.chunks.remove(run_id) {
            for text in chunks {
                self.chunk_bytes = self.chunk_bytes.saturating_sub(text.len());
                if let Some(on_chunk) = self.on_chunk.as_deref_mut() {
                    on_chunk(&text);
                }
            }
        }
    }

    fn take_matching_orphans(
        &mut self,
        run_id: &str,
        expected_type: Option<&str>,
    ) -> Vec<OrphanEvent> {
        let Some(mut orphans) = self.orphans.remove(run_id) else {
            return Vec::new();
        };
        self.orphan_count = self.orphan_count.saturating_sub(orphans.len());
        orphans.retain(|(_, orphan, _)| run_type(orphan) == expected_type);
        if !orphans.is_empty() {
            // A retry of this run is already on the stream.
            self.retried.insert(run_id.to_string());
        }
        orphans
    }

    fn run_event(
        &mut self,
        event_type: &str,
        run: &JsonObject,
        retry_run_id: Option<&str>,
    ) -> crate::Result<Option<JsonObject>> {
        let Some(run_id) = run
            .get("run_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            return Ok(None);
        };
        if !self.ties(run) {
            // A run of another type started by a tied run (a task) is not a
            // retry; keep only runs whose origin is not known yet, bounded to
            // avoid unbounded memory on busy shared streams.
            if self.orphan_count < MAX_ORPHAN_EVENTS {
                if let Some(origin) =
                    origin_of(run).filter(|origin| !self.runs.contains_key(*origin))
                {
                    self.orphans.entry(origin.to_string()).or_default().push((
                        event_type.to_string(),
                        run.clone(),
                        retry_run_id.map(str::to_string),
                    ));
                    self.orphan_count += 1;
                }
            }
            return Ok(None);
        }
        let attempt = retry_attempt(run);
        let run_ty = run_type(run).map(str::to_string);
        match self.runs.get_mut(run_id) {
            Some(seen) => {
                seen.retry_attempt = seen.retry_attempt.max(attempt);
                if seen.run_type.is_none() {
                    seen.run_type = run_ty.clone();
                }
            }
            None => {
                self.runs.insert(
                    run_id.to_string(),
                    TiedRun {
                        run_type: run_ty.clone(),
                        retry_attempt: attempt,
                        base_retry_attempt: 0,
                    },
                );
            }
        }
        self.flush_chunks(run_id);
        let effective_type = self.runs.get(run_id).and_then(|seen| seen.run_type.clone());
        let mut orphans = self.take_matching_orphans(run_id, effective_type.as_deref());

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

        // Correlate a retry run immediately when `run:fail` names top-level
        // `retry_run_id`.
        if let Some(retry_id) = retry_run_id.filter(|id| !id.is_empty() && *id != run_id) {
            self.retried.insert(run_id.to_string());
            if self
                .pending_failure
                .as_ref()
                .is_some_and(|pending| pending.run_id == run_id)
            {
                self.pending_failure = None;
            }
            let parent_attempt = self
                .runs
                .get(run_id)
                .map_or(attempt, |seen| seen.retry_attempt);
            if !self.runs.contains_key(retry_id) {
                self.runs.insert(
                    retry_id.to_string(),
                    TiedRun {
                        run_type: effective_type.clone(),
                        retry_attempt: parent_attempt,
                        base_retry_attempt: parent_attempt,
                    },
                );
                self.flush_chunks(retry_id);
                orphans.extend(self.take_matching_orphans(retry_id, effective_type.as_deref()));
            }
        }

        if let Some(result) = self.outcome(event_type, run_id, run)? {
            return Ok(Some(result));
        }
        for (orphan_type, orphan, orphan_retry_id) in orphans {
            if let Some(result) =
                self.run_event(&orphan_type, &orphan, orphan_retry_id.as_deref())?
            {
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
                let origin_base = origin_of(run)
                    .and_then(|origin| self.runs.get(origin))
                    .map_or(0, |origin| origin.retry_attempt);
                let self_base = self
                    .runs
                    .get(run_id)
                    .map_or(0, |seen| seen.base_retry_attempt);
                let base = origin_base.max(self_base);
                if retry_attempt(run) > base {
                    if self.pending_failure.is_none() {
                        self.pending_failure = Some(PendingFailure {
                            run_id: run_id.to_string(),
                            until: Instant::now().checked_add(RETRY_GRACE),
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

fn run_type(run: &JsonObject) -> Option<&str> {
    run.get("run_type").and_then(Value::as_str)
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse_event(value: Value) -> StreamEvent {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn stream_event_ext_reads_retry_run_id() {
        let with_retry = parse_event(json!({
            "type": "run:fail",
            "retry_run_id": "run_retry_2",
            "run": { "run_id": "run_1" }
        }));
        assert_eq!(with_retry.retry_run_id(), Some("run_retry_2"));

        let empty_retry = parse_event(json!({
            "type": "run:fail",
            "retry_run_id": "",
            "run": { "run_id": "run_1" }
        }));
        assert_eq!(empty_retry.retry_run_id(), None);

        let no_retry = parse_event(json!({ "type": "run:stop" }));
        assert_eq!(no_retry.retry_run_id(), None);
    }

    #[test]
    fn run_tracker_correlates_retry_run_id_immediately() {
        let mut chunks = Vec::new();
        let mut on_chunk = |text: &str| chunks.push(text.to_string());
        let mut tracker = RunTracker::new(Some("e1".to_string()), Some(&mut on_chunk));

        // Early chunk for retry run `b` arrives before `a` fails.
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "stream:data",
                "run_id": "b",
                "payload": { "text": "early-" }
            })))
            .unwrap()
            .is_none());

        // `a` fails naming top-level `retry_run_id: "b"` even when status is `failed`.
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "run:fail",
                "retry_run_id": "b",
                "run": { "run_id": "a", "event_id": "e1", "status": "failed", "retry_attempt": 1 }
            })))
            .unwrap()
            .is_none());
        assert!(tracker.wake_at().is_none());

        // Subsequent chunk for `b` streams immediately, and `b` completes even
        // without `origin_run_id`.
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "stream:data",
                "run_id": "b",
                "payload": { "text": "done" }
            })))
            .unwrap()
            .is_none());
        let finished = tracker
            .handle(&parse_event(json!({
                "type": "run:stop",
                "run": { "run_id": "b", "event_id": "e2", "status": "succeeded", "result": "ok" }
            })))
            .unwrap()
            .unwrap();
        assert_eq!(finished["run_id"], "b");
        assert_eq!(chunks, vec!["early-", "done"]);
    }

    #[test]
    fn run_tracker_retry_run_id_final_failure_fails_without_grace_wait() {
        let mut tracker = RunTracker::new(Some("e1".to_string()), None);
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "run:fail",
                "retry_run_id": "b",
                "run": { "run_id": "a", "event_id": "e1", "status": "pending_retry", "retry_attempt": 1 }
            })))
            .unwrap()
            .is_none());
        // `b` fails without `origin_run_id` at the same retry_attempt (no further retry).
        let err = tracker
            .handle(&parse_event(json!({
                "type": "run:fail",
                "run": { "run_id": "b", "event_id": "e2", "status": "failed", "retry_attempt": 1, "result": "final boom" }
            })))
            .unwrap_err();
        assert!(matches!(err, Error::RunFailed(msg) if msg == "final boom"));
    }

    #[test]
    fn run_tracker_ignores_missing_or_empty_run_id() {
        let mut tracker = RunTracker::new(Some("e1".to_string()), None);
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "run:stop",
                "run": { "event_id": "e1", "status": "succeeded" }
            })))
            .unwrap()
            .is_none());
        assert!(tracker
            .handle(&parse_event(json!({
                "type": "run:stop",
                "run": { "run_id": "", "event_id": "e1", "status": "succeeded" }
            })))
            .unwrap()
            .is_none());
        assert!(tracker.runs.is_empty());
    }

    #[test]
    fn run_tracker_caps_orphans() {
        let mut tracker = RunTracker::new(Some("e1".to_string()), None);
        for i in 0..(MAX_ORPHAN_EVENTS + 100) {
            let _ = tracker.handle(&parse_event(json!({
                "type": "run:start",
                "run": { "run_id": format!("r{i}"), "origin_run_id": format!("o{i}") }
            })));
        }
        assert_eq!(tracker.orphan_count, MAX_ORPHAN_EVENTS);
    }

    struct WakeWaiter {
        wake: Instant,
        woke: bool,
    }

    impl Waiter for WakeWaiter {
        type Output = JsonObject;

        fn event(&mut self, _event: &StreamEvent) -> crate::Result<Option<JsonObject>> {
            Ok(None)
        }

        fn wake_at(&self) -> Option<Instant> {
            Some(self.wake)
        }

        fn wake(&mut self) -> crate::Result<Option<JsonObject>> {
            self.woke = true;
            Err(Error::RunFailed("grace expired".into()))
        }
    }

    #[tokio::test]
    async fn consume_returns_timeout_when_outer_deadline_precedes_wake() {
        let source: EventStream = Box::pin(futures_util::stream::pending());
        let deadline = Instant::now().checked_add(Duration::from_millis(20));
        let mut waiter = WakeWaiter {
            wake: Instant::now() + Duration::from_secs(5),
            woke: false,
        };
        let err = consume(source, deadline, || Error::Timeout, &mut waiter)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::Timeout), "{err:?}");
        assert!(!waiter.woke, "waiter.wake() must not run on outer deadline");
    }

    #[tokio::test]
    async fn consume_calls_waiter_wake_when_wake_precedes_deadline() {
        let source: EventStream = Box::pin(futures_util::stream::pending());
        let deadline = Instant::now().checked_add(Duration::from_secs(5));
        let mut waiter = WakeWaiter {
            wake: Instant::now() + Duration::from_millis(20),
            woke: false,
        };
        let err = consume(source, deadline, || Error::Timeout, &mut waiter)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::RunFailed(msg) if msg == "grace expired"));
        assert!(waiter.woke);
    }

    #[tokio::test]
    async fn duration_max_does_not_panic_in_consume_or_reconnect() {
        // Duration::MAX overflows checked_add to None (no finite deadline).
        let deadline = Instant::now().checked_add(Duration::MAX);
        assert!(deadline.is_none());

        let source: EventStream = Box::pin(futures_util::stream::empty());
        let mut waiter = Snapshots(|_: &StreamEvent| Ok(None));
        let outcome = consume(source, deadline, || Error::Timeout, &mut waiter)
            .await
            .unwrap();
        assert!(matches!(outcome, Connection::Ended { .. }));

        let mut reconnect = Reconnect::new(1, deadline);
        // Should not panic when computing deadline check.
        reconnect
            .next(
                Progress::Rollover,
                None,
                || Error::Protocol("ended".into()),
                || Error::Timeout,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reconnect_rollover_checks_deadline_and_throttles_rapid_rollovers() {
        // Expired deadline fails immediately on rollover.
        let past = Instant::now();
        let mut expired = Reconnect::new(5, Some(past));
        let err = expired
            .next(
                Progress::Rollover,
                None,
                || Error::Protocol("ended".into()),
                || Error::Timeout,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout));

        // Burst of rapid rollovers within window starts throttling after RAPID_ROLLOVER_BURST.
        let tight_deadline = Instant::now() + Duration::from_millis(100);
        let mut rapid = Reconnect::new(5, Some(tight_deadline));
        for _ in 0..RAPID_ROLLOVER_BURST {
            rapid
                .next(
                    Progress::Rollover,
                    None,
                    || Error::Protocol("ended".into()),
                    || Error::Timeout,
                )
                .await
                .unwrap();
        }
        // Next rapid rollover wants >=250ms backoff which exceeds the 100ms deadline.
        let err = rapid
            .next(
                Progress::Rollover,
                None,
                || Error::Protocol("ended".into()),
                || Error::Timeout,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn idle_timeout_error_is_transient_and_reconnects() {
        let error = idle_error(IDLE_TIMEOUT);
        assert!(!error.is_permanent());
        assert!(error.to_string().contains("45 s"));

        let mut reconnect = Reconnect::new(1, Instant::now().checked_add(Duration::from_secs(5)));
        reconnect
            .next(
                Progress::Nothing,
                Some(error),
                || Error::Protocol("ended".into()),
                || Error::Timeout,
            )
            .await
            .unwrap();
    }
}
