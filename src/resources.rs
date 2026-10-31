//! Resource namespaces mirroring the Hot API v1 resources. This module is a
//! port of hot-python's resources.py.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use reqwest::{Method, Response};
use serde::Serialize;
use serde_json::{json, Value};

use crate::streaming::{
    extract_run_result, iter_stream, open_stream, wait_for_call_result, wait_for_run,
    wait_for_run_result, wait_for_task, EventStream, Progress, Reconnect, RunTracker,
    StreamEventExt, IDLE_TIMEOUT,
};
use crate::transport::{data_object, decode_data_response, enc, enc_path, Transport};
use crate::{Error, JsonObject, Result};

/// Optional parameters for [`StreamsResource::subscribe_with_event`].
pub struct SubscribeWithEventOptions {
    /// Scopes the subscription to a project id or slug.
    pub project: Option<String>,
    /// Automatically resubscribe across the API's 5-minute SSE limit and
    /// transient failures.
    pub reconnect: bool,
    /// Caps consecutive failed reconnects. A connection that delivers events
    /// resets the count, so routine 5-minute rollovers never use it up.
    pub max_attempts: u32,
}

impl Default for SubscribeWithEventOptions {
    fn default() -> Self {
        SubscribeWithEventOptions {
            project: None,
            reconnect: true,
            max_attempts: 5,
        }
    }
}

/// Optional parameters for [`StreamsResource::wait_for_run_result`].
pub struct WaitOptions<'a> {
    /// Bounds the wait.
    pub timeout: Duration,
    /// Receives streamed text chunks from the matched run.
    pub on_chunk: Option<&'a mut (dyn FnMut(&str) + Send)>,
}

impl Default for WaitOptions<'_> {
    fn default() -> Self {
        WaitOptions {
            timeout: Duration::from_secs(60),
            on_chunk: None,
        }
    }
}

/// Optional parameters for [`TasksResource::wait`].
pub struct TaskWaitOptions {
    /// Bounds the wait, including reconnects and their backoff.
    pub timeout: Duration,
    /// Caps consecutive failed reconnects.
    pub max_attempts: u32,
}

/// Optional parameters for [`RunsResource::wait`].
pub struct RunWaitOptions {
    /// Bounds the wait, including reconnects and their backoff.
    pub timeout: Duration,
    /// Caps consecutive failed reconnects.
    pub max_attempts: u32,
}

impl Default for RunWaitOptions {
    fn default() -> Self {
        RunWaitOptions {
            timeout: Duration::from_secs(60),
            max_attempts: 5,
        }
    }
}

impl Default for TaskWaitOptions {
    fn default() -> Self {
        TaskWaitOptions {
            timeout: Duration::from_secs(60),
            max_attempts: 5,
        }
    }
}

/// Optional parameters for [`EventsResource::call_hot`].
pub struct CallOptions<'a> {
    /// Bounds the wait for the run result.
    pub timeout: Duration,
    /// Receives streamed text chunks from the run.
    pub on_chunk: Option<&'a mut (dyn FnMut(&str) + Send)>,
}

impl Default for CallOptions<'_> {
    fn default() -> Self {
        CallOptions {
            timeout: Duration::from_secs(60),
            on_chunk: None,
        }
    }
}

/// A build archive to upload.
pub struct BuildUpload {
    /// The build archive content.
    pub content: Vec<u8>,
    /// Names the multipart file part. Defaults to "build".
    pub filename: Option<String>,
    /// The build content hash.
    pub hash: String,
    /// Optionally pins the build id.
    pub build_id: Option<String>,
}

macro_rules! resource {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        pub struct $name {
            transport: Arc<Transport>,
        }

        impl $name {
            pub(crate) fn new(transport: Arc<Transport>) -> Self {
                Self { transport }
            }
        }
    };
}

resource!(
    /// Publishes and inspects Hot events.
    EventsResource
);
resource!(
    /// Lists and inspects runs.
    RunsResource
);
resource!(
    /// Reads environment info and environment events.
    EnvResource
);
resource!(
    /// Reports organization usage and limits.
    OrgResource
);
resource!(
    /// Subscribes to Hot run streams.
    StreamsResource
);
resource!(
    /// Reads and waits for durable asynchronous tasks.
    TasksResource
);
resource!(
    /// Uploads, downloads, lists, and deletes files.
    FilesResource
);
resource!(
    /// Downloads large payloads that Hot spilled to blob storage. Requires an
    /// environment API key.
    BlobsResource
);
// Resources an org API key can also reach inside one environment, through
// `/v1/orgs/{org_id}/envs/{env_id}` (see [`OrgsResource::env`]).
macro_rules! scoped_resource {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        pub struct $name {
            transport: Arc<Transport>,
            prefix: String,
        }

        impl $name {
            pub(crate) fn new(transport: Arc<Transport>) -> Self {
                Self::scoped(transport, String::new())
            }

            pub(crate) fn scoped(transport: Arc<Transport>, prefix: String) -> Self {
                Self { transport, prefix }
            }
        }
    };
}

scoped_resource!(
    /// Manages projects.
    ProjectsResource
);
scoped_resource!(
    /// Uploads, deploys, and inspects builds.
    BuildsResource
);
scoped_resource!(
    /// Manages encrypted project context variables.
    ContextResource
);
resource!(
    /// Provisions and manages child organizations, their environments, and
    /// API keys. Requires an org API key.
    OrgsResource
);
resource!(
    /// Reads, narrows, rotates, and revokes org and environment API keys by
    /// ID. Requires an org API key.
    ApiKeysResource
);
resource!(
    /// Manages custom domains.
    DomainsResource
);
resource!(
    /// Creates and revokes scoped sessions.
    SessionsResource
);
resource!(
    /// Creates and revokes scoped service keys.
    ServiceKeysResource
);

impl EventsResource {
    /// Publishes an event and returns the created event data
    /// (event_id, stream_id, ...).
    pub async fn publish(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/events",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Publishes a hot:call event for `fn_name` with `args` and waits for the
    /// run result, unwrapping a `{"$ok": ...}` result value.
    pub async fn call_hot(
        &self,
        fn_name: &str,
        args: Vec<Value>,
        opts: CallOptions<'_>,
    ) -> Result<Value> {
        let body = json!({
            "event_type": "hot:call",
            "event_data": { "fn": fn_name, "args": args },
        });
        let Value::Object(body) = body else {
            unreachable!()
        };
        let run =
            wait_for_call_result(self.transport.clone(), body, opts.timeout, opts.on_chunk).await?;
        Ok(extract_run_result(run.get("result").cloned()))
    }

    /// Returns the event list envelope (data + pagination).
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/events", None, query)
            .await
    }

    /// Returns a single event.
    pub async fn get(&self, event_id: &str) -> Result<JsonObject> {
        let path = format!("/events/{}", enc(event_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the runs triggered by an event.
    pub async fn get_runs(&self, event_id: &str) -> Result<JsonObject> {
        let path = format!("/events/{}/runs", enc(event_id));
        self.transport
            .request_json(Method::GET, &path, None, &[])
            .await
    }
}

impl RunsResource {
    /// Returns the run list envelope (data + pagination).
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/runs", None, query)
            .await
    }

    /// Returns aggregate run statistics.
    pub async fn stats(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::GET, "/runs/stats", None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns a single run.
    pub async fn get(&self, run_id: &str) -> Result<JsonObject> {
        let path = format!("/runs/{}", enc(run_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a run's call traces. Returns `calls_deleted`,
    /// `blob_refs_released`, and `complete` (false when the server stopped at
    /// its time limit; call again).
    pub async fn purge_calls(&self, run_id: &str) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::DELETE,
                &format!("/runs/{}/calls", enc(run_id)),
                None,
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Streams durable run:update snapshots until the run is terminal.
    pub fn subscribe(&self, run_id: &str) -> EventStream {
        iter_stream(
            self.transport.clone(),
            Method::GET,
            format!("/runs/{}/subscribe", enc(run_id)),
            None,
            Vec::new(),
        )
    }

    /// Waits for successful run completion. Failed and cancelled runs return
    /// [`Error::RunWaitFailed`] with the final durable snapshot.
    pub async fn wait(&self, run_id: &str, opts: RunWaitOptions) -> Result<JsonObject> {
        wait_for_run(
            self.transport.clone(),
            run_id,
            opts.timeout,
            opts.max_attempts,
        )
        .await
    }
}

impl EnvResource {
    /// Returns environment info.
    pub async fn get(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::GET, "/env", None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes every call trace in the environment. Returns `calls_deleted`,
    /// `blob_refs_released`, and `complete` (false when the server stopped at
    /// its time limit; call again).
    pub async fn purge_calls(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::DELETE, "/env/calls", None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Streams environment events. Requires API key credentials and a live
    /// API pub/sub backend.
    pub fn subscribe(&self) -> EventStream {
        iter_stream(
            self.transport.clone(),
            Method::GET,
            "/env/subscribe".to_string(),
            None,
            Vec::new(),
        )
    }
}

impl OrgResource {
    /// Returns organization usage and limits.
    pub async fn usage(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::GET, "/org/usage", None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl StreamsResource {
    /// Deletes the call traces of every run on a stream. Returns `calls_deleted`,
    /// `blob_refs_released`, and `complete` (false when the server stopped at
    /// its time limit; call again).
    pub async fn purge_calls(&self, stream_id: &str) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::DELETE,
                &format!("/streams/{}/calls", enc(stream_id)),
                None,
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Streams events from an existing run stream.
    pub fn subscribe(&self, stream_id: &str, project: Option<&str>) -> EventStream {
        iter_stream(
            self.transport.clone(),
            Method::GET,
            format!("/streams/{}/subscribe", enc(stream_id)),
            None,
            project_query(project),
        )
    }

    /// Streams events from an existing run stream via POST.
    pub fn subscribe_post(&self, stream_id: &str, project: Option<&str>) -> EventStream {
        iter_stream(
            self.transport.clone(),
            Method::POST,
            format!("/streams/{}/subscribe", enc(stream_id)),
            None,
            project_query(project),
        )
    }

    /// Subscribes to a stream and returns the run object once the run created
    /// by `event_id` reaches run:stop. run:fail and run:cancel become
    /// [`Error::RunFailed`] carrying the run's result message.
    pub async fn wait_for_run_result(
        &self,
        stream_id: &str,
        event_id: &str,
        opts: WaitOptions<'_>,
    ) -> Result<JsonObject> {
        wait_for_run_result(
            self.transport.clone(),
            stream_id,
            event_id,
            opts.timeout,
            opts.on_chunk,
        )
        .await
    }

    /// Atomically subscribes then publishes an event, yielding stream events
    /// until the run completes.
    ///
    /// The Hot API closes SSE subscriptions after 5 minutes. While
    /// `opts.reconnect` is true (the default), the SDK resubscribes to the
    /// same stream_id and keeps yielding events until the terminal
    /// run:stop/run:fail/run:cancel of the run the event created, or of the
    /// retry that follows a `pending_retry` failure. Transient failures
    /// reconnect with jittered backoff; permanent 4xx errors end the stream.
    /// Replayed run:start and terminal events are deduped by run_id;
    /// stream:data chunks are not replayed and may be lost across the
    /// disconnect window.
    pub fn subscribe_with_event(
        &self,
        body: impl Serialize,
        opts: SubscribeWithEventOptions,
    ) -> EventStream {
        let body = match serde_json::to_value(body) {
            Ok(body) => body,
            Err(error) => {
                return Box::pin(async_stream::stream! { yield Err(Error::Json(error)) });
            }
        };
        let transport = self.transport.clone();
        let query = project_query(opts.project.as_deref());

        if !opts.reconnect {
            return iter_stream(
                transport,
                Method::POST,
                "/streams/subscribe-with-event".to_string(),
                Some(body),
                query,
            );
        }

        Box::pin(async_stream::stream! {
            let mut seen_start: HashSet<String> = HashSet::new();
            let mut seen_terminal: HashSet<String> = HashSet::new();
            let mut tracker = RunTracker::new(None, None);
            let mut stream_id: Option<String> = None;
            let mut reconnect = Reconnect::new(opts.max_attempts, None);

            loop {
                let mut source = match &stream_id {
                    None => open_stream(
                        transport.clone(),
                        Method::POST,
                        "/streams/subscribe-with-event".to_string(),
                        Some(body.clone()),
                        query.clone(),
                        Some(IDLE_TIMEOUT),
                    ),
                    Some(id) => open_stream(
                        transport.clone(),
                        Method::GET,
                        format!("/streams/{}/subscribe", enc(id)),
                        None,
                        query.clone(),
                        Some(IDLE_TIMEOUT),
                    ),
                };

                let mut progress = Progress::Nothing;
                let mut ended_with = None;
                loop {
                    let next = match tracker.wake_at() {
                        // A failure is waiting for its retry; when none comes,
                        // the failure (already yielded) ends the stream.
                        Some(until) => match tokio::time::timeout_at(until, source.next()).await {
                            Ok(next) => next,
                            Err(_) => return,
                        },
                        None => source.next().await,
                    };
                    let Some(item) = next else { break };
                    let event = match item {
                        Ok(event) => event,
                        Err(error) => {
                            ended_with = Some(error);
                            break;
                        }
                    };

                    if event.event_type() == "event:published" && stream_id.is_none() {
                        stream_id = event
                            .get("stream_id")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                    }
                    let duplicate = match (event.event_type(), event.run_id()) {
                        ("run:start", Some(run_id)) => !seen_start.insert(run_id.to_string()),
                        ("run:stop" | "run:fail" | "run:cancel", Some(run_id)) => {
                            !seen_terminal.insert(run_id.to_string())
                        }
                        _ => false,
                    };
                    if duplicate {
                        continue;
                    }
                    progress = progress.after(&event);

                    // A tied run that stops, fails for good, or is cancelled
                    // ends the stream after its event.
                    let terminal = !matches!(tracker.handle(&event), Ok(None));
                    yield Ok(event);
                    if terminal {
                        return;
                    }
                }

                // Publishing again would duplicate the event, so only a
                // connection that confirmed the publish is resumed.
                if stream_id.is_none() {
                    yield Err(ended_with.unwrap_or_else(|| Error::Protocol(
                        "stream ended before event:published was received".to_string(),
                    )));
                    return;
                }
                let next = reconnect
                    .next(
                        progress,
                        ended_with,
                        || Error::Protocol(
                            "stream ended before the published event's run completed".to_string(),
                        ),
                        || Error::Timeout,
                    )
                    .await;
                if let Err(error) = next {
                    yield Err(error);
                    return;
                }
            }
        })
    }
}

impl TasksResource {
    /// Returns the latest durable task snapshot.
    pub async fn get(&self, task_id: &str) -> Result<JsonObject> {
        let path = format!("/tasks/{}", enc(task_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Streams durable task:update snapshots until the task is terminal.
    pub fn subscribe(&self, task_id: &str) -> EventStream {
        iter_stream(
            self.transport.clone(),
            Method::GET,
            format!("/tasks/{}/subscribe", enc(task_id)),
            None,
            Vec::new(),
        )
    }

    /// Waits for successful task completion. Failed, cancelled, and timed-out
    /// tasks return [`Error::TaskFailed`] with the final durable snapshot.
    pub async fn wait(&self, task_id: &str, opts: TaskWaitOptions) -> Result<JsonObject> {
        wait_for_task(
            self.transport.clone(),
            task_id,
            opts.timeout,
            opts.max_attempts,
        )
        .await
    }
}

impl FilesResource {
    /// Returns the file list envelope (data + pagination).
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/files", None, query)
            .await
    }

    /// Returns a single file's metadata.
    pub async fn get(&self, file_id: &str) -> Result<JsonObject> {
        let path = format!("/files/{}", enc(file_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a file.
    pub async fn delete(&self, file_id: &str) -> Result<()> {
        let path = format!("/files/{}", enc(file_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Returns the raw HTTP response streaming the file content.
    pub async fn download(&self, file_id: &str) -> Result<Response> {
        let path = format!("/files/{}/download", enc(file_id));
        let builder = self.transport.request_builder(Method::GET, &path);
        self.transport.execute(builder).await
    }

    /// Uploads content to a slash-separated file path.
    pub async fn upload(
        &self,
        path: &str,
        content: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<JsonObject> {
        let request_path = format!("/files/upload/{}", enc_path(path));
        let mut builder = self
            .transport
            .request_builder(Method::PUT, &request_path)
            .header(ACCEPT, "application/json")
            .body(content);
        if let Some(content_type) = content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        let response = self.transport.execute(builder).await?;
        decode_data_response(response).await
    }

    /// Starts a multipart upload.
    pub async fn initiate_upload(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/files/uploads",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Uploads one part of a multipart upload.
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        content: Vec<u8>,
    ) -> Result<JsonObject> {
        let path = format!("/files/uploads/{}/{}", enc(upload_id), part_number);
        let builder = self
            .transport
            .request_builder(Method::PUT, &path)
            .header(ACCEPT, "application/json")
            .body(content);
        let response = self.transport.execute(builder).await?;
        decode_data_response(response).await
    }

    /// Finishes a multipart upload.
    pub async fn complete_upload(&self, upload_id: &str) -> Result<JsonObject> {
        let path = format!("/files/uploads/{}/complete", enc(upload_id));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Cancels a multipart upload.
    pub async fn abort_upload(&self, upload_id: &str) -> Result<()> {
        let path = format!("/files/uploads/{}", enc(upload_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }
}

impl BlobsResource {
    /// Returns the raw HTTP response streaming the blob's bytes
    /// (`application/octet-stream`; string payloads are UTF-8 text). The blob
    /// must belong to the caller's environment, and the key needs `read` on
    /// `blob:{blob_ref_id}`.
    pub async fn download(&self, blob_ref_id: &str) -> Result<Response> {
        let path = format!("/blobs/{}/download", enc(blob_ref_id));
        let builder = self.transport.request_builder(Method::GET, &path);
        self.transport.execute(builder).await
    }
}

impl ProjectsResource {
    /// Returns the project list envelope (data + pagination).
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(
                Method::GET,
                &format!("{}/projects", self.prefix),
                None,
                query,
            )
            .await
    }

    /// Creates a project.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                &format!("{}/projects", self.prefix),
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns a project by id or slug.
    pub async fn get(&self, project: &str) -> Result<JsonObject> {
        let path = format!("{}/projects/{}", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Patches a project.
    pub async fn update(&self, project: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("{}/projects/{}", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(
                Method::PATCH,
                &path,
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a project.
    pub async fn delete(&self, project: &str) -> Result<()> {
        let path = format!("{}/projects/{}", self.prefix, enc(project));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Activates a project.
    pub async fn activate(&self, project: &str) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/activate", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deactivates a project.
    pub async fn deactivate(&self, project: &str) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/deactivate", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the first page of the project's event handlers (list
    /// envelope). Use [`ProjectsResource::event_handlers_with_query`] to page.
    pub async fn event_handlers(&self, project: &str) -> Result<JsonObject> {
        self.event_handlers_with_query(project, &[]).await
    }

    /// Returns one page of the project's event handlers; pass `limit` (up to
    /// 500) and `offset`.
    pub async fn event_handlers_with_query(
        &self,
        project: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/event-handlers", self.prefix, enc(project));
        self.transport
            .request_json(Method::GET, &path, None, query)
            .await
    }

    /// Returns the first page of the project's schedules (list envelope).
    /// Use [`ProjectsResource::schedules_with_query`] to page.
    pub async fn schedules(&self, project: &str) -> Result<JsonObject> {
        self.schedules_with_query(project, &[]).await
    }

    /// Returns one page of the project's schedules; pass `limit` (up to 500)
    /// and `offset`.
    pub async fn schedules_with_query(
        &self,
        project: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/schedules", self.prefix, enc(project));
        self.transport
            .request_json(Method::GET, &path, None, query)
            .await
    }
}

impl BuildsResource {
    /// Returns the build list envelope across all projects.
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, &format!("{}/builds", self.prefix), None, query)
            .await
    }

    /// Returns the build list envelope for one project.
    pub async fn list_for_project(
        &self,
        project: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/builds", self.prefix, enc(project));
        self.transport
            .request_json(Method::GET, &path, None, query)
            .await
    }

    /// Returns a single build.
    pub async fn get(&self, project: &str, build_id: &str) -> Result<JsonObject> {
        let path = format!(
            "{}/projects/{}/builds/{}",
            self.prefix,
            enc(project),
            enc(build_id)
        );
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the project's deployed build.
    pub async fn deployed(&self, project: &str) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/builds/deployed", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the project's live build.
    pub async fn live(&self, project: &str) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/builds/live", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Uploads a build archive as multipart form data.
    pub async fn upload(&self, project: &str, upload: BuildUpload) -> Result<JsonObject> {
        let mut form = Form::new().text("hash", upload.hash);
        if let Some(build_id) = upload.build_id {
            form = form.text("build_id", build_id);
        }
        let part = Part::bytes(upload.content)
            .file_name(upload.filename.unwrap_or_else(|| "build".to_string()));
        form = form.part("file", part);

        let path = format!("{}/projects/{}/builds", self.prefix, enc(project));
        let builder = self
            .transport
            .request_builder(Method::POST, &path)
            .header(ACCEPT, "application/json")
            .multipart(form);
        let response = self.transport.execute(builder).await?;
        decode_data_response(response).await
    }

    /// Returns the raw HTTP response streaming the build archive.
    pub async fn download(&self, project: &str, build_id: &str) -> Result<Response> {
        let path = format!(
            "{}/projects/{}/builds/{}/download",
            self.prefix,
            enc(project),
            enc(build_id)
        );
        let builder = self.transport.request_builder(Method::GET, &path);
        self.transport.execute(builder).await
    }

    /// Deploys a build.
    pub async fn deploy(&self, project: &str, build_id: &str) -> Result<JsonObject> {
        let path = format!(
            "{}/projects/{}/builds/{}/deploy",
            self.prefix,
            enc(project),
            enc(build_id)
        );
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl ContextResource {
    /// Returns the first page of a project's context variables (list
    /// envelope). Use [`ContextResource::list_with_query`] to page.
    pub async fn list(&self, project: &str) -> Result<JsonObject> {
        self.list_with_query(project, &[]).await
    }

    /// Returns one page of a project's context variables; pass `limit` (up
    /// to 500) and `offset`.
    pub async fn list_with_query(
        &self,
        project: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/context", self.prefix, enc(project));
        self.transport
            .request_json(Method::GET, &path, None, query)
            .await
    }

    /// Creates a context variable.
    pub async fn create(&self, project: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("{}/projects/{}/context", self.prefix, enc(project));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, Some(&serde_json::to_value(body)?), &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Replaces a context variable.
    pub async fn update(
        &self,
        project: &str,
        key: &str,
        body: impl Serialize,
    ) -> Result<JsonObject> {
        let path = format!(
            "{}/projects/{}/context/{}",
            self.prefix,
            enc(project),
            enc(key)
        );
        let envelope = self
            .transport
            .request_json(Method::PUT, &path, Some(&serde_json::to_value(body)?), &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a context variable.
    pub async fn delete(&self, project: &str, key: &str) -> Result<()> {
        let path = format!(
            "{}/projects/{}/context/{}",
            self.prefix,
            enc(project),
            enc(key)
        );
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }
}

impl DomainsResource {
    /// Registers a custom domain.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/domains",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the first page of domains (list envelope). Use
    /// [`DomainsResource::list_with_query`] to page.
    pub async fn list(&self) -> Result<JsonObject> {
        self.list_with_query(&[]).await
    }

    /// Returns one page of domains; pass `limit` (up to 500) and `offset`.
    pub async fn list_with_query(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/domains", None, query)
            .await
    }

    /// Returns a single domain.
    pub async fn get(&self, domain_id: &str) -> Result<JsonObject> {
        let path = format!("/domains/{}", enc(domain_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a domain.
    pub async fn delete(&self, domain_id: &str) -> Result<()> {
        let path = format!("/domains/{}", enc(domain_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Triggers domain verification.
    pub async fn verify(&self, domain_id: &str) -> Result<JsonObject> {
        let path = format!("/domains/{}/verify", enc(domain_id));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl SessionsResource {
    /// Creates a scoped session.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/sessions",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the session list envelope.
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/sessions", None, query)
            .await
    }

    /// Revokes one session.
    pub async fn revoke(&self, session_id: &str) -> Result<()> {
        let path = format!("/sessions/{}", enc(session_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Revokes every session.
    pub async fn revoke_all(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::DELETE, "/sessions", None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl ServiceKeysResource {
    /// Creates a scoped service key.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/service-keys",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the service key list envelope.
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/service-keys", None, query)
            .await
    }

    /// Returns a single service key.
    pub async fn get(&self, service_key_id: &str) -> Result<JsonObject> {
        let path = format!("/service-keys/{}", enc(service_key_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Patches a service key.
    pub async fn update(&self, service_key_id: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("/service-keys/{}", enc(service_key_id));
        let envelope = self
            .transport
            .request_json(
                Method::PATCH,
                &path,
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Revokes one service key.
    pub async fn revoke(&self, service_key_id: &str) -> Result<()> {
        let path = format!("/service-keys/{}", enc(service_key_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Revokes every service key.
    pub async fn revoke_all(&self) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(Method::DELETE, "/service-keys", None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl OrgsResource {
    async fn data(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(method, path, body.as_ref(), query)
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the list envelope of the key's child organizations it may
    /// read; pass `limit` (up to 500) and `offset`.
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/orgs", None, query)
            .await
    }

    /// Provisions a child organization and its default environment. Returns
    /// `org`, `env_id`, and `created` (false when `idempotency_key` matched an
    /// earlier request; keep it stable across retries). Slugs have at most 32
    /// characters.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        self.data(
            Method::POST,
            "/orgs",
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }

    /// Returns one organization.
    pub async fn get(&self, org_id: &str) -> Result<JsonObject> {
        self.data(Method::GET, &format!("/orgs/{}", enc(org_id)), None, &[])
            .await
    }

    /// Changes a child's lifecycle state (`{"state": "active" | "suspended" |
    /// "archived"}`) and returns the organization.
    pub async fn update(&self, org_id: &str, body: impl Serialize) -> Result<JsonObject> {
        self.data(
            Method::PATCH,
            &format!("/orgs/{}", enc(org_id)),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }

    /// Returns account usage for the current billing period. Pass the root
    /// organization's ID (the org key's own organization).
    pub async fn usage(&self, org_id: &str) -> Result<JsonObject> {
        self.data(
            Method::GET,
            &format!("/orgs/{}/usage", enc(org_id)),
            None,
            &[],
        )
        .await
    }

    /// Deletes every call trace in an organization's environments, and in
    /// its children's when `include_sub_orgs` is true. Returns
    /// `calls_deleted`, `blob_refs_released`, and `complete`.
    pub async fn purge_calls(&self, org_id: &str, include_sub_orgs: bool) -> Result<JsonObject> {
        let query: &[(&str, &str)] = if include_sub_orgs {
            &[("include_sub_orgs", "true")]
        } else {
            &[]
        };
        self.data(
            Method::DELETE,
            &format!("/orgs/{}/calls", enc(org_id)),
            None,
            query,
        )
        .await
    }

    /// Returns the list envelope of an organization's environments.
    pub async fn list_envs(&self, org_id: &str) -> Result<JsonObject> {
        self.transport
            .request_json(
                Method::GET,
                &format!("/orgs/{}/envs", enc(org_id)),
                None,
                &[],
            )
            .await
    }

    /// Creates an environment (`{"name": ...}`); idempotent by exact name.
    pub async fn create_env(&self, org_id: &str, body: impl Serialize) -> Result<JsonObject> {
        self.data(
            Method::POST,
            &format!("/orgs/{}/envs", enc(org_id)),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }

    /// Returns one environment.
    pub async fn get_env(&self, org_id: &str, env_id: &str) -> Result<JsonObject> {
        self.data(
            Method::GET,
            &format!("/orgs/{}/envs/{}", enc(org_id), enc(env_id)),
            None,
            &[],
        )
        .await
    }

    /// Returns the list envelope of the organization's org API keys (the
    /// key's own organization only).
    pub async fn list_api_keys(&self, org_id: &str) -> Result<JsonObject> {
        self.transport
            .request_json(
                Method::GET,
                &format!("/orgs/{}/api-keys", enc(org_id)),
                None,
                &[],
            )
            .await
    }

    /// Mints an org API key (`description`, `permissions`, optional
    /// `expires_at`) within the caller's grants. The returned `token` is
    /// shown only once.
    pub async fn create_api_key(&self, org_id: &str, body: impl Serialize) -> Result<JsonObject> {
        self.data(
            Method::POST,
            &format!("/orgs/{}/api-keys", enc(org_id)),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }

    /// Returns the list envelope of an environment's API keys; pass `limit`
    /// (up to 500) and `offset`.
    pub async fn list_env_api_keys(
        &self,
        org_id: &str,
        env_id: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        self.transport
            .request_json(
                Method::GET,
                &format!("/orgs/{}/envs/{}/api-keys", enc(org_id), enc(env_id)),
                None,
                query,
            )
            .await
    }

    /// Mints an environment API key (`description`, `permissions`, optional
    /// `expires_at`) within the org key's grants for that environment. The
    /// returned `token` is shown only once; use it with a separate
    /// [`crate::HotClient`] for runtime calls.
    pub async fn create_env_api_key(
        &self,
        org_id: &str,
        env_id: &str,
        body: impl Serialize,
    ) -> Result<JsonObject> {
        self.data(
            Method::POST,
            &format!("/orgs/{}/envs/{}/api-keys", enc(org_id), enc(env_id)),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }

    /// Projects, builds, and context inside one environment, authorized by
    /// the org key's grants projected onto it.
    pub fn env(&self, org_id: &str, env_id: &str) -> OrgEnv {
        OrgEnv {
            transport: self.transport.clone(),
            prefix: format!("/orgs/{}/envs/{}", enc(org_id), enc(env_id)),
        }
    }
}

/// Project, build, and context resources of one environment, reached with
/// an org API key through `/v1/orgs/{org_id}/envs/{env_id}`. Returned by
/// [`OrgsResource::env`].
pub struct OrgEnv {
    transport: Arc<Transport>,
    prefix: String,
}

impl OrgEnv {
    pub fn projects(&self) -> ProjectsResource {
        ProjectsResource::scoped(self.transport.clone(), self.prefix.clone())
    }

    pub fn builds(&self) -> BuildsResource {
        BuildsResource::scoped(self.transport.clone(), self.prefix.clone())
    }

    pub fn context(&self) -> ContextResource {
        ContextResource::scoped(self.transport.clone(), self.prefix.clone())
    }
}

impl ApiKeysResource {
    /// Returns an org or environment API key's metadata (never its secret).
    pub async fn get(&self, key_id: &str) -> Result<JsonObject> {
        let path = format!("/api-keys/{}", enc(key_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Replaces a key's grants (`{"permissions": ...}`); they must fit inside
    /// the caller's own, and only keys the caller issued can change.
    pub async fn update(&self, key_id: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("/api-keys/{}", enc(key_id));
        let envelope = self
            .transport
            .request_json(
                Method::PATCH,
                &path,
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Revokes a key. Everything it issued stops on its next request.
    pub async fn revoke(&self, key_id: &str) -> Result<()> {
        let path = format!("/api-keys/{}", enc(key_id));
        self.transport
            .request_json(Method::DELETE, &path, None, &[])
            .await?;
        Ok(())
    }

    /// Issues a new secret for the same key and returns it as `token`. The
    /// previous secret keeps working for `overlap_seconds` (server default
    /// 86400, maximum 604800).
    pub async fn rotate(&self, key_id: &str, overlap_seconds: Option<u64>) -> Result<JsonObject> {
        let path = format!("/api-keys/{}/rotate", enc(key_id));
        let body = match overlap_seconds {
            Some(seconds) => json!({ "overlap_seconds": seconds }),
            None => json!({}),
        };
        let envelope = self
            .transport
            .request_json(Method::POST, &path, Some(&body), &[])
            .await?;
        Ok(data_object(envelope))
    }
}

fn project_query(project: Option<&str>) -> Vec<(String, String)> {
    match project {
        Some(project) => vec![("project".to_string(), project.to_string())],
        None => Vec::new(),
    }
}
