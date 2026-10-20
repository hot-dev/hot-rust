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
    extract_run_result, iter_stream, wait_for_call_result, wait_for_run, wait_for_run_result,
    wait_for_task, EventStream, StreamEventExt,
};
use crate::transport::{data_object, decode_data_response, enc, enc_path, Transport};
use crate::{Error, JsonObject, Result};

/// Optional parameters for [`StreamsResource::subscribe_with_event`].
pub struct SubscribeWithEventOptions {
    /// Scopes the subscription to a project id or slug.
    pub project: Option<String>,
    /// Automatically resubscribe across the API's 5-minute SSE timeout.
    pub reconnect: bool,
    /// Caps reconnection attempts.
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
    /// Bounds the wait.
    pub timeout: Duration,
    /// Caps reconnection attempts.
    pub max_attempts: u32,
}

/// Optional parameters for [`RunsResource::wait`].
pub struct RunWaitOptions {
    /// Bounds the wait.
    pub timeout: Duration,
    /// Caps reconnection attempts.
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
    /// Manages projects.
    ProjectsResource
);
resource!(
    /// Uploads, deploys, and inspects builds.
    BuildsResource
);
resource!(
    /// Manages encrypted project context variables.
    ContextResource
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
    /// The Hot API closes SSE subscriptions after 5 minutes of idle time.
    /// While `opts.reconnect` is true (the default), the SDK resubscribes to
    /// the same stream_id and keeps yielding events until a terminal
    /// run:stop/run:fail/run:cancel arrives. Replayed run:start and terminal
    /// events are deduped by run_id; stream:data chunks are not replayed and
    /// may be lost across the disconnect window.
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
        let once = move |transport: Arc<Transport>, query: Vec<(String, String)>| {
            iter_stream(
                transport,
                Method::POST,
                "/streams/subscribe-with-event".to_string(),
                Some(body.clone()),
                query,
            )
        };

        if !opts.reconnect {
            return once(transport, query);
        }

        Box::pin(async_stream::stream! {
            let mut seen_start: HashSet<String> = HashSet::new();
            let mut seen_terminal: HashSet<String> = HashSet::new();
            let mut stream_id: Option<String> = None;
            let mut event_id: Option<String> = None;
            let mut attempts: u32 = 0;

            loop {
                let mut source = match &stream_id {
                    None => once(transport.clone(), query.clone()),
                    Some(id) => iter_stream(
                        transport.clone(),
                        Method::GET,
                        format!("/streams/{}/subscribe", enc(id)),
                        None,
                        query.clone(),
                    ),
                };

                let mut terminal = false;
                while let Some(item) = source.next().await {
                    let event = match item {
                        Ok(event) => event,
                        Err(error) => {
                            if error.is_authorization() || stream_id.is_none() || attempts >= opts.max_attempts {
                                yield Err(error);
                                return;
                            }
                            break;
                        }
                    };

                    match event.event_type() {
                        "event:published" => {
                            if let Some(published) =
                                event.get("stream_id").and_then(Value::as_str)
                            {
                                stream_id = Some(published.to_string());
                            }
                            if let Some(published) =
                                event.get("event_id").and_then(Value::as_str)
                            {
                                event_id = Some(published.to_string());
                            }
                        }
                        "run:start" => {
                            let run_id = event.run_id().map(str::to_string);
                            if let Some(run_id) = run_id {
                                if !seen_start.insert(run_id) {
                                    continue;
                                }
                            }
                        }
                        "run:stop" | "run:fail" | "run:cancel" => {
                            let run_id = event.run_id().map(str::to_string);
                            if let Some(run_id) = run_id {
                                if !seen_terminal.insert(run_id) {
                                    continue;
                                }
                            }
                            terminal = event_id.as_deref().is_some_and(|published| {
                                crate::streaming::event_id_of_run(event.run()) == Some(published)
                            });
                        }
                        _ => {}
                    }

                    yield Ok(event);
                    if terminal {
                        return;
                    }
                }

                if terminal {
                    return;
                }
                if stream_id.is_none() {
                    yield Err(Error::Protocol(
                        "stream ended before event:published was received".to_string(),
                    ));
                    return;
                }
                if attempts >= opts.max_attempts {
                    yield Err(Error::Protocol(
                        "stream ended before the published event's run completed".to_string(),
                    ));
                    return;
                }

                attempts += 1;
                let delay = Duration::from_millis((250 * u64::from(attempts)).min(2000));
                tokio::time::sleep(delay).await;
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
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
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
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
        Ok(())
    }
}

impl ProjectsResource {
    /// Returns the project list envelope (data + pagination).
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/projects", None, query)
            .await
    }

    /// Creates a project.
    pub async fn create(&self, body: impl Serialize) -> Result<JsonObject> {
        let envelope = self
            .transport
            .request_json(
                Method::POST,
                "/projects",
                Some(&serde_json::to_value(body)?),
                &[],
            )
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns a project by id or slug.
    pub async fn get(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}", enc(project));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Patches a project.
    pub async fn update(&self, project: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("/projects/{}", enc(project));
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
        let path = format!("/projects/{}", enc(project));
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
        Ok(())
    }

    /// Activates a project.
    pub async fn activate(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/activate", enc(project));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deactivates a project.
    pub async fn deactivate(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/deactivate", enc(project));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the project's event handlers.
    pub async fn event_handlers(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/event-handlers", enc(project));
        self.transport
            .request_json(Method::GET, &path, None, &[])
            .await
    }

    /// Returns the project's schedules.
    pub async fn schedules(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/schedules", enc(project));
        self.transport
            .request_json(Method::GET, &path, None, &[])
            .await
    }
}

impl BuildsResource {
    /// Returns the build list envelope across all projects.
    pub async fn list(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/builds", None, query)
            .await
    }

    /// Returns the build list envelope for one project.
    pub async fn list_for_project(
        &self,
        project: &str,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        let path = format!("/projects/{}/builds", enc(project));
        self.transport
            .request_json(Method::GET, &path, None, query)
            .await
    }

    /// Returns a single build.
    pub async fn get(&self, project: &str, build_id: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/builds/{}", enc(project), enc(build_id));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the project's deployed build.
    pub async fn deployed(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/builds/deployed", enc(project));
        let envelope = self
            .transport
            .request_json(Method::GET, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Returns the project's live build.
    pub async fn live(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/builds/live", enc(project));
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

        let path = format!("/projects/{}/builds", enc(project));
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
            "/projects/{}/builds/{}/download",
            enc(project),
            enc(build_id)
        );
        let builder = self.transport.request_builder(Method::GET, &path);
        self.transport.execute(builder).await
    }

    /// Deploys a build.
    pub async fn deploy(&self, project: &str, build_id: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/builds/{}/deploy", enc(project), enc(build_id));
        let envelope = self
            .transport
            .request_json(Method::POST, &path, None, &[])
            .await?;
        Ok(data_object(envelope))
    }
}

impl ContextResource {
    /// Returns the context variable list envelope for a project.
    pub async fn list(&self, project: &str) -> Result<JsonObject> {
        let path = format!("/projects/{}/context", enc(project));
        self.transport
            .request_json(Method::GET, &path, None, &[])
            .await
    }

    /// Creates a context variable.
    pub async fn create(&self, project: &str, body: impl Serialize) -> Result<JsonObject> {
        let path = format!("/projects/{}/context", enc(project));
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
        let path = format!("/projects/{}/context/{}", enc(project), enc(key));
        let envelope = self
            .transport
            .request_json(Method::PUT, &path, Some(&serde_json::to_value(body)?), &[])
            .await?;
        Ok(data_object(envelope))
    }

    /// Deletes a context variable.
    pub async fn delete(&self, project: &str, key: &str) -> Result<()> {
        let path = format!("/projects/{}/context/{}", enc(project), enc(key));
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
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

    /// Returns the domain list envelope.
    pub async fn list(&self) -> Result<JsonObject> {
        self.transport
            .request_json(Method::GET, "/domains", None, &[])
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
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
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
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
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
        let builder = self.transport.request_builder(Method::DELETE, &path);
        self.transport.execute(builder).await?;
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

fn project_query(project: Option<&str>) -> Vec<(String, String)> {
    match project {
        Some(project) => vec![("project".to_string(), project.to_string())],
        None => Vec::new(),
    }
}
