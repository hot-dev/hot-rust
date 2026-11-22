use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::InvalidHeaderValue;
use reqwest::{Method, RequestBuilder, Response};
use serde_json::Value;

use crate::resources::{
    ApiKeysResource, BlobsResource, BuildsResource, ContextResource, DomainsResource, EnvResource,
    EventsResource, FilesResource, OrgResource, OrgsResource, ProjectsResource, RunsResource,
    ServiceKeysResource, SessionsResource, StreamsResource, TasksResource,
};
use crate::transport::{authorization, normalize_api_path, Transport, DEFAULT_BASE_URL};
use crate::{JsonObject, Result};

/// Hot API v1 client. Construct it with [`HotClient::builder`]; the resource
/// accessors mirror the Hot API resources.
///
/// One client works with either kind of API key. Environment keys (and the
/// sessions and service keys they issue) call the runtime resources; org API
/// keys call only [`HotClient::orgs`] and [`HotClient::api_keys`]. Calling
/// the other kind's routes returns a `403 key_scope_mismatch`
/// [`crate::ApiError`] whose [`crate::ApiError::required_scope`] names the
/// key the call needs.
#[derive(Clone)]
pub struct HotClient {
    transport: Arc<Transport>,
}

impl fmt::Debug for HotClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotClient")
            .field("base_url", &self.transport.base_url())
            .field("timeout", &self.transport.timeout())
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}

/// Builder for [`HotClient`].
pub struct HotClientBuilder {
    token: String,
    base_url: String,
    timeout: Option<Duration>,
    client: Option<reqwest::Client>,
}

impl fmt::Debug for HotClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotClientBuilder")
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .field("has_http_client", &self.client.is_some())
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl HotClient {
    /// Returns a client with default configuration.
    pub fn new(token: impl Into<String>) -> HotClient {
        HotClient::builder(token).build()
    }

    /// Returns a builder using the given Hot API key (environment or org),
    /// session token, or service key. Surrounding whitespace is removed.
    pub fn builder(token: impl Into<String>) -> HotClientBuilder {
        HotClientBuilder {
            token: token.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            timeout: None,
            client: None,
        }
    }

    /// The configured base URL without any trailing `/` or `/v1` suffix.
    pub fn base_url(&self) -> &str {
        self.transport.base_url()
    }

    /// Returns the public server health status from `GET /status` without
    /// sending an `Authorization` header.
    pub async fn status(&self) -> Result<Value> {
        self.transport.status().await
    }

    /// Blobs that Hot spilled large payloads to. Requires an environment API
    /// key.
    pub fn blobs(&self) -> BlobsResource {
        BlobsResource::new(self.transport.clone())
    }

    /// Uploads, deploys, and inspects project builds.
    pub fn builds(&self) -> BuildsResource {
        BuildsResource::new(self.transport.clone())
    }

    /// Manages encrypted project context variables.
    pub fn context(&self) -> ContextResource {
        ContextResource::new(self.transport.clone())
    }

    /// Registers, verifies, lists, and deletes custom domains.
    pub fn domains(&self) -> DomainsResource {
        DomainsResource::new(self.transport.clone())
    }

    /// Reads environment info, subscribes to environment events, and purges
    /// environment call traces.
    pub fn env(&self) -> EnvResource {
        EnvResource::new(self.transport.clone())
    }

    /// Publishes, lists, and inspects events and invokes Hot functions via
    /// `call_hot`.
    pub fn events(&self) -> EventsResource {
        EventsResource::new(self.transport.clone())
    }

    /// Uploads, downloads, lists, and deletes files, including multipart
    /// uploads.
    pub fn files(&self) -> FilesResource {
        FilesResource::new(self.transport.clone())
    }

    /// Reports organization usage and billing limits for the caller's
    /// environment.
    pub fn org(&self) -> OrgResource {
        OrgResource::new(self.transport.clone())
    }

    /// Sub-organizations, their environments, and API keys. Requires an
    /// org API key.
    pub fn orgs(&self) -> OrgsResource {
        OrgsResource::new(self.transport.clone())
    }

    /// Org and environment API keys by ID: read, narrow, rotate, revoke.
    /// Requires an org API key.
    pub fn api_keys(&self) -> ApiKeysResource {
        ApiKeysResource::new(self.transport.clone())
    }

    /// Creates, lists, updates, activates, deactivates, and deletes projects.
    pub fn projects(&self) -> ProjectsResource {
        ProjectsResource::new(self.transport.clone())
    }

    /// Lists, inspects, subscribes to, and waits for durable runs.
    pub fn runs(&self) -> RunsResource {
        RunsResource::new(self.transport.clone())
    }

    /// Creates, lists, updates, and revokes scoped service keys.
    pub fn service_keys(&self) -> ServiceKeysResource {
        ServiceKeysResource::new(self.transport.clone())
    }

    /// Creates, lists, and revokes scoped sessions.
    pub fn sessions(&self) -> SessionsResource {
        SessionsResource::new(self.transport.clone())
    }

    /// Subscribes to run streams, publishes events atomically, and waits for
    /// run results.
    pub fn streams(&self) -> StreamsResource {
        StreamsResource::new(self.transport.clone())
    }

    /// Reads, subscribes to, and waits for durable background tasks.
    pub fn tasks(&self) -> TasksResource {
        TasksResource::new(self.transport.clone())
    }

    /// Calls a JSON API endpoint that does not yet have a resource helper.
    /// `path` is relative to `/v1` (a missing leading `/` is added) and the
    /// returned object is the response envelope.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        self.transport.request_json(method, path, body, query).await
    }

    /// Returns an authorized [`RequestBuilder`] for a path relative to `/v1`,
    /// for endpoints needing full control (raw bodies, custom headers).
    /// Send it with [`HotClient::execute`] to get API error mapping.
    pub fn request_builder(&self, method: Method, path: &str) -> RequestBuilder {
        let normalized = normalize_api_path(path);
        self.transport.request_builder(method, &normalized)
    }

    /// Sends a request built with [`HotClient::request_builder`], mapping
    /// non-2xx responses to [`crate::ApiError`].
    pub async fn execute(&self, builder: RequestBuilder) -> Result<Response> {
        self.transport.execute(builder).await
    }
}

impl HotClientBuilder {
    /// Overrides the base URL (default `https://api.hot.dev`). Trailing `/`
    /// and `/v1` suffixes are normalized automatically. For local development
    /// with `hot dev`, use `http://localhost:4681`.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Bounds each JSON request (default 60 seconds). Not applied to
    /// streaming or raw requests; waiters take their own timeouts.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Overrides the underlying [`reqwest::Client`], e.g. to configure
    /// proxies or connection pools. Do not set a client-wide timeout on a
    /// client used for SSE subscriptions — it would sever long-lived streams.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Builds the client. A token that is empty or not a valid HTTP header
    /// value after trimming (for example whitespace-only or containing an
    /// embedded newline) makes every authenticated request fail with a
    /// request-builder [`crate::Error::Http`] before anything is sent; use
    /// [`HotClientBuilder::try_build`] to reject it here instead.
    pub fn build(self) -> HotClient {
        let authorization = authorization(&self.token).ok();
        self.finish(authorization)
    }

    /// Builds the client, or returns an error when the token is empty or not
    /// a valid HTTP header value after trimming surrounding whitespace.
    pub fn try_build(self) -> std::result::Result<HotClient, InvalidHeaderValue> {
        let authorization = authorization(&self.token)?;
        Ok(self.finish(Some(authorization)))
    }

    fn finish(self, authorization: Option<reqwest::header::HeaderValue>) -> HotClient {
        HotClient {
            transport: Arc::new(Transport::new(
                authorization,
                self.base_url,
                self.timeout,
                self.client,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ApiError, BuildUpload, Error, EventStream, KeyScope, OrgEnv, RunWaitOptions,
        SubscribeWithEventOptions, TaskWaitOptions,
    };

    #[test]
    fn try_build_rejects_empty_whitespace_and_invalid_tokens() {
        assert!(HotClient::builder("").try_build().is_err());
        assert!(HotClient::builder("   \t\n  ").try_build().is_err());
        assert!(HotClient::builder("hot_a\nhot_b").try_build().is_err());
        assert!(HotClient::builder("  hot_valid\n").try_build().is_ok());
    }

    #[test]
    fn debug_redacts_token_on_client_and_builder() {
        let builder = HotClient::builder("hot_top_secret_999");
        let builder_dbg = format!("{builder:?}");
        assert!(!builder_dbg.contains("hot_top_secret_999"), "{builder_dbg}");
        assert!(builder_dbg.contains("[REDACTED]"), "{builder_dbg}");

        let client = builder.build();
        let client_dbg = format!("{client:?}");
        assert!(!client_dbg.contains("hot_top_secret_999"), "{client_dbg}");
        assert!(client_dbg.contains("[REDACTED]"), "{client_dbg}");
    }

    #[test]
    fn base_url_strips_trailing_slash_and_v1() {
        let c1 = HotClient::builder("tok")
            .base_url("https://api.hot.dev/v1/")
            .build();
        assert_eq!(c1.base_url(), "https://api.hot.dev");

        let c2 = HotClient::builder("tok")
            .base_url("http://localhost:4681/v1")
            .build();
        assert_eq!(c2.base_url(), "http://localhost:4681");
    }

    #[test]
    fn public_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}

        assert_send_sync::<HotClient>();
        assert_send_sync::<HotClientBuilder>();
        assert_send_sync::<ApiKeysResource>();
        assert_send_sync::<BlobsResource>();
        assert_send_sync::<BuildsResource>();
        assert_send_sync::<ContextResource>();
        assert_send_sync::<DomainsResource>();
        assert_send_sync::<EnvResource>();
        assert_send_sync::<EventsResource>();
        assert_send_sync::<FilesResource>();
        assert_send_sync::<OrgResource>();
        assert_send_sync::<OrgsResource>();
        assert_send_sync::<OrgEnv>();
        assert_send_sync::<ProjectsResource>();
        assert_send_sync::<RunsResource>();
        assert_send_sync::<ServiceKeysResource>();
        assert_send_sync::<SessionsResource>();
        assert_send_sync::<StreamsResource>();
        assert_send_sync::<TasksResource>();
        assert_send_sync::<SubscribeWithEventOptions>();
        assert_send_sync::<RunWaitOptions>();
        assert_send_sync::<TaskWaitOptions>();
        assert_send_sync::<BuildUpload>();
        assert_send_sync::<Error>();
        assert_send_sync::<ApiError>();
        assert_send_sync::<KeyScope>();
        assert_send::<EventStream>();
    }
}
