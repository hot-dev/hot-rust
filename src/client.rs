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
use crate::transport::{authorization, Transport, DEFAULT_BASE_URL};
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

/// Builder for [`HotClient`].
pub struct HotClientBuilder {
    token: String,
    base_url: String,
    timeout: Option<Duration>,
    client: Option<reqwest::Client>,
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

    /// The configured base URL without the /v1 suffix.
    pub fn base_url(&self) -> &str {
        self.transport.base_url()
    }

    /// Blobs that Hot spilled large payloads to. Requires an environment API
    /// key.
    pub fn blobs(&self) -> BlobsResource {
        BlobsResource::new(self.transport.clone())
    }

    pub fn builds(&self) -> BuildsResource {
        BuildsResource::new(self.transport.clone())
    }

    pub fn context(&self) -> ContextResource {
        ContextResource::new(self.transport.clone())
    }

    pub fn domains(&self) -> DomainsResource {
        DomainsResource::new(self.transport.clone())
    }

    pub fn env(&self) -> EnvResource {
        EnvResource::new(self.transport.clone())
    }

    pub fn events(&self) -> EventsResource {
        EventsResource::new(self.transport.clone())
    }

    pub fn files(&self) -> FilesResource {
        FilesResource::new(self.transport.clone())
    }

    pub fn org(&self) -> OrgResource {
        OrgResource::new(self.transport.clone())
    }

    /// Child organizations, their environments, and API keys. Requires an
    /// org API key.
    pub fn orgs(&self) -> OrgsResource {
        OrgsResource::new(self.transport.clone())
    }

    /// Org and environment API keys by ID: read, narrow, rotate, revoke.
    /// Requires an org API key.
    pub fn api_keys(&self) -> ApiKeysResource {
        ApiKeysResource::new(self.transport.clone())
    }

    pub fn projects(&self) -> ProjectsResource {
        ProjectsResource::new(self.transport.clone())
    }

    pub fn runs(&self) -> RunsResource {
        RunsResource::new(self.transport.clone())
    }

    pub fn service_keys(&self) -> ServiceKeysResource {
        ServiceKeysResource::new(self.transport.clone())
    }

    pub fn sessions(&self) -> SessionsResource {
        SessionsResource::new(self.transport.clone())
    }

    pub fn streams(&self) -> StreamsResource {
        StreamsResource::new(self.transport.clone())
    }

    pub fn tasks(&self) -> TasksResource {
        TasksResource::new(self.transport.clone())
    }

    /// Calls a JSON API endpoint that does not yet have a resource helper.
    /// `path` is relative to /v1 and the returned object is the response
    /// envelope.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        self.transport.request_json(method, path, body, query).await
    }

    /// Returns an authorized [`RequestBuilder`] for a path relative to /v1,
    /// for endpoints needing full control (raw bodies, custom headers).
    /// Send it with [`HotClient::execute`] to get API error mapping.
    pub fn request_builder(&self, method: Method, path: &str) -> RequestBuilder {
        self.transport.request_builder(method, path)
    }

    /// Sends a request built with [`HotClient::request_builder`], mapping
    /// non-2xx responses to [`crate::ApiError`].
    pub async fn execute(&self, builder: RequestBuilder) -> Result<Response> {
        self.transport.execute(builder).await
    }
}

impl HotClientBuilder {
    /// Overrides the base URL (default `https://api.hot.dev`). For local
    /// development with `hot dev`, use `http://localhost:4681`.
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

    /// Builds the client. A token that is not a valid HTTP header value
    /// even after trimming (for example one with an embedded newline) makes
    /// every request fail with a request-builder [`crate::Error::Http`]
    /// before anything is sent; use [`HotClientBuilder::try_build`] to reject
    /// it here instead.
    pub fn build(self) -> HotClient {
        let authorization = authorization(&self.token).ok();
        self.finish(authorization)
    }

    /// Builds the client, or returns an error when the token is not a valid
    /// HTTP header value after trimming surrounding whitespace.
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
