use crate::transport::{data_object, enc, Transport, DEFAULT_BASE_URL};
use crate::{JsonObject, Result};
use reqwest::Method;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// Root control-plane client. Requires an hm_ token issued by a root admin.
/// Use a separate HotClient with a child environment key for runtime calls.
#[derive(Clone)]
pub struct HotManagementClient {
    transport: Arc<Transport>,
}

/// Builder for a root management client.
pub struct HotManagementClientBuilder {
    token: String,
    base_url: String,
    timeout: Option<Duration>,
    client: Option<reqwest::Client>,
}

impl HotManagementClient {
    pub fn new(token: impl Into<String>) -> Self {
        Self::builder(token).build()
    }
    pub fn builder(token: impl Into<String>) -> HotManagementClientBuilder {
        HotManagementClientBuilder {
            token: token.into(),
            base_url: DEFAULT_BASE_URL.into(),
            timeout: None,
            client: None,
        }
    }
    /// Returns children and total for authorized targets, not a list envelope.
    pub async fn list_children(&self, query: &[(&str, &str)]) -> Result<JsonObject> {
        self.data(Method::GET, "/management/children", None, query)
            .await
    }
    /// Provision a child. Keep idempotency_key stable across retries.
    pub async fn create_child(&self, body: impl Serialize) -> Result<JsonObject> {
        self.data(
            Method::POST,
            "/management/children",
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }
    /// Set active, suspended, or archived. Success has no body.
    pub async fn set_child_state(&self, child_id: &str, state: &str) -> Result<()> {
        self.data(
            Method::PATCH,
            &format!("/management/children/{}/state", enc(child_id)),
            Some(json!({"state":state})),
            &[],
        )
        .await?;
        Ok(())
    }
    pub async fn create_environment(
        &self,
        child_id: &str,
        body: impl Serialize,
    ) -> Result<JsonObject> {
        self.data(
            Method::POST,
            &format!("/management/children/{}/environments", enc(child_id)),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }
    /// Issue a child environment key within this credential's delegation ceiling.
    pub async fn create_api_key(
        &self,
        child_id: &str,
        env_id: &str,
        body: impl Serialize,
    ) -> Result<JsonObject> {
        self.data(
            Method::POST,
            &format!(
                "/management/children/{}/environments/{}/api-keys",
                enc(child_id),
                enc(env_id)
            ),
            Some(serde_json::to_value(body)?),
            &[],
        )
        .await
    }
    /// Account aggregate; requires usage:read and explicit all_children authority.
    pub async fn usage(&self) -> Result<JsonObject> {
        self.data(Method::GET, "/management/usage", None, &[]).await
    }
    async fn data(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        query: &[(&str, &str)],
    ) -> Result<JsonObject> {
        Ok(data_object(
            self.transport
                .request_json(method, path, body.as_ref(), query)
                .await?,
        ))
    }
}

impl HotManagementClientBuilder {
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }
    pub fn build(self) -> HotManagementClient {
        HotManagementClient {
            transport: Arc::new(Transport::new(
                self.token,
                self.base_url,
                self.timeout,
                self.client,
            )),
        }
    }
}
