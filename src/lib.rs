//! Official Rust SDK for the [Hot Dev](https://hot.dev) API.
//!
//! The client is a thin mirror of the Hot API v1 resources. Request and
//! response payloads use the Hot API wire format (`event_type`, `stream_id`,
//! `event_data`, ...) as [`JsonObject`] values; the SDK never transforms
//! user-owned payloads such as `event_data`. SDK-only options use
//! Rust-idiomatic names (`base_url`, `timeout`).
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use hot_dev::{HotClient, StreamEventExt, SubscribeWithEventOptions};
//! use serde_json::json;
//!
//! # async fn demo() -> hot_dev::Result<()> {
//! let client = HotClient::builder(std::env::var("HOT_API_KEY").unwrap()).build();
//!
//! // base_url defaults to https://api.hot.dev. For local development with
//! // `hot dev`, use .base_url("http://localhost:4681").
//!
//! let mut events = client.streams().subscribe_with_event(
//!     json!({
//!         "event_type": "team-agent:ask",
//!         "event_data": { "question": "what is blocking launch?" },
//!     }),
//!     SubscribeWithEventOptions::default(),
//! );
//! while let Some(event) = events.next().await {
//!     let event = event?;
//!     if event.event_type() == "run:stop" {
//!         println!("{:?}", event.run());
//!         break;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Authenticated clients should run server-side; browser or untrusted clients
//! should call your own backend instead of embedding a Hot API key.

mod client;
mod error;
mod management;
mod resources;
mod sse;
mod streaming;
mod transport;

pub use client::{HotClient, HotClientBuilder};
pub use error::{ApiError, Error};
pub use management::{HotManagementClient, HotManagementClientBuilder};
pub use resources::{
    BuildUpload, BuildsResource, CallOptions, ContextResource, DomainsResource, EnvResource,
    EventsResource, FilesResource, OrgResource, ProjectsResource, RunWaitOptions, RunsResource,
    ServiceKeysResource, SessionsResource, StreamsResource, SubscribeWithEventOptions,
    TaskWaitOptions, TasksResource, WaitOptions,
};
pub use streaming::{EventStream, StreamEventExt};

/// A JSON object in the Hot API wire format.
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

/// One event from a Hot run stream (`type`, `data_type`, `payload`, `run`, ...).
pub type StreamEvent = JsonObject;

/// Result alias for SDK operations.
pub type Result<T> = std::result::Result<T, Error>;
