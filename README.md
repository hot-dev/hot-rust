# hot-rust

Official Rust SDK for the [Hot Dev](https://hot.dev) API.

- crates.io: `hot-dev`
- Import: `hot_dev`
- Async (tokio); TLS via rustls
- MSRV: 1.86

## Install

```bash
cargo add hot-dev tokio --features tokio/full
cargo add futures-util serde_json
```

## Quick Start

```rust,no_run
use futures_util::StreamExt;
use hot_dev::{HotClient, StreamEventExt, SubscribeWithEventOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> hot_dev::Result<()> {
    let client = HotClient::builder(std::env::var("HOT_API_KEY").unwrap()).build();

    // base_url defaults to https://api.hot.dev. For local development with
    // `hot dev`, use .base_url("http://localhost:4681").

    let mut events = client.streams().subscribe_with_event(
        json!({
            "event_type": "team-agent:ask",
            "event_data": {
                "session_id": "web:chat:demo",
                "user_id": "web:user:demo",
                "user_name": "Demo User",
                "question": "what is blocking launch?",
            },
        }),
        SubscribeWithEventOptions::default(),
    );
    while let Some(event) = events.next().await {
        let event = event?;

        if event.event_type() == "stream:data" {
            println!("{:?} {:?}", event.get("data_type"), event.get("payload"));
        }

        if event.event_type() == "run:stop" {
            println!("{:?}", event.run().and_then(|run| run.get("result")));
            break;
        }
    }
    Ok(())
}
```

Authenticated clients should run server-side. Browser (wasm) apps and
untrusted clients should call your own backend route instead of exposing a Hot
API key. Most management endpoints require an API key. Sessions and service
keys are permission-scoped and are mainly for event publishing and stream
reads.

When code has a run id but does not need the full stream, wait on its durable
terminal snapshot:

```rust,no_run
# async fn demo(client: hot_dev::HotClient, run_id: &str) -> hot_dev::Result<()> {
let run = client
    .runs()
    .wait(run_id, hot_dev::RunWaitOptions::default())
    .await?;
# Ok(())
# }
```

When a run returns a background task id, wait through the durable task resource:

```rust,no_run
# async fn demo(client: hot_dev::HotClient, task_id: &str) -> hot_dev::Result<()> {
let task = client
    .tasks()
    .wait(task_id, hot_dev::TaskWaitOptions {
        timeout: std::time::Duration::from_secs(300),
        ..Default::default()
    })
    .await?;
# Ok(())
# }
```

The task waiter receives the latest persisted state first and reconnects, so it
cannot miss a task that completed before subscription. Failed, cancelled, and
timed-out tasks return a structured error with the final task record.
The task's parent stream also emits durable `task:update` events, which is
useful when one subscription needs to coordinate several tasks.

## Errors

Non-2xx API responses return `Error::Api(ApiError)` with structured fields:

```rust,no_run
# async fn demo(client: hot_dev::HotClient) {
match client.projects().get("missing-project").await {
    Err(hot_dev::Error::Api(error)) => {
        println!(
            "{} {:?} {:?} {:?}",
            error.status_code, error.code, error.request_id, error.retry_after
        );
    }
    other => drop(other),
}
# }
```

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`; other errors are returned as-is. Streaming and raw
requests are never retried.

## Resources

`HotClient` mirrors the Hot API v1 resources:

- `client.events()` — publish, list, get, inspect event runs, and call Hot functions with `call_hot(fn, args, opts)`
- `client.streams()` — subscribe to run and task updates, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; set `reconnect: false` to opt out)
- `client.runs()` — list, inspect, subscribe to, and wait for durable runs
- `client.tasks()` — get, subscribe to, and wait for durable background tasks
- `client.files()` — upload, download, list, and delete files (including multipart uploads)
- `client.projects()` — create, list, update, activate, deactivate, and delete projects
- `client.builds()` — upload, download, deploy, and look up live/deployed builds
- `client.context()` — manage encrypted project context variables
- `client.domains()` — register, verify, list, and delete custom domains
- `client.sessions()` — create and revoke scoped sessions
- `client.service_keys()` — create and revoke scoped service keys
- `client.org()` — view usage and limits
- `client.env()` — read environment info and subscribe to environment events

Use `client.request(...)` for JSON endpoints that do not yet have a resource
helper, or `client.request_builder(...)` + `client.execute(...)` for full
control over the request.

`client.env().subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503.
`subscribe_with_event` reconnects across the API's 5-minute SSE timeout and
stops after the terminal run correlated to the event it published. Unrelated
runs on the same stream do not end the iterator.

## Customizing the HTTP Client

Pass your own `reqwest::Client` for proxies, pools, or custom TLS:

```rust,no_run
let http = reqwest::Client::builder().build().unwrap();
let client = hot_dev::HotClient::builder("token").http_client(http).build();
```

Do not set a client-wide timeout on a client used for SSE subscriptions — it
bounds the whole request, including the stream. Use the builder's `timeout`
(JSON requests only) instead.

## Request Bodies

Body parameters accept any `serde::Serialize` value: `serde_json::json!`
literals, `JsonObject` maps, or your own `#[derive(Serialize)]` structs.
Responses are returned as `JsonObject` in the wire format.

## Casing Policy

Core API request and response payloads use the Hot API wire format:
`event_type`, `event_data`, `stream_id`, and so on. SDK-only options use Rust
style names such as `base_url` and `timeout`.

The SDK never transforms user-owned payloads such as `event_data`.

## Local Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
```

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Python SDK](https://pypi.org/project/hot-dev/)

## License

Apache-2.0
