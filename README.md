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
API key. Administrative runtime endpoints require an API key with appropriate grants. Sessions and service
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

### Reconnects

The durable helpers (`runs().wait`, `tasks().wait`,
`streams().wait_for_run_result`, `events().call_hot`, and
`streams().subscribe_with_event`) share one reconnect policy:

- Permanent errors end the wait at once: every 4xx except 408 and 429 (for
  example a revoked key's `401`, a `403`, or a `404`).
- Transport errors, dead connections (nothing, not even a keepalive, for 45
  seconds), `408`, `429`, and `5xx` reconnect with jittered exponential
  backoff (0.25–0.5 s, doubling to at most 5–10 s), waiting at least the
  server's `retry_after`.
- `max_attempts` (default 5) caps *consecutive* failed connections. A
  connection that delivers events resets the count, and the API's routine
  5-minute rollover (`stream:complete`) reconnects at once, so a run of any
  length can be awaited.
- The `timeout` bounds everything, backoff included: a wait never outlives it.

Run-result helpers follow handler retries: a run that fails with status
`pending_retry` is not the result, and the retry run (linked by
`origin_run_id`) is followed instead. Streamed `on_chunk` text is buffered
per run, so chunks that arrive before `run:start` are still delivered, in
order.

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

For a `403 key_scope_mismatch`, `error.required_scope()` returns the kind of
key the call needs (`KeyScope::Org` or `KeyScope::Env`): see
[Org API keys](#org-api-keys).

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`, and idempotent ones (`GET`, `PUT`, `DELETE`) also on
502, 503, or 504; other errors are returned as-is. Each JSON request is bounded
by the builder's `timeout` (default 60 seconds). Raw requests (uploads and
downloads) are never retried; streaming helpers reconnect as described under
[Reconnects](#reconnects).

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
- `client.env()` — read environment info, subscribe to environment events, and purge call traces
- `client.orgs()` and `client.api_keys()` — child organizations, environments, and API keys, with an org API key (see [Org API keys](#org-api-keys))

List helpers return the first page of results (`data` plus `pagination` with
`total`, `limit`, `offset`, and `has_more`). Methods that take a `query` accept
`limit` (up to 500) and `offset`; `context().list_with_query`,
`domains().list_with_query`, `projects().event_handlers_with_query`, and
`projects().schedules_with_query` page the helpers that take none.

Use `client.request(...)` for JSON endpoints that do not yet have a resource
helper, or `client.request_builder(...)` + `client.execute(...)` for full
control over the request.

`client.env().subscribe()` requires API key credentials and a live API pub/sub
backend; local API servers without pub/sub return a 503.
`subscribe_with_event` reconnects across the API's 5-minute SSE timeout and
stops after the terminal run correlated to the event it published. Unrelated
runs on the same stream do not end the iterator.

## Customizing the HTTP Client

Pass your own `reqwest::Client` for proxies, pools, or custom TLS. Build it
with the `reqwest` the SDK re-exports, so the types match whatever `reqwest`
version your application uses:

```rust,no_run
let http = hot_dev::reqwest::Client::builder().build().unwrap();
let client = hot_dev::HotClient::builder("token").http_client(http).build();
```

Tokens are trimmed, so one read from a file or secret store with a trailing
newline works. A token that is still not a valid header value fails every
request before it is sent; `HotClientBuilder::try_build` reports it when the
client is built instead.

Do not set a client-wide timeout on a client used for SSE subscriptions — it
bounds the whole request, including the stream. Use the builder's `timeout`
(JSON requests only) instead.

### TLS dependency updates

Since 1.3.0 the SDK requires `rustls` 0.23.46, `rustls-webpki` 0.103.16, and
`webpki-roots` 1.0.10 or newer compatible versions (RUSTSEC-2026-0346 to 0349,
0381, 0382). These minimums apply to applications using the published crate,
independently of this repository's lockfile.

When upgrading an existing application, refresh its TLS dependencies and rebuild:

```bash
cargo update -p rustls -p rustls-webpki -p webpki-roots
```

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

## Org API keys

Requires Hot 3.0.1 or later. An org API key belongs to a root organization and
calls only the control-plane routes: `client.orgs()` and `client.api_keys()`.
It provisions child organizations and their environments and mints
environment API keys there. Runtime calls (events, streams, runs, and the rest)
need an environment API key; use a separate `HotClient` per environment key. A
call with the wrong kind of key returns `403 key_scope_mismatch`, and
`error.required_scope()` names the key the call needs.

```rust,no_run
use hot_dev::{HotClient, KeyScope};
use serde_json::json;
# async fn example(org_key: &str) -> hot_dev::Result<()> {
let org = HotClient::new(org_key);
// Keep the idempotency key stable across retries. Slugs have at most 32 characters.
let child = org.orgs().create(json!({
    "name": "Customer", "slug": "customer-123",
    "idempotency_key": "customer-provisioning-123",
})).await?;
let org_id = child["org"]["org_id"].as_str().unwrap();
let env_id = child["env_id"].as_str().unwrap();
let key = org.orgs().create_env_api_key(org_id, env_id, json!({
    "description": "Customer backend",
    "permissions": {"event:*": ["create", "read"], "stream:*": ["read"]},
})).await?;

let customer = HotClient::new(key["token"].as_str().unwrap());
customer.events().publish(json!({"event_type": "app:hello", "event_data": {}})).await?;

// The org key cannot make runtime calls.
let error = org.events().list(&[]).await.unwrap_err();
assert_eq!(error.required_scope(), Some(KeyScope::Env));
# Ok(())
# }
```

| Method | Route |
| --- | --- |
| `orgs().list(query)` | `GET /v1/orgs` (children the key may read) |
| `orgs().create(body)` | `POST /v1/orgs` (returns `org`, `env_id`, `created`) |
| `orgs().get(org_id)` / `orgs().update(org_id, {"state": ...})` | `GET`/`PATCH /v1/orgs/{org_id}` (`active`, `suspended`, `archived`) |
| `orgs().usage(root_org_id)` | `GET /v1/orgs/{org_id}/usage` (account usage; the root's ID) |
| `orgs().purge_calls(org_id, include_sub_orgs)` | `DELETE /v1/orgs/{org_id}/calls` |
| `orgs().list_envs(org_id)` / `create_env` / `get_env` | `/v1/orgs/{org_id}/envs` |
| `orgs().list_api_keys(org_id)` / `create_api_key` | `/v1/orgs/{org_id}/api-keys` (org keys) |
| `orgs().list_env_api_keys(org_id, env_id, query)` / `create_env_api_key` | `/v1/orgs/{org_id}/envs/{env_id}/api-keys` |
| `api_keys().get` / `update` / `revoke` / `rotate(key_id, overlap_seconds)` | `/v1/api-keys/{key_id}` |
| `orgs().env(org_id, env_id).projects()` / `.builds()` / `.context()` | projects, builds, deploys, and context in that environment |

Grants use qualified resources such as `org:*` (children), `org:*/env:*`, and
`org:*/env:*/project:*`; a key can only mint keys within its own grants, and
revoking or narrowing a key restricts everything it issued. See the server's
[org API documentation](https://hot.dev/docs/api/management).

### Sub-org integration tests

The fixture in `integration/suborg_fixture.py` creates an owned disposable
database and loopback API server, seeds an org API key and a root environment
key, runs the SDK tests, and removes only its own resources. The canonical
fixture lives in hot-python and is synchronized across all five SDKs. It needs
Python 3 and a Hot CLI built from the server revision in
`integration/hot-server.ref`. For PostgreSQL, also install Docker and `psql`
and provide that hot checkout (default: sibling `hot`). Migrations use a
non-superuser schema owner; the API uses a distinct non-owner
`NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  cargo test --test suborg_integration -- --ignored
# For SQLite, use --backend sqlite.
```

Coverage includes child and environment provisioning and idempotency,
identical project names in siblings, foreign-ID and stream denial, parent
runtime isolation, key-scope errors in both directions, key narrowing,
rotation and revocation, project and context routes through an org key,
paginated context lists, service and session issuance and revocation, issuer
revocation and narrowing of existing descendants, and child suspension and
resumption.
