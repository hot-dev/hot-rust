# hot-rust — Implementation Plan

Official Rust SDK for the Hot Dev API. Mirrors `hot-python` (Layer 1 only: client,
resources, SSE streaming, errors) and follows the shared SDK conventions.

## Package identity

- crates.io name: **`hot-dev`** (confirmed unclaimed as of 2026-07-15 — publish a
  placeholder early; crates.io names are first-come)
- Library import: `hot_dev` (default dash→underscore mapping), e.g.
  `use hot_dev::HotClient;`
- License: Apache-2.0
- Versioning: lockstep with `@hot-dev/sdk` / `hot-dev` (PyPI), starting at the
  current shared version. Tags `v<version>`.
- MSRV: pin (e.g. 1.78) and test it in CI; bumping MSRV is a minor release.

## Tech choices

- **HTTP**: `reqwest` with `rustls-tls` (avoid the native-tls/openssl default for
  portability). Async-first on `tokio`. Optional `blocking` cargo feature wrapping
  `reqwest::blocking` if demand appears — don't build it in v1.
- **JSON**: `serde` + `serde_json`. Match the Python SDK's v1 typing policy:
  request/response payloads are `serde_json::Value` / `serde_json::Map`; typed
  structs generated from the OpenAPI spec are a later release. The SDK never
  transforms user-owned payloads (`event_data`).
- **SSE**: hand-rolled parser over the reqwest byte stream (both existing SDKs
  hand-roll SSE; it's ~50–90 lines and avoids a dependency).
- **Errors**: `thiserror`. `HotError` enum with `Api(HotApiError)` and
  `Transport(...)` variants. `HotApiError` carries `status_code`, `code`,
  `request_id`, `retry_after` — same fields as the other SDKs.

## Architecture (mirror hot-python's file split)

```
src/
  lib.rs          — re-exports: HotClient, HotApiError, core types
  client.rs       — HotClient::builder(): token, base_url (default https://api.hot.dev),
                    timeout, custom reqwest::Client; request()/request_raw() escape hatches
  transport.rs    — auth header, JSON request/response, error mapping
  errors.rs
  sse.rs          — SSE line/event parser
  resources/      — builds, context, domains, env, events, files, org, projects,
                    runs, service_keys, sessions, streams (12 namespaces, ~59 operations)
  streaming.rs    — StreamEvent enum, subscribe/subscribe_with_event returning
                    impl Stream<Item = Result<StreamEvent, HotError>>, wait_for_run_result
```

Behavioral contract to preserve:
- `streams.subscribe*` auto-reconnects across the API's 5-minute SSE timeout
  (opt-out flag), and `subscribe_with_event` ends after the first terminal
  `run:*` event.
- Casing policy: wire format (`event_type`, `stream_id`) in payloads;
  Rust-idiomatic names (`base_url`, `timeout`) for SDK-only options.
- `files` includes multipart upload/download; `builds` upload/download stream bytes.

## Implementation phases

1. Scaffold: `cargo new --lib`, CI, LICENSE, README, AGENTS.md, rustfmt/clippy config.
2. Transport + errors + escape-hatch `request`/`request_raw`.
3. Resource namespaces (port from `hot-python/src/hot/resources.py`, which is the
   canonical untyped reference).
4. SSE parser + reconnect loop + `subscribe_with_event` + `wait_for_run_result`.
5. Docs: rustdoc on all public items, README quick start mirroring the JS/Python
   ones (team-agent:ask example), `examples/` dir with a runnable example.
6. OpenAPI coverage test + fixture (see Testing).

## Testing

- **Unit/integration**: `wiremock` crate for mocked HTTP (JSON endpoints, error
  mapping, retry-after) and streamed SSE bodies (reconnect behavior — mirror
  `test_reconnect.py` / `reconnect.test.ts` cases).
- **OpenAPI coverage**: check in `tests/fixtures/openapi-operations.json`
  (same 59-operation format as the other SDKs) plus a port of
  `update-openapi-operations.mjs` (fetch `http://localhost:4681/openapi.json`
  from a local `hot dev`). Coverage test asserts every operation maps to a
  resource method.
- **Live smoke test** (optional, `#[ignore]`-gated): run against a local
  `hot dev` server, same as manual verification for the other SDKs.
- CI: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`,
  `cargo doc --no-deps`, on stable + MSRV, pushes/PRs to `main`.

## Publishing (crates.io)

One-time setup (Curt):
1. Create the public GitHub repo `hot-dev/hot-rust`.
2. Log in to crates.io with the hot-dev GitHub account; verify email.
3. **First publish must be manual** with an API token (`cargo login && cargo publish`)
   — crates.io Trusted Publishing can only be configured on an existing crate.
   Consider publishing `0.0.1` immediately just to reserve the `hot-dev` name.
4. After first publish: on crates.io → crate Settings → Trusted Publishing, add
   `hot-dev/hot-rust` + workflow `release.yml`. No long-lived token in CI after that.
5. Optionally add teammates as crate owners (`cargo owner --add`).

Release workflow (`.github/workflows/release.yml`, on `v*` tags):
- Validate tag matches `Cargo.toml` version (same guard as hot-js).
- fmt/clippy/test, then `rust-lang/crates-io-auth-action` (OIDC) + `cargo publish`.
- docs.rs builds automatically after publish — verify metadata renders
  (`[package.metadata.docs.rs]` if features need enabling).

Release flow: bump `Cargo.toml` version → commit `release v<version>` → tag →
push. Note crates.io publishes are **permanent** (yank-only, no delete/overwrite).

## Open questions

- Whether to also reserve the bare `hot` crate name (likely taken — check).
- Blocking client feature: skip for v1 unless a user asks.
