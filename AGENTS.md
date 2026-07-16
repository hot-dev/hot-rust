# AGENTS.md - hot-rust

This repo contains the Rust SDK for the Hot Dev API.

## Package Shape

- crates.io name: `hot-dev`; library imports as `hot_dev`
- Async-only on tokio; reqwest with rustls (no openssl); hand-rolled SSE parser
- Request bodies accept `impl Serialize` (`json!` literals, maps, or user
  structs); responses are `serde_json::Map` (`JsonObject`) in the Hot API wire
  format (`event_type`, `stream_id`). The SDK never transforms user-owned
  payloads such as `event_data`. SDK-only options use Rust-style names.
- MSRV 1.86 (declared in Cargo.toml `rust-version`; CI checks the library only — dev-dependencies may need newer toolchains)
- Versioning follows the public API package version, in lockstep with
  `@hot-dev/sdk` (npm) and `hot-dev` (PyPI).

## Commands

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
```

## Layout

- `src/client.rs` / `src/transport.rs` — builder, authorized requests, envelope handling
- `src/streaming.rs` — SSE event streams, wait_for_run_result
- `src/resources.rs` — all 12 resources (port of hot-python's resources.py)
- `tests/openapi_coverage.rs` — every OpenAPI operation must map to an SDK
  method or carry an explicit ignore; `method_exists` pins method names at
  compile time. Fixture in `tests/fixtures/`.

## CI and Releases

- CI runs on pushes and pull requests to `main`: fmt, clippy (deny warnings),
  test, doc, on stable and the MSRV toolchain.
- crates.io releases run from tags named `v<version>` matching the
  `Cargo.toml` version.
- Publishing uses crates.io Trusted Publishing (GitHub OIDC) via
  `rust-lang/crates-io-auth-action` — no long-lived token secret. The trusted
  publisher must be configured on the crate's crates.io settings first, which
  requires one initial manual `cargo publish` with an API token.
- crates.io publishes are permanent: yank-only, no deletes or overwrites.
- docs.rs builds automatically after each publish.

Release flow:

```bash
# Bump the version in Cargo.toml.
git add Cargo.toml Cargo.lock
git commit -m "release v<version>"
git tag v<version>
git push origin main v<version>
```

Always update the SDK version in lockstep with new API surface area added to
the matching `@hot-dev/sdk` JavaScript release when possible.

## Integration Tests

`integration/run.sh` starts a scratch `hot dev` (requires the `hot` CLI and
python3 on PATH) serving the Hot fixture project in `integration/fixture`
(project `sdk-fixture`: an echo event handler, an always-failing handler and
function, and hot:call targets). It binds 127.0.0.1 on HOT_TEST_PORT (default
4722), mints a full-access API key directly in the fixture's sqlite database,
exports HOT_TEST_BASE_URL / HOT_TEST_API_KEY, and runs the `#[ignore]`d tests in tests/integration.rs via `cargo test --test integration -- --ignored`.
The integration tests skip when HOT_TEST_API_KEY is unset, so regular test
runs are unaffected. The fixture pins the API to 127.0.0.1 because binding
"localhost" can yield an IPv6-only listener that JDK-style clients (which
dial the first resolved address only) cannot reach.

## Lockstep Releases

All five SDK repos (hot-js, hot-python, hot-go, hot-rust, hot-java) release in
lockstep. Checklist for a coordinated release:

1. Bump every SDK's version: js `npm version` (syncs src/version.ts via the
   `version` script), python pyproject.toml, rust Cargo.toml, java
   gradle.properties + README snippets, go `Version` constant in client.go.
2. Regenerate each repo's openapi-operations fixture from a current `hot dev`
   and reconcile the coverage tables.
3. Run every repo's `integration/run.sh`.
4. Commit, tag `v<version>`, push in each repo.

The integration fixture (integration/fixture, mint_key.py, run.sh) is
duplicated across all five repos; the canonical copy lives in **hot-python** —
change it there first, then sync the others.
