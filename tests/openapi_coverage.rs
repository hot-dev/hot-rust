//! Every OpenAPI operation must map to an SDK method or carry an explicit
//! ignore. Rust has no runtime reflection, so method existence is enforced by
//! `method_exists` below, which references each covered method at compile
//! time. Regenerate the fixture from a local `hot dev` server:
//!
//! ```bash
//! curl -s http://localhost:4681/openapi.json | python3 -c '
//! import json,sys
//! spec=json.load(sys.stdin)
//! ops=[{"method":m.upper(),"path":p} for p,i in spec["paths"].items()
//!      for m in ("get","post","put","patch","delete") if m in i]
//! ops.sort(key=lambda o:(o["path"],o["method"]))
//! print(json.dumps(ops,indent=2))' > tests/fixtures/openapi-operations.json
//! ```

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

fn coverage() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("GET /v1/orgs", "orgs.list"),
        ("POST /v1/orgs", "orgs.create"),
        ("GET /v1/orgs/{org_id}", "orgs.get"),
        ("PATCH /v1/orgs/{org_id}", "orgs.update"),
        ("GET /v1/orgs/{org_id}/usage", "orgs.usage"),
        ("DELETE /v1/orgs/{org_id}/calls", "orgs.purge_calls"),
        ("GET /v1/orgs/{org_id}/envs", "orgs.list_envs"),
        ("POST /v1/orgs/{org_id}/envs", "orgs.create_env"),
        ("GET /v1/orgs/{org_id}/envs/{env_id}", "orgs.get_env"),
        ("GET /v1/orgs/{org_id}/api-keys", "orgs.list_api_keys"),
        ("POST /v1/orgs/{org_id}/api-keys", "orgs.create_api_key"),
        (
            "GET /v1/orgs/{org_id}/envs/{env_id}/api-keys",
            "orgs.list_env_api_keys",
        ),
        (
            "POST /v1/orgs/{org_id}/envs/{env_id}/api-keys",
            "orgs.create_env_api_key",
        ),
        // The spec lists only these two of the forwarded environment routes;
        // orgs.env(org, env) reaches every project, build, and context route.
        (
            "GET /v1/orgs/{org_id}/envs/{env_id}/projects",
            "orgs.env.projects.list",
        ),
        (
            "GET /v1/orgs/{org_id}/envs/{env_id}/builds",
            "orgs.env.builds.list",
        ),
        ("GET /v1/api-keys/{key_id}", "api_keys.get"),
        ("PATCH /v1/api-keys/{key_id}", "api_keys.update"),
        ("DELETE /v1/api-keys/{key_id}", "api_keys.revoke"),
        ("POST /v1/api-keys/{key_id}/rotate", "api_keys.rotate"),
        ("DELETE /v1/env/calls", "env.purge_calls"),
        ("DELETE /v1/runs/{run_id}/calls", "runs.purge_calls"),
        (
            "DELETE /v1/streams/{stream_id}/calls",
            "streams.purge_calls",
        ),
        // "" marks operations intentionally left out of the SDK.
        ("GET /status", ""), // public health endpoint
        ("GET /v1/projects", "projects.list"),
        ("POST /v1/projects", "projects.create"),
        ("GET /v1/projects/{project_id_or_slug}", "projects.get"),
        ("PATCH /v1/projects/{project_id_or_slug}", "projects.update"),
        (
            "DELETE /v1/projects/{project_id_or_slug}",
            "projects.delete",
        ),
        (
            "POST /v1/projects/{project_id_or_slug}/activate",
            "projects.activate",
        ),
        (
            "POST /v1/projects/{project_id_or_slug}/deactivate",
            "projects.deactivate",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/event-handlers",
            "projects.event_handlers",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/schedules",
            "projects.schedules",
        ),
        ("GET /v1/builds", "builds.list"),
        (
            "GET /v1/projects/{project_id_or_slug}/builds",
            "builds.list_for_project",
        ),
        (
            "POST /v1/projects/{project_id_or_slug}/builds",
            "builds.upload",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/builds/deployed",
            "builds.deployed",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/builds/live",
            "builds.live",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/builds/{build_id}",
            "builds.get",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/builds/{build_id}/download",
            "builds.download",
        ),
        (
            "POST /v1/projects/{project_id_or_slug}/builds/{build_id}/deploy",
            "builds.deploy",
        ),
        (
            "GET /v1/projects/{project_id_or_slug}/context",
            "context.list",
        ),
        (
            "POST /v1/projects/{project_id_or_slug}/context",
            "context.create",
        ),
        (
            "PUT /v1/projects/{project_id_or_slug}/context/{key}",
            "context.update",
        ),
        (
            "DELETE /v1/projects/{project_id_or_slug}/context/{key}",
            "context.delete",
        ),
        ("GET /v1/runs", "runs.list"),
        ("GET /v1/runs/stats", "runs.stats"),
        ("GET /v1/runs/{run_id}", "runs.get"),
        ("GET /v1/runs/{run_id}/subscribe", "runs.subscribe"),
        ("GET /v1/tasks/{task_id}", "tasks.get"),
        ("GET /v1/tasks/{task_id}/subscribe", "tasks.subscribe"),
        ("POST /v1/events", "events.publish"),
        ("GET /v1/events", "events.list"),
        ("GET /v1/events/{event_id}", "events.get"),
        ("GET /v1/events/{event_id}/runs", "events.get_runs"),
        ("POST /v1/sessions", "sessions.create"),
        ("GET /v1/sessions", "sessions.list"),
        ("DELETE /v1/sessions", "sessions.revoke_all"),
        ("DELETE /v1/sessions/{session_id}", "sessions.revoke"),
        ("POST /v1/service-keys", "service_keys.create"),
        ("GET /v1/service-keys", "service_keys.list"),
        ("DELETE /v1/service-keys", "service_keys.revoke_all"),
        ("GET /v1/service-keys/{service_key_id}", "service_keys.get"),
        (
            "PATCH /v1/service-keys/{service_key_id}",
            "service_keys.update",
        ),
        (
            "DELETE /v1/service-keys/{service_key_id}",
            "service_keys.revoke",
        ),
        ("POST /v1/domains", "domains.create"),
        ("GET /v1/domains", "domains.list"),
        ("GET /v1/domains/{domain_id}", "domains.get"),
        ("DELETE /v1/domains/{domain_id}", "domains.delete"),
        ("POST /v1/domains/{domain_id}/verify", "domains.verify"),
        ("GET /v1/env", "env.get"),
        ("GET /v1/env/subscribe", "env.subscribe"),
        ("GET /v1/org/usage", "org.usage"),
        ("GET /v1/streams/{stream_id}/subscribe", "streams.subscribe"),
        (
            "POST /v1/streams/{stream_id}/subscribe",
            "streams.subscribe_post",
        ),
        (
            "POST /v1/streams/subscribe-with-event",
            "streams.subscribe_with_event",
        ),
        ("GET /v1/files", "files.list"),
        ("GET /v1/files/{file_id}", "files.get"),
        ("DELETE /v1/files/{file_id}", "files.delete"),
        ("GET /v1/files/{file_id}/download", "files.download"),
        ("GET /v1/blobs/{blob_ref_id}/download", "blobs.download"),
        ("PUT /v1/files/upload/{path}", "files.upload"),
        ("POST /v1/files/uploads", "files.initiate_upload"),
        (
            "PUT /v1/files/uploads/{upload_id}/{part_number}",
            "files.upload_part",
        ),
        (
            "POST /v1/files/uploads/{upload_id}/complete",
            "files.complete_upload",
        ),
        ("DELETE /v1/files/uploads/{upload_id}", "files.abort_upload"),
    ])
}

/// Compile-time proof that every covered method exists with these names. A
/// removed or renamed SDK method breaks this function's body.
#[allow(dead_code, clippy::diverging_sub_expression)]
async fn method_exists(client: hot_dev::HotClient) -> hot_dev::Result<()> {
    use hot_dev::{
        BuildUpload, CallOptions, SubscribeWithEventOptions, TaskWaitOptions, WaitOptions,
    };
    let body = serde_json::Map::new();
    let q: &[(&str, &str)] = &[];

    let orgs = client.orgs();
    let _ = orgs.list(q).await?;
    let _ = orgs.create(body.clone()).await?;
    let _ = orgs.get("o").await?;
    let _ = orgs.update("o", body.clone()).await?;
    let _ = orgs.usage("o").await?;
    let _ = orgs.purge_calls("o", true).await?;
    let _ = orgs.list_envs("o").await?;
    let _ = orgs.create_env("o", body.clone()).await?;
    let _ = orgs.get_env("o", "e").await?;
    let _ = orgs.list_api_keys("o").await?;
    let _ = orgs.create_api_key("o", body.clone()).await?;
    let _ = orgs.list_env_api_keys("o", "e", q).await?;
    let _ = orgs.create_env_api_key("o", "e", body.clone()).await?;
    let _ = orgs.env("o", "e").projects().list(q).await?;
    let _ = orgs.env("o", "e").builds().list(q).await?;
    let _ = client.api_keys().get("k").await?;
    let _ = client.api_keys().update("k", body.clone()).await?;
    client.api_keys().revoke("k").await?;
    let _ = client.api_keys().rotate("k", None).await?;
    let _ = client.env().purge_calls().await?;
    let _ = client.runs().purge_calls("r").await?;
    let _ = client.streams().purge_calls("s").await?;

    let _ = client.projects().list(q).await?;
    let _ = client.projects().create(body.clone()).await?;
    let _ = client.projects().get("p").await?;
    let _ = client.projects().update("p", body.clone()).await?;
    client.projects().delete("p").await?;
    let _ = client.projects().activate("p").await?;
    let _ = client.projects().deactivate("p").await?;
    let _ = client.projects().event_handlers("p").await?;
    let _ = client.projects().schedules("p").await?;

    let _ = client.builds().list(q).await?;
    let _ = client.builds().list_for_project("p", q).await?;
    let _ = client
        .builds()
        .upload(
            "p",
            BuildUpload {
                content: vec![],
                filename: None,
                hash: String::new(),
                build_id: None,
            },
        )
        .await?;
    let _ = client.builds().deployed("p").await?;
    let _ = client.builds().live("p").await?;
    let _ = client.builds().get("p", "b").await?;
    let _ = client.builds().download("p", "b").await?;
    let _ = client.builds().deploy("p", "b").await?;

    let _ = client.context().list("p").await?;
    let _ = client.context().create("p", body.clone()).await?;
    let _ = client.context().update("p", "k", body.clone()).await?;
    client.context().delete("p", "k").await?;

    let _ = client.runs().list(q).await?;
    let _ = client.runs().stats().await?;
    let _ = client.runs().get("r").await?;

    let _ = client.events().publish(body.clone()).await?;
    let _ = client
        .events()
        .call_hot("f", vec![], CallOptions::default())
        .await?;
    let _ = client.events().list(q).await?;
    let _ = client.events().get("e").await?;
    let _ = client.events().get_runs("e").await?;

    let _ = client.sessions().create(body.clone()).await?;
    let _ = client.sessions().list(q).await?;
    let _ = client.sessions().revoke_all().await?;
    client.sessions().revoke("s").await?;

    let _ = client.service_keys().create(body.clone()).await?;
    let _ = client.service_keys().list(q).await?;
    let _ = client.service_keys().revoke_all().await?;
    let _ = client.service_keys().get("k").await?;
    let _ = client.service_keys().update("k", body.clone()).await?;
    client.service_keys().revoke("k").await?;

    let _ = client.domains().create(body.clone()).await?;
    let _ = client.domains().list().await?;
    let _ = client.domains().get("d").await?;
    client.domains().delete("d").await?;
    let _ = client.domains().verify("d").await?;

    let _ = client.env().get().await?;
    let _ = client.env().subscribe();
    let _ = client.org().usage().await?;

    let _ = client.runs().subscribe("r");
    let _ = client
        .runs()
        .wait("r", hot_dev::RunWaitOptions::default())
        .await?;

    let _ = client.streams().subscribe("s", None);
    let _ = client.streams().subscribe_post("s", None);
    let _ = client
        .streams()
        .subscribe_with_event(body.clone(), SubscribeWithEventOptions::default());
    let _ = client
        .streams()
        .wait_for_run_result("s", "e", WaitOptions::default())
        .await?;
    let _ = client.tasks().get("t").await?;
    let _ = client.tasks().subscribe("t");
    let _ = client.tasks().wait("t", TaskWaitOptions::default()).await?;

    let _ = client.files().list(q).await?;
    let _ = client.files().get("f").await?;
    client.files().delete("f").await?;
    let _ = client.files().download("f").await?;
    let _ = client.files().upload("path", vec![], None).await?;
    let _ = client.files().initiate_upload(body.clone()).await?;
    let _ = client.files().upload_part("u", 1, vec![]).await?;
    let _ = client.files().complete_upload("u").await?;
    client.files().abort_upload("u").await?;

    let _ = client.blobs().download("b").await?;

    Ok(())
}

#[test]
fn openapi_operations_have_sdk_coverage_or_explicit_ignore() {
    let fixture = include_str!("fixtures/openapi-operations.json");
    let operations: Vec<Value> = serde_json::from_str(fixture).unwrap();
    let operation_keys: BTreeSet<String> = operations
        .iter()
        .map(|operation| {
            format!(
                "{} {}",
                operation["method"].as_str().unwrap(),
                operation["path"].as_str().unwrap()
            )
        })
        .collect();
    let coverage_keys: BTreeSet<String> = coverage().keys().map(|key| key.to_string()).collect();

    let missing: Vec<_> = operation_keys.difference(&coverage_keys).collect();
    let stale: Vec<_> = coverage_keys.difference(&operation_keys).collect();
    assert!(
        missing.is_empty(),
        "operations without coverage: {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "coverage for unknown operations: {stale:?}"
    );
}
