//! Run with integration/suborg_fixture.py; owns a disposable DB and restricted PG runtime.
//! The fixture passes an org API key (`org_token`), a root environment key
//! (`root_token`), and credentials issued under a delegated org key that it
//! then revoked or narrowed.
use futures_util::StreamExt;
use hot_dev::{Error, HotClient, JsonObject, KeyScope};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn fixture() -> Option<Value> {
    let data = std::env::var("HOT_TEST_SUBORG_FIXTURE").ok()?;
    Some(serde_json::from_str(&data).unwrap())
}
fn client(token: &str) -> HotClient {
    HotClient::builder(token)
        .base_url(std::env::var("HOT_TEST_BASE_URL").unwrap())
        .timeout(Duration::from_secs(10))
        .build()
}
fn denied<T: std::fmt::Debug>(result: hot_dev::Result<T>, statuses: &[u16]) {
    match result {
        Err(Error::Api(e)) if statuses.contains(&e.status_code) => (),
        other => panic!("expected API denial, got {other:?}"),
    }
}
fn text<'a>(object: &'a JsonObject, key: &str) -> &'a str {
    object[key].as_str().unwrap()
}
/// A unique slug within the server's 32-character limit.
fn slug(i: usize) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sdk-rs-{}-{i}", nanos % 1_000_000_000_000)
}

#[tokio::test]
#[ignore = "requires disposable sub-org fixture"]
async fn suborg_isolation() {
    let conf = fixture().expect("run integration/suborg_fixture.py");
    let org = client(conf["org_token"].as_str().unwrap());
    let orgs = org.orgs();
    let mut children = Vec::new();
    let mut envs = Vec::new();
    let mut keys = Vec::new();
    let mut clients = Vec::new();
    for i in 0..2 {
        let identity = slug(i);
        let body = json!({"name":"SDK Customer","slug":identity,"external_id":identity,"idempotency_key":identity});
        let child = orgs.create(&body).await.unwrap();
        assert_eq!(child["created"], true);
        let child_id = child["org"]["org_id"].as_str().unwrap().to_string();
        children.push(child_id.clone());
        let replay = orgs.create(&body).await.unwrap();
        assert_eq!(replay["created"], false);
        assert_eq!(replay["org"]["org_id"], child_id);
        assert_eq!(
            orgs.get(&child_id).await.unwrap()["slug"],
            identity.as_str()
        );
        let env = orgs
            .create_env(&child_id, json!({"name":"production"}))
            .await
            .unwrap();
        let again = orgs
            .create_env(&child_id, json!({"name":"production"}))
            .await
            .unwrap();
        assert_eq!(again["env_id"], env["env_id"]);
        let env_id = text(&env, "env_id").to_string();
        assert_eq!(
            orgs.get_env(&child_id, &env_id).await.unwrap()["name"],
            "production"
        );
        let listed = orgs.list_envs(&child_id).await.unwrap();
        assert_eq!(
            listed["data"].as_array().unwrap().len(),
            2,
            "default and production"
        );
        envs.push(env_id.clone());
        let key = orgs
            .create_env_api_key(
                &child_id,
                &env_id,
                json!({"description":"sdk","permissions":{"*:*": ["*"]}}),
            )
            .await
            .unwrap();
        assert_eq!(key["scope"], "env");
        let runtime = client(text(&key, "token"));
        assert_eq!(runtime.env().get().await.unwrap()["org_id"], child_id);
        let usage = runtime.org().usage().await.unwrap();
        assert_eq!(usage["env_id"], env_id);
        assert_eq!(usage["usage_scope"], "environment");
        assert_eq!(usage["limits_scope"], "billing_account");
        let env_keys = orgs
            .list_env_api_keys(&child_id, &env_id, &[("limit", "500")])
            .await
            .unwrap();
        assert!(env_keys["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|listed| listed["api_key_id"] == key["api_key_id"]));
        keys.push(key);
        clients.push(runtime);
    }
    let a = &clients[0];
    let b = &clients[1];
    let pa = a
        .projects()
        .create(json!({"name":"shared-name"}))
        .await
        .unwrap();
    let pb = b
        .projects()
        .create(json!({"name":"shared-name"}))
        .await
        .unwrap();
    assert_ne!(pa["project_id"], pb["project_id"]);
    assert_eq!(
        a.projects().get("shared-name").await.unwrap()["project_id"],
        pa["project_id"]
    );
    assert_eq!(
        b.projects().get("shared-name").await.unwrap()["project_id"],
        pb["project_id"]
    );
    denied(b.projects().get(text(&pa, "project_id")).await, &[404]);
    denied(
        client(conf["root_token"].as_str().unwrap())
            .projects()
            .get(text(&pa, "project_id"))
            .await,
        &[404],
    );
    let body = json!({"event_type":"sdk:isolation","event_data":{"owner":"a"}});
    let event = a.events().publish(&body).await.unwrap();
    let event_id = text(&event, "event_id");
    assert_eq!(
        a.events().get(event_id).await.unwrap()["event_data"],
        json!({"owner":"a"})
    );
    denied(b.events().get(event_id).await, &[404]);
    let mut stream = b.streams().subscribe(text(&event, "stream_id"), None);
    denied(stream.next().await.unwrap(), &[403, 404]);

    // One client type, two key kinds: the wrong kind gets a typed scope error.
    let error = a.orgs().list(&[]).await.unwrap_err();
    assert_eq!(error.required_scope(), Some(KeyScope::Org), "{error:?}");
    let error = org.env().get().await.unwrap_err();
    assert_eq!(error.required_scope(), Some(KeyScope::Env), "{error:?}");

    let listed = orgs.list(&[("limit", "500")]).await.unwrap();
    assert!(listed["pagination"]["total"].as_u64().unwrap() >= 2);
    let root = conf["root_org_id"].as_str().unwrap();
    let usage = orgs.usage(root).await.unwrap();
    assert_eq!(usage["org_id"], root);
    assert_eq!(usage["usage_scope"], "account");
    assert_eq!(orgs.get(root).await.unwrap()["org_id"], root);
    assert!(!orgs.list_api_keys(root).await.unwrap()["data"]
        .as_array()
        .unwrap()
        .is_empty());

    // Narrow, rotate, and revoke an environment key by ID.
    let restricted = orgs
        .create_env_api_key(
            &children[0],
            &envs[0],
            json!({"description":"event-only","permissions":{"event:sdk:*": ["create","read"]}}),
        )
        .await
        .unwrap();
    let restricted_id = text(&restricted, "api_key_id");
    let limited = client(text(&restricted, "token"));
    let published = limited.events().publish(&body).await.unwrap();
    denied(limited.projects().list(&[]).await, &[403]);
    let narrowed = org
        .api_keys()
        .update(
            restricted_id,
            json!({"permissions":{"event:sdk:*": ["create"]}}),
        )
        .await
        .unwrap();
    assert_eq!(narrowed["permissions"], json!({"event:sdk:*": ["create"]}));
    denied(
        limited.events().get(text(&published, "event_id")).await,
        &[403],
    );
    assert_eq!(
        org.api_keys().get(restricted_id).await.unwrap()["scope"],
        "env"
    );
    let rotated = org.api_keys().rotate(restricted_id, Some(0)).await.unwrap();
    denied(limited.events().publish(&body).await, &[401]);
    let limited = client(text(&rotated, "token"));
    limited.events().publish(&body).await.unwrap();
    org.api_keys().revoke(restricted_id).await.unwrap();
    denied(limited.events().publish(&body).await, &[401]);

    // Project and context routes inside a child environment, with the org key.
    let scoped = orgs.env(&children[0], &envs[0]);
    let project = scoped
        .projects()
        .create(json!({"name":"via-org-key"}))
        .await
        .unwrap();
    assert_eq!(
        a.projects().get("via-org-key").await.unwrap()["project_id"],
        project["project_id"]
    );
    assert!(scoped.projects().list(&[]).await.unwrap()["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|listed| listed["name"] == "via-org-key"));
    for i in 0..25 {
        scoped
            .context()
            .create(
                "via-org-key",
                json!({"key": format!("sdk.var{i:02}"), "value": "v"}),
            )
            .await
            .unwrap();
    }
    let first = a.context().list("via-org-key").await.unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 20);
    assert_eq!(first["pagination"]["has_more"], true);
    let all = scoped
        .context()
        .list_with_query("via-org-key", &[("limit", "100")])
        .await
        .unwrap();
    assert_eq!(all["data"].as_array().unwrap().len(), 25);
    let rest = a
        .context()
        .list_with_query("via-org-key", &[("offset", "20")])
        .await
        .unwrap();
    assert_eq!(rest["data"].as_array().unwrap().len(), 5);
    assert_eq!(rest["pagination"]["has_more"], false);
    let purged = orgs.purge_calls(&children[0], false).await.unwrap();
    assert!(purged["complete"].is_boolean(), "{purged:?}");
    a.env().purge_calls().await.unwrap();

    let permissions = json!({"event:sdk:*": ["create","read"],"stream:*": ["read"]});
    let service = a
        .service_keys()
        .create(json!({"name":"sdk-service","permissions":permissions}))
        .await
        .unwrap();
    let session = a
        .sessions()
        .create(json!({"permissions":permissions}))
        .await
        .unwrap();
    let other = b.events().publish(&body).await.unwrap();
    for credential in [&service, &session] {
        let runtime = client(text(credential, "token"));
        runtime.events().publish(&body).await.unwrap();
        denied(
            runtime.events().get(text(&other, "event_id")).await,
            &[403, 404],
        );
    }
    a.service_keys()
        .revoke(text(&service, "service_key_id"))
        .await
        .unwrap();
    a.sessions()
        .revoke(text(&session, "session_id"))
        .await
        .unwrap();
    for credential in [&service, &session] {
        denied(
            client(text(credential, "token"))
                .events()
                .publish(&body)
                .await,
            &[401],
        );
    }
    let suspended = orgs
        .update(&children[0], json!({"state":"suspended"}))
        .await
        .unwrap();
    assert_eq!(suspended["lifecycle_state"], "suspended");
    denied(a.env().get().await, &[401, 403]);
    b.env().get().await.unwrap();
    orgs.update(&children[0], json!({"state":"active"}))
        .await
        .unwrap();
    a.env().get().await.unwrap();
    for child in &children {
        orgs.update(child, json!({"state":"archived"}))
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable sub-org fixture"]
async fn suborg_issuer_chain() {
    let conf = fixture().expect("run integration/suborg_fixture.py");
    let body = json!({"event_type":"sdk:isolation","event_data":{}});
    for token in conf["revoked"].as_array().unwrap() {
        denied(
            client(token.as_str().unwrap())
                .events()
                .publish(&body)
                .await,
            &[401],
        );
    }
    for token in conf["narrowed"].as_array().unwrap() {
        let runtime = client(token.as_str().unwrap());
        runtime.events().publish(&body).await.unwrap();
        denied(runtime.projects().list(&[]).await, &[403]);
        denied(
            runtime
                .events()
                .publish(json!({"event_type":"other:blocked","event_data":{}}))
                .await,
            &[403],
        );
    }
}
