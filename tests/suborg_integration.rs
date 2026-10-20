//! Run with integration/suborg_fixture.py; owns a disposable DB and restricted PG runtime.
use futures_util::StreamExt;
use hot_dev::{Error, HotClient, HotManagementClient};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn fixture() -> Option<Value> {
    let data = std::env::var("HOT_TEST_SUBORG_FIXTURE").ok()?;
    Some(serde_json::from_str(&data).unwrap())
}
fn runtime(token: &str) -> HotClient {
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
#[tokio::test]
#[ignore = "requires disposable sub-org fixture"]
async fn suborg_isolation() {
    let conf = fixture().expect("run integration/suborg_fixture.py");
    let management = HotManagementClient::builder(conf["management_token"].as_str().unwrap())
        .base_url(std::env::var("HOT_TEST_BASE_URL").unwrap())
        .build();
    let mut children = Vec::new();
    let mut envs = Vec::new();
    let mut clients = Vec::new();
    for i in 0..2 {
        let identity = format!(
            "sdk-rust-{}-{i}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let body = json!({"name":"SDK Customer","slug":identity,"external_id":identity,"idempotency_key":identity});
        let child = management.create_child(&body).await.unwrap();
        let child_id = child["org"]["org_id"].as_str().unwrap().to_string();
        children.push(child_id.clone());
        let replay = management.create_child(&body).await.unwrap();
        assert_eq!(replay["created"], false);
        assert_eq!(replay["org"]["org_id"], child_id);
        let env = management
            .create_environment(&child_id, json!({"name":"production"}))
            .await
            .unwrap();
        assert_eq!(
            management
                .create_environment(&child_id, json!({"name":"production"}))
                .await
                .unwrap(),
            env
        );
        let env_id = env["env_id"].as_str().unwrap().to_string();
        envs.push(env_id.clone());
        let key = management
            .create_api_key(
                &child_id,
                &env_id,
                json!({"description":"sdk","permissions":{"*:*": ["*"]}}),
            )
            .await
            .unwrap();
        let client = runtime(key["token"].as_str().unwrap());
        assert_eq!(client.env().get().await.unwrap()["org_id"], child_id);
        let usage = client.org().usage().await.unwrap();
        assert_eq!(usage["env_id"], env_id);
        assert_eq!(usage["usage_scope"], "environment");
        assert_eq!(usage["limits_scope"], "billing_account");
        clients.push(client);
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
    denied(
        b.projects().get(pa["project_id"].as_str().unwrap()).await,
        &[404],
    );
    denied(
        runtime(conf["root_token"].as_str().unwrap())
            .projects()
            .get(pa["project_id"].as_str().unwrap())
            .await,
        &[404],
    );
    let body = json!({"event_type":"sdk:isolation","event_data":{"owner":"a"}});
    let event = a.events().publish(&body).await.unwrap();
    let event_id = event["event_id"].as_str().unwrap();
    assert_eq!(
        a.events().get(event_id).await.unwrap()["event_data"],
        json!({"owner":"a"})
    );
    denied(b.events().get(event_id).await, &[404]);
    let mut stream = b
        .streams()
        .subscribe(event["stream_id"].as_str().unwrap(), None);
    denied(stream.next().await.unwrap(), &[403, 404]);
    let bad = HotManagementClient::builder(conf["root_token"].as_str().unwrap())
        .base_url(std::env::var("HOT_TEST_BASE_URL").unwrap())
        .build();
    denied(bad.list_children(&[]).await, &[401]);
    denied(
        runtime(conf["management_token"].as_str().unwrap())
            .env()
            .get()
            .await,
        &[401],
    );
    assert!(
        management.list_children(&[]).await.unwrap()["total"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert_eq!(
        management.usage().await.unwrap()["root_org_id"],
        conf["root_org_id"]
    );
    let restricted = management
        .create_api_key(
            &children[0],
            &envs[0],
            json!({"description":"event-only","permissions":{"event:sdk:*": ["create","read"]}}),
        )
        .await
        .unwrap();
    let limited = runtime(restricted["token"].as_str().unwrap());
    limited.events().publish(&body).await.unwrap();
    denied(limited.projects().list(&[]).await, &[403]);
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
        let client = runtime(credential["token"].as_str().unwrap());
        client.events().publish(&body).await.unwrap();
        denied(
            client
                .events()
                .get(other["event_id"].as_str().unwrap())
                .await,
            &[403, 404],
        );
    }
    a.service_keys()
        .revoke(service["service_key_id"].as_str().unwrap())
        .await
        .unwrap();
    a.sessions()
        .revoke(session["session_id"].as_str().unwrap())
        .await
        .unwrap();
    for credential in [&service, &session] {
        denied(
            runtime(credential["token"].as_str().unwrap())
                .events()
                .publish(&body)
                .await,
            &[401],
        );
    }
    management
        .set_child_state(&children[0], "suspended")
        .await
        .unwrap();
    denied(a.env().get().await, &[401, 403]);
    b.env().get().await.unwrap();
    management
        .set_child_state(&children[0], "active")
        .await
        .unwrap();
    a.env().get().await.unwrap();
    for child in &children {
        management.set_child_state(child, "archived").await.unwrap();
    }
}
#[tokio::test]
#[ignore = "requires disposable sub-org fixture"]
async fn suborg_issuer_chain() {
    let conf = fixture().expect("run integration/suborg_fixture.py");
    let body = json!({"event_type":"sdk:isolation","event_data":{}});
    for token in conf["revoked"].as_array().unwrap() {
        denied(
            runtime(token.as_str().unwrap())
                .events()
                .publish(&body)
                .await,
            &[401],
        );
    }
    for token in conf["narrowed"].as_array().unwrap() {
        let client = runtime(token.as_str().unwrap());
        client.events().publish(&body).await.unwrap();
        denied(client.projects().list(&[]).await, &[403]);
        denied(
            client
                .events()
                .publish(json!({"event_type":"other:blocked","event_data":{}}))
                .await,
            &[403],
        );
    }
}
