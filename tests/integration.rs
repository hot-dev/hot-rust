//! Integration tests against a live `hot dev` instance serving the fixture
//! project in integration/fixture. Start one and run this file with
//! integration/run.sh — the tests are `#[ignore]`d so plain `cargo test`
//! skips them; the runner passes `-- --ignored`.

use futures_util::StreamExt;
use hot_dev::{CallOptions, Error, HotClient, StreamEventExt, SubscribeWithEventOptions};
use serde_json::json;

fn client() -> HotClient {
    let key = std::env::var("HOT_TEST_API_KEY")
        .expect("HOT_TEST_API_KEY not set; run integration/run.sh");
    let base_url =
        std::env::var("HOT_TEST_BASE_URL").unwrap_or_else(|_| "http://localhost:4681".to_string());
    HotClient::builder(key).base_url(base_url).build()
}

#[tokio::test]
#[ignore = "requires a running hot dev (integration/run.sh)"]
async fn env_and_projects() {
    let client = client();

    let env = client.env().get().await.unwrap();
    assert_eq!(env["name"], "development");

    let projects = client.projects().list(&[]).await.unwrap();
    let names: Vec<&str> = projects["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|project| project["name"].as_str())
        .collect();
    assert!(names.contains(&"sdk-fixture"), "projects = {names:?}");
}

#[tokio::test]
#[ignore = "requires a running hot dev (integration/run.sh)"]
async fn subscribe_with_event_echo() {
    let client = client();
    let mut events = client.streams().subscribe_with_event(
        json!({
            "event_type": "sdk:echo",
            "event_data": { "text": "hello" },
        }),
        SubscribeWithEventOptions::default(),
    );

    let mut result = None;
    while let Some(event) = events.next().await {
        let event = event.unwrap();
        match event.event_type() {
            "run:fail" | "run:cancel" => panic!("run did not complete: {event:?}"),
            "run:stop" => {
                result = event.run().and_then(|run| run.get("result")).cloned();
                break;
            }
            _ => {}
        }
    }

    let result = result.expect("no run:stop observed");
    assert_eq!(result["echoed"]["text"], "hello", "result = {result:?}");
}

#[tokio::test]
#[ignore = "requires a running hot dev (integration/run.sh)"]
async fn call_hot_add_nums() {
    let client = client();
    let result = client
        .events()
        .call_hot(
            "::fixture::sdk/add-nums",
            vec![json!(2), json!(3)],
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!(5));
}

#[tokio::test]
#[ignore = "requires a running hot dev (integration/run.sh)"]
async fn run_failure_message() {
    let client = client();
    let published = client
        .events()
        .publish(json!({ "event_type": "sdk:fail", "event_data": {} }))
        .await
        .unwrap();

    let stream_id = published["stream_id"].as_str().unwrap();
    let event_id = published["event_id"].as_str().unwrap();
    let error = client
        .streams()
        .wait_for_run_result(stream_id, event_id, Default::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&error, Error::RunFailed(message) if message.contains("sdk fixture failure")),
        "{error:?}"
    );
}
