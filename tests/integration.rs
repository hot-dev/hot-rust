//! Integration tests against a live `hot dev` instance serving the fixture
//! project in integration/fixture. Start one and run this file with
//! integration/run.sh — the tests are `#[ignore]`d so plain `cargo test`
//! skips them; the runner passes `-- --ignored`.

use futures_util::StreamExt;
use hot_dev::{
    CallOptions, Error, HotClient, RunWaitOptions, StreamEventExt, SubscribeWithEventOptions,
    TaskWaitOptions,
};
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
async fn wait_for_task_started_by_event_handler() {
    let client = client();
    let mut events = client.streams().subscribe_with_event(
        json!({
            "event_type": "sdk:task",
            "event_data": { "value": "hello" },
        }),
        SubscribeWithEventOptions::default(),
    );

    let mut task_id = None;
    let mut handler_run_id = None;
    let mut stream_id = None;
    while let Some(event) = events.next().await {
        let event = event.unwrap();
        match event.event_type() {
            "run:fail" | "run:cancel" => panic!("run did not complete: {event:?}"),
            "run:stop" => {
                handler_run_id = event.run_id().map(str::to_string);
                stream_id = event
                    .run()
                    .and_then(|run| run.get("stream_id"))
                    .and_then(|id| id.as_str())
                    .map(str::to_string);
                task_id = event
                    .run()
                    .and_then(|run| run.get("result"))
                    .and_then(|result| result.get("work"))
                    .and_then(|work| work.get("task"))
                    .and_then(|id| id.as_str())
                    .map(str::to_string);
                break;
            }
            _ => {}
        }
    }

    let task_id = task_id.expect("run result did not contain work.task");
    let handler_run = client
        .runs()
        .wait(
            &handler_run_id.expect("run:stop did not include run_id"),
            RunWaitOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(handler_run["status"], "succeeded");

    let mut stream = client.streams().subscribe(
        &stream_id.expect("run:stop did not include stream_id"),
        None,
    );
    let streamed_task = loop {
        let event = stream
            .next()
            .await
            .expect("stream ended before task completed")
            .unwrap();
        if event.event_type() != "task:update" {
            continue;
        }
        let Some(task) = event.get("task").and_then(|task| task.as_object()) else {
            continue;
        };
        if task.get("task_id").and_then(|id| id.as_str()) != Some(task_id.as_str()) {
            continue;
        }
        match task.get("status").and_then(|status| status.as_str()) {
            Some("completed") => break task.clone(),
            Some("failed" | "cancelled" | "timed_out") => {
                panic!("task did not complete: {task:?}")
            }
            _ => {}
        }
    };
    assert_eq!(streamed_task["result"]["processed"], "hello");

    let task = client
        .tasks()
        .wait(&task_id, TaskWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(task["status"], "completed");
    assert_eq!(task["result"]["processed"], "hello");
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
