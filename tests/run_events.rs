//! How the run-result helpers tie runs to the published event: retries of a
//! `pending_retry` failure, and stream:data that arrives before run:start.

use futures_util::StreamExt;
use hot_dev::{CallOptions, Error, HotClient, StreamEventExt, SubscribeWithEventOptions};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PUBLISHED: &str = r#"{"type":"event:published","stream_id":"s1","event_id":"e1"}"#;
// The first attempt fails and is retried: the retry run gets a new event id
// and names the failed run as its origin.
const PENDING_RETRY: &str = r#"{"type":"run:fail","run":{"run_id":"a","event_id":"e1","status":"pending_retry","retry_attempt":1,"result":"first attempt"}}"#;
const RETRY_STOPS: &str = r#"{"type":"run:stop","run":{"run_id":"b","event_id":"e2","origin_run_id":"a","status":"succeeded","retry_attempt":1,"result":{"$ok":"recovered"}}}"#;

fn sse_response(events: &[&str]) -> ResponseTemplate {
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")
}

async fn server_with(route: &str, verb: &str, events: &[&str]) -> (MockServer, HotClient) {
    let server = MockServer::start().await;
    Mock::given(method(verb))
        .and(path(route))
        .respond_with(sse_response(events))
        .mount(&server)
        .await;
    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    (server, client)
}

#[tokio::test]
async fn wait_for_run_result_follows_a_pending_retry() {
    for events in [
        [PENDING_RETRY, RETRY_STOPS],
        // The retry's result can arrive before the failure that caused it.
        [RETRY_STOPS, PENDING_RETRY],
    ] {
        let (_server, client) = server_with("/v1/streams/s1/subscribe", "GET", &events).await;
        let run = client
            .streams()
            .wait_for_run_result("s1", "e1", Default::default())
            .await
            .unwrap();
        assert_eq!(run["run_id"], "b");
    }
}

#[tokio::test]
async fn call_hot_follows_a_pending_retry() {
    let (_server, client) = server_with(
        "/v1/streams/subscribe-with-event",
        "POST",
        &[PUBLISHED, PENDING_RETRY, RETRY_STOPS],
    )
    .await;
    let result = client
        .events()
        .call_hot("f", vec![], CallOptions::default())
        .await
        .unwrap();
    assert_eq!(result, json!("recovered"));
}

#[tokio::test]
async fn a_failure_that_is_not_retried_still_fails() {
    let (_server, client) = server_with(
        "/v1/streams/s1/subscribe",
        "GET",
        &[r#"{"type":"run:fail","run":{"run_id":"a","event_id":"e1","status":"failed","result":"boom"}}"#],
    )
    .await;
    let error = client
        .streams()
        .wait_for_run_result("s1", "e1", Default::default())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::RunFailed(message) if message == "boom"),
        "{error:?}"
    );
}

#[tokio::test]
async fn subscribe_with_event_continues_past_a_pending_retry() {
    let (_server, client) = server_with(
        "/v1/streams/subscribe-with-event",
        "POST",
        &[
            PUBLISHED,
            PENDING_RETRY,
            RETRY_STOPS,
            r#"{"type":"run:start","run":{"run_id":"late"}}"#,
        ],
    )
    .await;
    let mut stream = client
        .streams()
        .subscribe_with_event(json!({}), SubscribeWithEventOptions::default());
    let mut types = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event.unwrap();
        types.push(format!(
            "{} {}",
            event.event_type(),
            event.run_id().unwrap_or("")
        ));
    }
    assert_eq!(types, ["event:published ", "run:fail a", "run:stop b"]);
}

#[tokio::test]
async fn chunks_are_delivered_in_either_order() {
    let early = [
        PUBLISHED,
        r#"{"type":"stream:data","run_id":"r1","payload":{"text":"a"}}"#,
        r#"{"type":"stream:data","run_id":"r2","payload":{"text":"other run"}}"#,
        r#"{"type":"stream:data","run_id":"r1","payload":{"text":"b"}}"#,
        r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":{"$ok":"done"}}}"#,
        r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
    ];
    let documented = [
        PUBLISHED,
        r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
        r#"{"type":"stream:data","run_id":"r1","payload":{"text":"a"}}"#,
        r#"{"type":"stream:data","run_id":"r1","payload":{"text":"b"}}"#,
        r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":{"$ok":"done"}}}"#,
    ];
    for events in [&early[..], &documented[..]] {
        let (_server, client) =
            server_with("/v1/streams/subscribe-with-event", "POST", events).await;
        let mut chunks = Vec::new();
        let mut on_chunk = |text: &str| chunks.push(text.to_string());
        let result = client
            .events()
            .call_hot(
                "f",
                vec![],
                CallOptions {
                    on_chunk: Some(&mut on_chunk),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result, json!("done"));
        assert_eq!(chunks, ["a", "b"]);
    }
}

#[tokio::test]
async fn retry_chunks_are_delivered() {
    let (_server, client) = server_with(
        "/v1/streams/s1/subscribe",
        "GET",
        &[
            r#"{"type":"stream:data","run_id":"b","payload":{"text":"from the retry"}}"#,
            PENDING_RETRY,
            RETRY_STOPS,
        ],
    )
    .await;
    let mut chunks = Vec::new();
    let mut on_chunk = |text: &str| chunks.push(text.to_string());
    client
        .streams()
        .wait_for_run_result(
            "s1",
            "e1",
            hot_dev::WaitOptions {
                on_chunk: Some(&mut on_chunk),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(chunks, ["from the retry"]);
}

// How the server's database poll replays a retried first attempt: as run:stop
// while it is pending_retry, and as a plain failure once the retry is queued.
const REPLAYED_PENDING: &str = r#"{"type":"run:stop","run":{"run_id":"a","event_id":"e1","status":"pending_retry","retry_attempt":1,"result":"first attempt"}}"#;
const REPLAYED_FAILED: &str = r#"{"type":"run:fail","run":{"run_id":"a","event_id":"e1","status":"failed","retry_attempt":1,"result":"first attempt"}}"#;
const RETRY_STARTS: &str = r#"{"type":"run:start","run":{"run_id":"b","event_id":"e2","origin_run_id":"a","status":"running","retry_attempt":1}}"#;
const RETRY_SUCCEEDS: &str = r#"{"type":"run:stop","run":{"run_id":"b","event_id":"e2","origin_run_id":"a","status":"succeeded","retry_attempt":1,"result":"recovered"}}"#;

async fn wait_result(events: &[&str]) -> (Result<hot_dev::JsonObject, Error>, std::time::Duration) {
    let (_server, client) = server_with("/v1/streams/s1/subscribe", "GET", events).await;
    let started = std::time::Instant::now();
    let result = client
        .streams()
        .wait_for_run_result(
            "s1",
            "e1",
            hot_dev::WaitOptions {
                timeout: std::time::Duration::from_secs(20),
                ..Default::default()
            },
        )
        .await;
    (result, started.elapsed())
}

#[tokio::test]
async fn replayed_retries_are_followed() {
    for events in [
        // Replayed while pending_retry: a run:stop that is not the result.
        &[REPLAYED_PENDING, RETRY_STARTS, RETRY_SUCCEEDS][..],
        // Replayed after the retry was queued, before the retry run exists.
        &[REPLAYED_FAILED, RETRY_STARTS, RETRY_SUCCEEDS][..],
        // The poll lists newest runs first.
        &[
            RETRY_STARTS,
            RETRY_SUCCEEDS,
            r#"{"type":"run:start","run":{"run_id":"a","event_id":"e1","status":"failed","retry_attempt":1}}"#,
            REPLAYED_FAILED,
        ][..],
        // The retry is running when the failed attempt is replayed.
        &[
            r#"{"type":"run:start","run":{"run_id":"a","event_id":"e1","status":"failed","retry_attempt":1}}"#,
            RETRY_STARTS,
            REPLAYED_FAILED,
            RETRY_SUCCEEDS,
        ][..],
    ] {
        let (result, elapsed) = wait_result(events).await;
        let run = result.unwrap();
        assert_eq!(run["run_id"], "b");
        assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
    }
}

#[tokio::test]
async fn final_failures_are_reported_without_waiting() {
    for events in [
        // No retry configured.
        &[
            r#"{"type":"run:fail","run":{"run_id":"a","event_id":"e1","status":"failed","retry_attempt":0,"result":"boom"}}"#,
        ][..],
        // The last retry fails.
        &[
            PENDING_RETRY,
            r#"{"type":"run:fail","run":{"run_id":"b","event_id":"e2","origin_run_id":"a","status":"failed","retry_attempt":1,"result":"boom"}}"#,
        ][..],
    ] {
        let (_server, client) = server_with("/v1/streams/s1/subscribe", "GET", events).await;
        let started = std::time::Instant::now();
        let error = client
            .streams()
            .wait_for_run_result("s1", "e1", Default::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::RunFailed(message) if message == "boom"),
            "{error:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}

#[tokio::test]
async fn a_scheduled_retry_that_never_appears_is_reported_as_the_failure() {
    let (result, elapsed) = wait_result(&[REPLAYED_FAILED]).await;
    let error = result.unwrap_err();
    assert!(
        matches!(&error, Error::RunFailed(message) if message == "first attempt"),
        "{error:?}"
    );
    assert!(elapsed >= std::time::Duration::from_secs(5), "{elapsed:?}");
    assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
}

#[tokio::test]
async fn subscribe_with_event_follows_replayed_retries() {
    let (_server, client) = server_with(
        "/v1/streams/subscribe-with-event",
        "POST",
        &[PUBLISHED, REPLAYED_FAILED, RETRY_STARTS, RETRY_SUCCEEDS],
    )
    .await;
    let mut stream = client
        .streams()
        .subscribe_with_event(json!({}), SubscribeWithEventOptions::default());
    let mut last = None;
    while let Some(event) = stream.next().await {
        last = Some(event.unwrap());
    }
    assert_eq!(last.unwrap().run_id(), Some("b"));
}

// A task started by our run names it as origin, on the same stream, but is
// not a retry: its run_type is "task".
const OUR_START: &str = r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"running"}}"#;
const OUR_STOP: &str = r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"succeeded","result":{"$ok":"ours"}}}"#;
const TASK_START: &str = r#"{"type":"run:start","run":{"run_id":"t1","event_id":"e9","origin_run_id":"r1","run_type":"task","status":"running"}}"#;
const TASK_STOP: &str = r#"{"type":"run:stop","run":{"run_id":"t1","event_id":"e9","origin_run_id":"r1","run_type":"task","status":"succeeded","result":{"$ok":"task result"}}}"#;
const TASK_FAIL: &str = r#"{"type":"run:fail","run":{"run_id":"t1","event_id":"e9","origin_run_id":"r1","run_type":"task","status":"failed","result":"task failed"}}"#;
const TASK_CHUNK: &str =
    r#"{"type":"stream:data","run_id":"t1","payload":{"text":"from the task"}}"#;

#[tokio::test]
async fn task_runs_started_by_our_run_do_not_end_or_alter_the_result() {
    for events in [
        &[
            PUBLISHED, OUR_START, TASK_START, TASK_CHUNK, TASK_STOP, OUR_STOP,
        ][..],
        // The task finishes before our run is known.
        &[PUBLISHED, TASK_CHUNK, TASK_STOP, OUR_START, OUR_STOP][..],
        &[PUBLISHED, OUR_START, TASK_FAIL, OUR_STOP][..],
    ] {
        let (_server, client) =
            server_with("/v1/streams/subscribe-with-event", "POST", events).await;
        let mut chunks = Vec::new();
        let mut on_chunk = |text: &str| chunks.push(text.to_string());
        let result = client
            .events()
            .call_hot(
                "f",
                vec![],
                CallOptions {
                    on_chunk: Some(&mut on_chunk),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result, json!("ours"));
        assert!(chunks.is_empty(), "{chunks:?}");

        let (_server, client) = server_with("/v1/streams/s1/subscribe", "GET", &events[1..]).await;
        let run = client
            .streams()
            .wait_for_run_result("s1", "e1", Default::default())
            .await
            .unwrap();
        assert_eq!(run["run_id"], "r1");
    }
}

#[tokio::test]
async fn our_failure_still_fails_at_once_when_it_started_a_task() {
    let our_fail = r#"{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"failed","retry_attempt":0,"result":"boom"}}"#;
    for events in [
        &[PUBLISHED, OUR_START, TASK_START, TASK_STOP, our_fail][..],
        &[PUBLISHED, TASK_STOP, OUR_START, our_fail][..],
    ] {
        let (_server, client) =
            server_with("/v1/streams/subscribe-with-event", "POST", events).await;
        let started = std::time::Instant::now();
        let error = client
            .events()
            .call_hot("f", vec![], CallOptions::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::RunFailed(message) if message == "boom"),
            "{error:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}

#[tokio::test]
async fn subscribe_with_event_is_not_ended_by_a_task_run() {
    let (_server, client) = server_with(
        "/v1/streams/subscribe-with-event",
        "POST",
        &[PUBLISHED, OUR_START, TASK_START, TASK_STOP, OUR_STOP],
    )
    .await;
    let mut stream = client
        .streams()
        .subscribe_with_event(json!({}), SubscribeWithEventOptions::default());
    let mut last = None;
    while let Some(event) = stream.next().await {
        last = Some(event.unwrap());
    }
    assert_eq!(last.unwrap().run_id(), Some("r1"));
}
