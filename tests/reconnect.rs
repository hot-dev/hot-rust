use std::time::{Duration, Instant};

use futures_util::StreamExt;
use hot_dev::{Error, HotClient, StreamEventExt, SubscribeWithEventOptions};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PUBLISHED: &str =
    r#"{"type":"event:published","stream_id":"s1","event_id":"e1","event_type":"x"}"#;

fn sse_response(events: &[&str]) -> ResponseTemplate {
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")
}

async fn collect_events(
    server: &MockServer,
    options: SubscribeWithEventOptions,
) -> Result<Vec<hot_dev::StreamEvent>, Error> {
    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let mut stream = client
        .streams()
        .subscribe_with_event(json!({ "event_type": "x", "event_data": {} }), options);

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event?);
    }
    Ok(events)
}

fn event_types(events: &[hot_dev::StreamEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| event.event_type().to_string())
        .collect()
}

#[tokio::test]
async fn subscribe_with_event_reconnects_via_stream_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            PUBLISHED,
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
        ]))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(sse_response(&[
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
            r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}"#,
        ]))
        .expect(1)
        .mount(&server)
        .await;

    let events = collect_events(&server, SubscribeWithEventOptions::default())
        .await
        .unwrap();

    assert_eq!(
        event_types(&events),
        vec!["event:published", "run:start", "run:stop"]
    );
}

#[tokio::test]
async fn subscribe_with_event_no_reconnect_returns_after_first_connection() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            PUBLISHED,
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
        ]))
        .expect(1)
        .mount(&server)
        .await;

    let options = SubscribeWithEventOptions {
        reconnect: false,
        ..Default::default()
    };
    let events = collect_events(&server, options).await.unwrap();

    assert_eq!(event_types(&events), vec!["event:published", "run:start"]);
}

#[tokio::test]
async fn subscribe_with_event_dedupes_run_events_by_run_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            PUBLISHED,
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(sse_response(&[
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
            r#"{"type":"run:start","run":{"run_id":"r2","event_id":"e2"}}"#,
            r#"{"type":"run:fail","run":{"run_id":"r2","event_id":"e2","result":"other"}}"#,
            r#"{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","result":"err"}}"#,
            r#"{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","result":"err"}}"#,
        ]))
        .mount(&server)
        .await;

    let events = collect_events(&server, SubscribeWithEventOptions::default())
        .await
        .unwrap();

    let start_ids: Vec<_> = events
        .iter()
        .filter(|event| event.event_type() == "run:start")
        .filter_map(|event| event.run_id().map(str::to_string))
        .collect();
    let fail_ids: Vec<_> = events
        .iter()
        .filter(|event| event.event_type() == "run:fail")
        .filter_map(|event| event.run_id().map(str::to_string))
        .collect();

    assert_eq!(start_ids, vec!["r1", "r2"]);
    assert_eq!(fail_ids, vec!["r2", "r1"]);
}

#[tokio::test]
async fn subscribe_with_event_errors_when_no_event_published() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("", "text/event-stream"))
        .mount(&server)
        .await;

    let error = collect_events(&server, SubscribeWithEventOptions::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&error, Error::Protocol(message) if message.contains("event:published")),
        "{error:?}"
    );
}

#[tokio::test]
async fn subscribe_post_uses_post_stream_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(sse_response(&[
            r#"{"type":"stream:complete","stream_id":"s1"}"#,
        ]))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let mut stream = client.streams().subscribe_post("s1", None);

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type(), "stream:complete");
}

const COMPLETE: &str = r#"{"type":"stream:complete","stream_id":"s1"}"#;

fn client_for(server: &MockServer) -> HotClient {
    HotClient::builder("test-token")
        .base_url(server.uri())
        .build()
}

fn error_response(status: u16) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .set_body_json(json!({ "error": { "code": "x", "message": format!("status {status}") } }))
}

async fn get_requests(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.method.as_str() == "GET")
        .count()
}

fn is_status(result: &Result<impl std::fmt::Debug, Error>, status: u16) -> bool {
    matches!(result, Err(Error::Api(error)) if error.status_code == status)
}

#[tokio::test]
async fn permanent_errors_stop_every_waiter_after_one_request() {
    for status in [400, 401, 403, 404, 410, 422] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/streams/subscribe-with-event"))
            .respond_with(sse_response(&[PUBLISHED]))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(error_response(status))
            .mount(&server)
            .await;
        let client = client_for(&server);

        let mut events = client
            .streams()
            .subscribe_with_event(json!({}), SubscribeWithEventOptions::default());
        events.next().await.unwrap().unwrap();
        let error = events.next().await.unwrap();
        assert!(is_status(&error, status), "{status}: {error:?}");
        assert!(is_status(
            &client
                .tasks()
                .wait("t", hot_dev::TaskWaitOptions::default())
                .await,
            status
        ));
        assert!(is_status(
            &client
                .runs()
                .wait("r", hot_dev::RunWaitOptions::default())
                .await,
            status
        ));
        assert!(is_status(
            &client
                .streams()
                .wait_for_run_result("s1", "e1", hot_dev::WaitOptions::default())
                .await,
            status
        ));
        assert_eq!(get_requests(&server).await, 4, "{status}");
    }
}

#[tokio::test]
async fn transient_errors_back_off_then_recover() {
    for status in [500, 502, 503, 504, 408] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/runs/r1/subscribe"))
            .respond_with(error_response(status))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/runs/r1/subscribe"))
            .respond_with(sse_response(&[
                r#"{"type":"run:update","run":{"run_id":"r1","status":"succeeded"}}"#,
            ]))
            .mount(&server)
            .await;

        let started = Instant::now();
        let run = client_for(&server)
            .runs()
            .wait("r1", hot_dev::RunWaitOptions::default())
            .await
            .unwrap();
        assert_eq!(run["status"], "succeeded");
        // Backoff before failures 1 and 2 is at least 250 ms and 500 ms.
        assert!(started.elapsed() >= Duration::from_millis(700), "{status}");
        assert_eq!(get_requests(&server).await, 3);
    }
}

#[tokio::test]
async fn rate_limits_honour_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/tasks/t1/subscribe"))
        .respond_with(error_response(429).insert_header("Retry-After", "2"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/tasks/t1/subscribe"))
        .respond_with(sse_response(&[
            r#"{"type":"task:update","task":{"task_id":"t1","status":"completed"}}"#,
        ]))
        .mount(&server)
        .await;

    let started = Instant::now();
    let task = client_for(&server)
        .tasks()
        .wait("t1", hot_dev::TaskWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(task["status"], "completed");
    assert!(started.elapsed() >= Duration::from_secs(2));
}

/// A server whose run, task, and stream subscriptions each roll over seven
/// times (healthy 5-minute windows ending with stream:complete) before the
/// result: more rollovers than the five-reconnect budget.
async fn rollover_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[PUBLISHED, COMPLETE]))
        .mount(&server)
        .await;
    for (route, running, done) in [
        (
            "/v1/streams/s1/subscribe",
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}"#,
            r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":{"$ok":7}}}"#,
        ),
        (
            "/v1/runs/r1/subscribe",
            r#"{"type":"run:update","run":{"run_id":"r1","status":"running"}}"#,
            r#"{"type":"run:update","run":{"run_id":"r1","status":"succeeded"}}"#,
        ),
        (
            "/v1/tasks/t1/subscribe",
            r#"{"type":"task:update","task":{"task_id":"t1","status":"running"}}"#,
            r#"{"type":"task:update","task":{"task_id":"t1","status":"completed"}}"#,
        ),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(sse_response(&[running, COMPLETE]))
            .up_to_n_times(7)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(sse_response(&[done]))
            .mount(&server)
            .await;
    }
    server
}

#[tokio::test]
async fn rollovers_do_not_use_up_the_reconnect_budget() {
    let started = Instant::now();

    let server = rollover_server().await;
    let events = collect_events(&server, SubscribeWithEventOptions::default())
        .await
        .unwrap();
    assert_eq!(events.last().unwrap().event_type(), "run:stop");

    let server = rollover_server().await;
    let result = client_for(&server)
        .events()
        .call_hot("f", vec![], hot_dev::CallOptions::default())
        .await
        .unwrap();
    assert_eq!(result, json!(7));

    let server = rollover_server().await;
    let run = client_for(&server)
        .streams()
        .wait_for_run_result("s1", "e1", hot_dev::WaitOptions::default())
        .await
        .unwrap();
    assert_eq!(run["run_id"], "r1");

    let server = rollover_server().await;
    let run = client_for(&server)
        .runs()
        .wait("r1", hot_dev::RunWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(run["status"], "succeeded");

    let server = rollover_server().await;
    let task = client_for(&server)
        .tasks()
        .wait("t1", hot_dev::TaskWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(task["status"], "completed");

    // Rollovers reconnect without backoff.
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn deadline_bounds_backoff() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(error_response(503))
        .mount(&server)
        .await;

    let started = Instant::now();
    let error = client_for(&server)
        .streams()
        .wait_for_run_result(
            "s1",
            "e1",
            hot_dev::WaitOptions {
                timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(matches!(error, Error::Timeout), "{error:?}");
    // Not milliseconds of immediate retries, and not past the deadline.
    assert!(elapsed >= Duration::from_millis(700), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(2500), "{elapsed:?}");
    assert!(get_requests(&server).await <= 4);
}

#[tokio::test]
async fn a_short_outage_does_not_end_a_long_wait() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(error_response(503))
        .up_to_n_times(3)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(sse_response(&[
            r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}"#,
        ]))
        .mount(&server)
        .await;

    let run = client_for(&server)
        .streams()
        .wait_for_run_result(
            "s1",
            "e1",
            hot_dev::WaitOptions {
                timeout: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(run["result"], "done");
}

#[tokio::test]
async fn refused_connections_back_off_until_the_deadline() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let client = HotClient::builder("test-token")
        .base_url(format!("http://127.0.0.1:{port}"))
        .build();

    let started = Instant::now();
    let error = client
        .runs()
        .wait(
            "r1",
            hot_dev::RunWaitOptions {
                timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Timeout), "{error:?}");
    assert!(started.elapsed() >= Duration::from_millis(700));
}

#[tokio::test]
async fn consecutive_failures_exhaust_the_budget() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(error_response(503))
        .mount(&server)
        .await;

    let error = client_for(&server)
        .runs()
        .wait(
            "r1",
            hot_dev::RunWaitOptions {
                timeout: Duration::from_secs(30),
                max_attempts: 2,
            },
        )
        .await;
    assert!(is_status(&error, 503), "{error:?}");
    assert_eq!(get_requests(&server).await, 3);
}

#[tokio::test]
async fn connections_without_events_count_as_failures() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(sse_response(&[]))
        .mount(&server)
        .await;

    let error = client_for(&server)
        .runs()
        .wait(
            "r1",
            hot_dev::RunWaitOptions {
                timeout: Duration::from_secs(30),
                max_attempts: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    assert_eq!(get_requests(&server).await, 2);
}
