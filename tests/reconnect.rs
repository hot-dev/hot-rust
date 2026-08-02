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
