use futures_util::StreamExt;
use hot_dev::{CallOptions, Error, HotClient, SubscribeWithEventOptions};
use serde_json::{json, Value};
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn envelope(data: Value) -> Value {
    json!({ "data": data, "meta": { "request_id": "req_123", "timestamp": "now" } })
}

fn sse_body(events: &[&str]) -> String {
    events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect()
}

fn sse_response(events: &[&str]) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(sse_body(events), "text/event-stream")
}

#[tokio::test]
async fn publish_event_sends_bearer_token_and_returns_data() {
    let server = MockServer::start().await;
    let body = json!({
        "event_type": "team-agent:ask",
        "event_data": { "question": "what changed?" },
    });
    Mock::given(method("POST"))
        .and(path("/v1/events"))
        .and(header("authorization", "Bearer test-token"))
        .and(header(
            "user-agent",
            concat!("hot-sdk-rust/", env!("CARGO_PKG_VERSION")),
        ))
        .and(body_json(&body))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({
            "event_id": "evt_123", "stream_id": "str_123",
        }))))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let event = client.events().publish(&body).await.unwrap();

    assert_eq!(event["event_id"], "evt_123");
}

#[tokio::test]
async fn list_methods_return_api_list_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs"))
        .and(query_param("status", "completed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{ "id": "run_123" }],
            "meta": { "request_id": "req_123" },
            "pagination": { "page": 1, "per_page": 20, "total": 1, "total_pages": 1 },
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let response = client
        .runs()
        .list(&[("status", "completed")])
        .await
        .unwrap();

    assert_eq!(response["data"], json!([{ "id": "run_123" }]));
    assert_eq!(response["pagination"]["total"], 1);
}

#[tokio::test]
async fn streams_parse_sse_events() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            r#"{"type":"stream:data","data_type":"text","payload":"hi"}"#,
            r#"{"type":"run:stop","run":{"result":"done"}}"#,
        ]))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let options = SubscribeWithEventOptions {
        reconnect: false,
        ..Default::default()
    };
    let mut stream = client
        .streams()
        .subscribe_with_event(json!({ "event_type": "team-agent:ask" }), options);

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(Value::Object(event.unwrap()));
    }

    assert_eq!(
        events,
        vec![
            json!({ "type": "stream:data", "data_type": "text", "payload": "hi" }),
            json!({ "type": "run:stop", "run": { "result": "done" } }),
        ]
    );
}

#[tokio::test]
async fn api_error_is_structured() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/events"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("Retry-After", "5")
                .set_body_json(json!({
                    "error": {
                        "code": "unauthorized",
                        "message": "Bad token",
                        "request_id": "req_123",
                        "retry_after": 5,
                    }
                })),
        )
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client.events().list(&[]).await.unwrap_err();

    let Error::Api(api_error) = error else {
        panic!("expected Error::Api, got {error:?}");
    };
    assert_eq!(api_error.status_code, 401);
    assert_eq!(api_error.code.as_deref(), Some("unauthorized"));
    assert_eq!(api_error.request_id.as_deref(), Some("req_123"));
    assert_eq!(api_error.retry_after, Some(5));
    assert_eq!(api_error.message, "Bad token");
}

#[tokio::test]
async fn retries_429_with_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/run_123"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "1")
                .set_body_json(json!({
                    "error": { "code": "rate_limited", "message": "Slow down" }
                })),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/run_123"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "id": "run_123" }))),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let run = client.runs().get("run_123").await.unwrap();

    assert_eq!(run["id"], "run_123");
}

#[tokio::test]
async fn does_not_retry_429_without_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/run_123"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({
            "error": { "code": "rate_limited", "message": "Slow down" }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client.runs().get("run_123").await.unwrap_err();

    let Error::Api(api_error) = error else {
        panic!("expected Error::Api, got {error:?}");
    };
    assert_eq!(api_error.status_code, 429);
}

#[tokio::test]
async fn api_error_falls_back_to_retry_after_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/events"))
        .respond_with(
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "30")
                .set_body_string("slow down"),
        )
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client.events().list(&[]).await.unwrap_err();

    let Error::Api(api_error) = error else {
        panic!("expected Error::Api, got {error:?}");
    };
    assert_eq!(api_error.status_code, 503);
    assert_eq!(api_error.retry_after, Some(30));
    assert_eq!(api_error.message, "slow down");
}

#[tokio::test]
async fn call_hot_uses_nested_run_failure_result() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            r#"{"type":"event:published","event_id":"evt_123","stream_id":"str_123"}"#,
            r#"{"type":"run:start","run":{"run_id":"run_123","event_id":"evt_123"}}"#,
            r#"{"type":"run:fail","run":{"run_id":"run_123","event_id":"evt_123","status":"failed","result":{"$err":{"message":"boom"}}}}"#,
        ]))
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client
        .events()
        .call_hot("demo/fn", vec![], CallOptions::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&error, Error::RunFailed(message) if message == "boom"),
        "{error:?}"
    );
}

#[tokio::test]
async fn call_hot_surfaces_hot_run_failure_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .respond_with(sse_response(&[
            r#"{"type":"event:published","event_id":"evt_123","stream_id":"str_123"}"#,
            r#"{"type":"run:start","run":{"run_id":"run_123","event_id":"evt_123"}}"#,
            r#"{"type":"run:fail","run":{"run_id":"run_123","event_id":"evt_123","status":"failed","result":{"$type":"::hot::run/Failure","$origin":{"function":"::hot::exec/fail"},"$val":{"msg":"too hot","err":"too hot"}}}}"#,
        ]))
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client
        .events()
        .call_hot("demo/fn", vec![], CallOptions::default())
        .await
        .unwrap_err();

    assert!(
        matches!(&error, Error::RunFailed(message) if message == "too hot"),
        "{error:?}"
    );
}

#[tokio::test]
async fn call_hot_unwraps_ok_result_and_streams_chunks() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/streams/subscribe-with-event"))
        .and(body_json(json!({
            "event_type": "hot:call",
            "event_data": { "fn": "demo/fn", "args": [1, 2] },
        })))
        .respond_with(sse_response(&[
            r#"{"type":"event:published","event_id":"evt_1","stream_id":"str_1"}"#,
            r#"{"type":"run:start","run":{"run_id":"r1","event_id":"evt_1"}}"#,
            r#"{"type":"stream:data","run_id":"r1","payload":{"text":"partial"}}"#,
            r#"{"type":"run:stop","run":{"run_id":"r1","event_id":"evt_1","result":{"$ok":42}}}"#,
        ]))
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let mut chunks = Vec::new();
    let mut on_chunk = |text: &str| chunks.push(text.to_string());
    let result = client
        .events()
        .call_hot(
            "demo/fn",
            vec![json!(1), json!(2)],
            CallOptions {
                on_chunk: Some(&mut on_chunk),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(result, json!(42));
    assert_eq!(chunks, vec!["partial"]);
}

#[tokio::test]
async fn request_escape_hatch_handles_empty_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/new-endpoint"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let response = client
        .request(reqwest::Method::POST, "/new-endpoint", None, &[])
        .await
        .unwrap();

    assert_eq!(response["data"], Value::Null);
}

#[tokio::test]
async fn path_segments_are_escaped() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/a%2Fb%20c"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({}))))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    client.projects().get("a/b c").await.unwrap();
}
