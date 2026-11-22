use futures_util::StreamExt;
use hot_dev::{BuildUpload, CallOptions, Error, HotClient, KeyScope, SubscribeWithEventOptions};
use serde_json::{json, Value};
use wiremock::matchers::{body_json, header, header_exists, method, path, query_param};
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
            "pagination": { "total": 1, "limit": 20, "offset": 0, "has_more": false },
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
    // POST is not idempotent, so the 503 is returned rather than retried.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/events"))
        .respond_with(
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "30")
                .set_body_string("slow down"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client.events().publish(json!({})).await.unwrap_err();

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

#[tokio::test]
async fn blob_download_returns_the_raw_bytes() {
    let server = MockServer::start().await;
    let bytes: Vec<u8> = vec![0, 159, 146, 150, 255, 10, 13];
    Mock::given(method("GET"))
        .and(path("/v1/blobs/blob_123/download"))
        .and(header("authorization", "Bearer env-token"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(bytes.clone(), "application/octet-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("env-token")
        .base_url(server.uri())
        .build();
    let response = client.blobs().download("blob_123").await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
    assert_eq!(response.bytes().await.unwrap().to_vec(), bytes);
}

#[tokio::test]
async fn blob_download_escapes_the_id_and_maps_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/blobs/a%2Fb%20c/download"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": { "code": "not_found", "message": "Blob not found" }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("env-token")
        .base_url(server.uri())
        .build();
    let error = client.blobs().download("a/b c").await.unwrap_err();

    let Error::Api(api_error) = error else {
        panic!("expected Error::Api, got {error:?}");
    };
    assert_eq!(api_error.status_code, 404);
    assert_eq!(api_error.code.as_deref(), Some("not_found"));
}

#[tokio::test]
async fn idempotent_requests_retry_unavailable_responses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/run_123"))
        .respond_with(ResponseTemplate::new(503))
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
    assert_eq!(client.runs().get("run_123").await.unwrap()["id"], "run_123");
}

#[tokio::test]
async fn deletes_use_the_request_timeout_and_retry_rate_limits() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/v1/projects/slow"))
        .respond_with(ResponseTemplate::new(204).set_delay(std::time::Duration::from_secs(3)))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/service-keys/k1"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "1")
                .set_body_json(
                    json!({ "error": { "code": "rate_limited", "message": "Slow down" } }),
                ),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/service-keys/k1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .timeout(std::time::Duration::from_millis(200))
        .build();
    let started = std::time::Instant::now();
    let error = client.projects().delete("slow").await.unwrap_err();
    assert!(
        matches!(&error, Error::Http(error) if error.is_timeout()),
        "{error:?}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    client.service_keys().revoke("k1").await.unwrap();
}

#[tokio::test]
async fn token_whitespace_is_trimmed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/env"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({}))))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("  test-token\n")
        .base_url(server.uri())
        .build();
    client.env().get().await.unwrap();
}

#[tokio::test]
async fn invalid_tokens_are_rejected_before_sending() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({}))))
        .expect(0)
        .mount(&server)
        .await;

    assert!(HotClient::builder("").try_build().is_err());
    assert!(HotClient::builder("   \t\n").try_build().is_err());
    assert!(HotClient::builder("hot_one\nhot_two").try_build().is_err());
    assert!(HotClient::builder("hot_key\n").try_build().is_ok());

    let empty_client = HotClient::builder("   ").base_url(server.uri()).build();
    assert!(matches!(
        empty_client.env().get().await.unwrap_err(),
        Error::Http(error) if error.is_builder()
    ));

    let client = HotClient::builder("hot_one\nhot_two")
        .base_url(server.uri())
        .build();
    let error = client.env().get().await.unwrap_err();
    assert!(
        matches!(&error, Error::Http(error) if error.is_builder()),
        "{error:?}"
    );
    let mut stream = client.env().subscribe();
    assert!(matches!(stream.next().await, Some(Err(Error::Http(_)))));
}

#[tokio::test]
async fn key_scope_mismatch_names_the_key_the_call_needs() {
    let server = MockServer::start().await;
    for (route, server_message) in [
        ("/v1/env", "This route needs an environment API key; org API keys call /v1/orgs and /v1/api-keys"),
        ("/v1/orgs", "This route needs an org API key; environment keys, sessions and service keys call runtime routes"),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "error": { "code": "key_scope_mismatch", "message": server_message }
            })))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/v1/projects"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "code": "forbidden", "message": "no" }
        })))
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let error = client.env().get().await.unwrap_err();
    assert_eq!(error.required_scope(), Some(KeyScope::Env));
    assert!(
        error.to_string().contains("needs an environment API key"),
        "{error}"
    );
    let error = client.orgs().list(&[]).await.unwrap_err();
    assert_eq!(error.required_scope(), Some(KeyScope::Org));
    let Error::Api(api_error) = &error else {
        panic!("{error:?}");
    };
    assert_eq!(api_error.status_code, 403);
    assert_eq!(api_error.code.as_deref(), Some("key_scope_mismatch"));
    assert!(api_error
        .message
        .starts_with("This call needs an org API key"));
    // The server's own message is kept in the raw error object.
    assert_eq!(
        api_error.error.as_ref().unwrap()["message"],
        "This route needs an org API key; environment keys, sessions and service keys call runtime routes"
    );
    assert_eq!(KeyScope::Org.as_str(), "org");
    let error = client.projects().list(&[]).await.unwrap_err();
    assert_eq!(error.required_scope(), None);
}

#[tokio::test]
async fn org_api_contract() {
    let server = MockServer::start().await;
    let body = json!({ "marker": "request" });
    for (verb, route, query, has_body) in [
        ("GET", "/v1/orgs", Some(("limit", "7")), false),
        ("POST", "/v1/orgs", None, true),
        ("GET", "/v1/orgs/o%2F1", None, false),
        ("PATCH", "/v1/orgs/o%2F1", None, true),
        ("GET", "/v1/orgs/o%2F1/usage", None, false),
        (
            "DELETE",
            "/v1/orgs/o%2F1/calls",
            Some(("include_sub_orgs", "true")),
            false,
        ),
        ("GET", "/v1/orgs/o%2F1/envs", None, false),
        ("POST", "/v1/orgs/o%2F1/envs", None, true),
        ("GET", "/v1/orgs/o%2F1/envs/e1", None, false),
        ("GET", "/v1/orgs/o%2F1/api-keys", None, false),
        ("POST", "/v1/orgs/o%2F1/api-keys", None, true),
        (
            "GET",
            "/v1/orgs/o%2F1/envs/e1/api-keys",
            Some(("offset", "3")),
            false,
        ),
        ("POST", "/v1/orgs/o%2F1/envs/e1/api-keys", None, true),
        ("GET", "/v1/api-keys/k1", None, false),
        ("PATCH", "/v1/api-keys/k1", None, true),
        ("DELETE", "/v1/api-keys/k1", None, false),
        ("DELETE", "/v1/env/calls", None, false),
        ("DELETE", "/v1/runs/r1/calls", None, false),
        ("DELETE", "/v1/streams/s1/calls", None, false),
    ] {
        let mut mock = Mock::given(method(verb)).and(path(route));
        if let Some((name, value)) = query {
            mock = mock.and(query_param(name, value));
        }
        if has_body {
            mock = mock.and(body_json(&body));
        }
        mock.respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "ok": route }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/api-keys/k1/rotate"))
        .and(body_json(json!({ "overlap_seconds": 60 })))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({ "token": "new" }))))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("org-token")
        .base_url(server.uri())
        .build();
    let orgs = client.orgs();
    assert_eq!(
        orgs.list(&[("limit", "7")]).await.unwrap()["data"]["ok"],
        "/v1/orgs"
    );
    assert_eq!(orgs.create(&body).await.unwrap()["ok"], "/v1/orgs");
    orgs.get("o/1").await.unwrap();
    orgs.update("o/1", &body).await.unwrap();
    orgs.usage("o/1").await.unwrap();
    orgs.purge_calls("o/1", true).await.unwrap();
    orgs.list_envs("o/1").await.unwrap();
    orgs.create_env("o/1", &body).await.unwrap();
    orgs.get_env("o/1", "e1").await.unwrap();
    orgs.list_api_keys("o/1").await.unwrap();
    orgs.create_api_key("o/1", &body).await.unwrap();
    orgs.list_env_api_keys("o/1", "e1", &[("offset", "3")])
        .await
        .unwrap();
    orgs.create_env_api_key("o/1", "e1", &body).await.unwrap();
    client.api_keys().get("k1").await.unwrap();
    client.api_keys().update("k1", &body).await.unwrap();
    client.api_keys().revoke("k1").await.unwrap();
    assert_eq!(
        client.api_keys().rotate("k1", Some(60)).await.unwrap()["token"],
        "new"
    );
    client.env().purge_calls().await.unwrap();
    client.runs().purge_calls("r1").await.unwrap();
    client.streams().purge_calls("s1").await.unwrap();
}

#[tokio::test]
async fn org_env_scope_prefixes_project_build_and_context_routes() {
    let server = MockServer::start().await;
    for (verb, route) in [
        ("GET", "/v1/orgs/o1/envs/e1/projects"),
        ("GET", "/v1/orgs/o1/envs/e1/projects/p1"),
        ("POST", "/v1/orgs/o1/envs/e1/projects/p1/activate"),
        ("GET", "/v1/orgs/o1/envs/e1/builds"),
        ("POST", "/v1/orgs/o1/envs/e1/projects/p1/builds/b1/deploy"),
        ("GET", "/v1/orgs/o1/envs/e1/projects/p1/context"),
        ("PUT", "/v1/orgs/o1/envs/e1/projects/p1/context/a.b"),
        ("GET", "/v1/orgs/o1/envs/e1/projects/p1/schedules"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({}))))
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = HotClient::builder("org-token")
        .base_url(server.uri())
        .build();
    let env = client.orgs().env("o1", "e1");
    env.projects().list(&[]).await.unwrap();
    env.projects().get("p1").await.unwrap();
    env.projects().activate("p1").await.unwrap();
    env.builds().list(&[]).await.unwrap();
    env.builds().deploy("p1", "b1").await.unwrap();
    env.context().list("p1").await.unwrap();
    env.context()
        .update("p1", "a.b", json!({ "value": "v" }))
        .await
        .unwrap();
    env.projects().schedules("p1").await.unwrap();
}

#[tokio::test]
async fn list_helpers_accept_pagination() {
    let server = MockServer::start().await;
    for route in [
        "/v1/projects/p1/context",
        "/v1/domains",
        "/v1/events/e1/runs",
        "/v1/orgs/o1/envs",
        "/v1/orgs/o1/api-keys",
        "/v1/projects/p1/event-handlers",
        "/v1/projects/p1/schedules",
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let page: &[(&str, &str)] = &[("limit", "100"), ("offset", "20")];
    client.context().list_with_query("p1", page).await.unwrap();
    client.domains().list_with_query(page).await.unwrap();
    client
        .events()
        .get_runs_with_query("e1", page)
        .await
        .unwrap();
    client
        .orgs()
        .list_envs_with_query("o1", page)
        .await
        .unwrap();
    client
        .orgs()
        .list_api_keys_with_query("o1", page)
        .await
        .unwrap();
    client
        .projects()
        .event_handlers_with_query("p1", page)
        .await
        .unwrap();
    client
        .projects()
        .schedules_with_query("p1", page)
        .await
        .unwrap();
}

#[test]
fn reqwest_is_reexported_for_custom_http_clients() {
    let http = hot_dev::reqwest::Client::builder().build().unwrap();
    let _client = HotClient::builder("token").http_client(http).build();
}

#[tokio::test]
async fn status_hits_unauthenticated_root_status_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/status"))
        .and(header(
            "user-agent",
            concat!("hot-sdk-rust/", env!("CARGO_PKG_VERSION")),
        ))
        .and(header("accept", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "ok",
            "version": "3.0.14",
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Even when configured with a trailing `/v1/`, status hits `{base_url}/status`.
    let client = HotClient::builder("secret-token")
        .base_url(format!("{}/v1/", server.uri()))
        .build();
    let status = client.status().await.unwrap();
    assert_eq!(status["status"], "ok");
    assert_eq!(status["version"], "3.0.14");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        !requests[0].headers.contains_key("authorization"),
        "GET /status must not send Authorization header"
    );
}

#[tokio::test]
async fn path_segment_validation_rejects_bad_segments_before_http() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    assert!(matches!(
        client.sessions().revoke(".").await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        client.context().delete("p1", "..").await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        client.projects().get("").await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        client.files().upload("../x", b"abc".to_vec(), None).await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        client.orgs().env("..", "e1").projects().list(&[]).await,
        Err(Error::Protocol(_))
    ));
}

#[tokio::test]
async fn redirects_are_not_followed_and_map_to_api_error() {
    let server = MockServer::start().await;
    for (verb, route) in [
        ("GET", "/v1/projects/p1"),
        ("GET", "/v1/blobs/b1/download"),
        ("GET", "/v1/streams/s1/subscribe"),
        ("GET", "/status"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/redirected", server.uri()))
                    .set_body_string("Found"),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/redirected"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();

    assert!(matches!(
        client.projects().get("p1").await,
        Err(Error::Api(err)) if err.status_code == 302
    ));
    assert!(matches!(
        client.blobs().download("b1").await,
        Err(Error::Api(err)) if err.status_code == 302
    ));
    let mut stream = client.streams().subscribe("s1", None);
    assert!(matches!(
        stream.next().await,
        Some(Err(Error::Api(err))) if err.status_code == 302
    ));
    assert!(matches!(
        client.status().await,
        Err(Error::Api(err)) if err.status_code == 302
    ));
}

#[tokio::test]
async fn events_publish_supports_event_id_retries_503_in_progress_and_surfaces_409() {
    let server = MockServer::start().await;
    let payload = json!({
        "event_id": "01950000-0000-7000-8000-000000000001",
        "event_type": "app:order",
        "event_data": { "order": 1 },
    });

    // 503 event_publish_in_progress on POST is retried automatically.
    Mock::given(method("POST"))
        .and(path("/v1/events"))
        .and(body_json(&payload))
        .respond_with(
            ResponseTemplate::new(503)
                .insert_header("Retry-After", "1")
                .set_body_json(json!({
                    "error": {
                        "code": "event_publish_in_progress",
                        "message": "Event publish already in progress",
                    }
                })),
        )
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/events"))
        .and(body_json(&payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({
            "event_id": "01950000-0000-7000-8000-000000000001",
            "stream_id": "str_1",
        }))))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let event = client.events().publish(&payload).await.unwrap();
    assert_eq!(event["event_id"], "01950000-0000-7000-8000-000000000001");

    // 409 event_id_conflict is returned immediately without retry.
    let conflict_payload = json!({
        "event_id": "01950000-0000-7000-8000-000000000001",
        "event_type": "app:order",
        "event_data": { "order": 2 },
    });
    Mock::given(method("POST"))
        .and(path("/v1/events"))
        .and(body_json(&conflict_payload))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": {
                "code": "event_id_conflict",
                "message": "Event ID already used with different payload",
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let error = client
        .events()
        .publish(&conflict_payload)
        .await
        .unwrap_err();
    let Error::Api(api_err) = error else {
        panic!("expected ApiError, got {error:?}");
    };
    assert_eq!(api_err.status_code, 409);
    assert_eq!(api_err.code.as_deref(), Some("event_id_conflict"));
}

#[tokio::test]
async fn files_resource_contract_and_empty_204_upload() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/files"))
        .and(query_param("limit", "10"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/files/f1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "file_id": "f1" }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/files/f1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(b"hello".to_vec(), "text/plain"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/files/f1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/v1/files/upload/docs/hello%20world.txt"))
        .and(header("content-type", "text/plain"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(envelope(json!({ "path": "docs/hello world.txt" }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    // Empty 204 body from upload decodes cleanly via decode_data_response.
    Mock::given(method("PUT"))
        .and(path("/v1/files/upload/empty.bin"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/files/uploads"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "upload_id": "u1" }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/v1/files/uploads/u1/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({ "part": 1 }))))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/files/uploads/u1/complete"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "file_id": "f2" }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/files/uploads/u1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let files = client.files();
    files.list(&[("limit", "10")]).await.unwrap();
    assert_eq!(files.get("f1").await.unwrap()["file_id"], "f1");
    let dl = files.download("f1").await.unwrap();
    assert_eq!(dl.bytes().await.unwrap().as_ref(), b"hello");
    files.delete("f1").await.unwrap();
    assert_eq!(
        files
            .upload("/docs/hello world.txt", b"hi".to_vec(), Some("text/plain"))
            .await
            .unwrap()["path"],
        "docs/hello world.txt"
    );
    assert!(files
        .upload("empty.bin", vec![], None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        files
            .initiate_upload(json!({ "path": "big.bin" }))
            .await
            .unwrap()["upload_id"],
        "u1"
    );
    assert_eq!(
        files.upload_part("u1", 1, b"part".to_vec()).await.unwrap()["part"],
        1
    );
    assert_eq!(files.complete_upload("u1").await.unwrap()["file_id"], "f2");
    files.abort_upload("u1").await.unwrap();
}

#[tokio::test]
async fn builds_domains_sessions_service_keys_and_escape_hatch_contract() {
    let server = MockServer::start().await;
    for (verb, route) in [
        ("GET", "/v1/builds"),
        ("GET", "/v1/projects/p1/builds"),
        ("GET", "/v1/projects/p1/builds/b1"),
        ("GET", "/v1/projects/p1/builds/deployed"),
        ("GET", "/v1/projects/p1/builds/live"),
        ("POST", "/v1/projects/p1/builds/b1/deploy"),
        ("POST", "/v1/domains"),
        ("GET", "/v1/domains"),
        ("GET", "/v1/domains/d1"),
        ("DELETE", "/v1/domains/d1"),
        ("POST", "/v1/domains/d1/verify"),
        ("POST", "/v1/sessions"),
        ("GET", "/v1/sessions"),
        ("DELETE", "/v1/sessions/s1"),
        ("DELETE", "/v1/sessions"),
        ("POST", "/v1/service-keys"),
        ("GET", "/v1/service-keys"),
        ("GET", "/v1/service-keys/sk1"),
        ("PATCH", "/v1/service-keys/sk1"),
        ("DELETE", "/v1/service-keys/sk1"),
        ("DELETE", "/v1/service-keys"),
        ("GET", "/v1/custom-route"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({ "ok": true }))))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/projects/p1/builds"))
        .and(header_exists("content-type"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(envelope(json!({ "build_id": "b2" }))),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/projects/p1/builds/b1/download"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(b"tarbytes".to_vec(), "application/octet-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/raw-check"))
        .and(header("x-custom", "yes"))
        .respond_with(ResponseTemplate::new(200).set_body_string("raw-ok"))
        .expect(1)
        .mount(&server)
        .await;

    // Also verifies base_url normalization with trailing `/v1/` and request() without leading `/`.
    let client = HotClient::builder("test-token")
        .base_url(format!("{}/v1/", server.uri()))
        .build();

    client.builds().list(&[]).await.unwrap();
    client.builds().list_for_project("p1", &[]).await.unwrap();
    client.builds().get("p1", "b1").await.unwrap();
    client.builds().deployed("p1").await.unwrap();
    client.builds().live("p1").await.unwrap();
    let uploaded = client
        .builds()
        .upload(
            "p1",
            BuildUpload {
                content: b"archive".to_vec(),
                filename: Some("app.tar".to_string()),
                hash: "sha256:abc".to_string(),
                build_id: Some("b2".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(uploaded["build_id"], "b2");
    let archive = client.builds().download("p1", "b1").await.unwrap();
    assert_eq!(archive.bytes().await.unwrap().as_ref(), b"tarbytes");
    client.builds().deploy("p1", "b1").await.unwrap();

    client
        .domains()
        .create(json!({ "domain": "app.example" }))
        .await
        .unwrap();
    client.domains().list().await.unwrap();
    client.domains().get("d1").await.unwrap();
    client.domains().delete("d1").await.unwrap();
    client.domains().verify("d1").await.unwrap();

    client.sessions().create(json!({})).await.unwrap();
    client.sessions().list(&[]).await.unwrap();
    client.sessions().revoke("s1").await.unwrap();
    client.sessions().revoke_all().await.unwrap();

    client.service_keys().create(json!({})).await.unwrap();
    client.service_keys().list(&[]).await.unwrap();
    client.service_keys().get("sk1").await.unwrap();
    client
        .service_keys()
        .update("sk1", json!({}))
        .await
        .unwrap();
    client.service_keys().revoke("sk1").await.unwrap();
    client.service_keys().revoke_all().await.unwrap();

    // Escape hatch without leading `/` works cleanly.
    client
        .request(hot_dev::reqwest::Method::GET, "custom-route", None, &[])
        .await
        .unwrap();

    let builder = client
        .request_builder(hot_dev::reqwest::Method::GET, "raw-check")
        .header("x-custom", "yes");
    let raw = client.execute(builder).await.unwrap();
    assert_eq!(raw.text().await.unwrap(), "raw-ok");
}

#[tokio::test]
async fn streams_handle_crlf_multibyte_and_invalid_utf8_over_http() {
    let server = MockServer::start().await;
    let mut raw = Vec::new();
    raw.extend_from_slice(b"\xEF\xBB\xBFdata: {\"type\":\"a\",\"text\":\"\xE2\x82\xAC");
    raw.push(0xFF);
    raw.extend_from_slice(b"\"}\r\n\r\ndata: {\"type\":\"b\"}\r\n\r\n");

    Mock::given(method("GET"))
        .and(path("/v1/streams/s1/subscribe"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(raw, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;

    let client = HotClient::builder("test-token")
        .base_url(server.uri())
        .build();
    let mut stream = client.streams().subscribe("s1", None);
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["type"], "a");
    assert_eq!(events[0]["text"], "€\u{FFFD}");
    assert_eq!(events[1]["type"], "b");
}
