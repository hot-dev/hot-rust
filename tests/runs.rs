use hot_dev::{Error, HotClient, RunWaitOptions};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn run_event(status: &str, result: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(
        format!(
            "data: {{\"type\":\"run:update\",\"run\":{{\"run_id\":\"r1\",\"status\":\"{status}\",\"result\":\"{result}\"}}}}\n\n"
        ),
        "text/event-stream",
    )
}

#[tokio::test]
async fn run_wait_returns_succeeded_durable_snapshot() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/r1/subscribe"))
        .respond_with(run_event("succeeded", "done"))
        .expect(1)
        .mount(&server)
        .await;
    let client = HotClient::builder("token").base_url(server.uri()).build();
    let run = client
        .runs()
        .wait("r1", RunWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(run["status"], "succeeded");
    assert_eq!(run["result"], "done");
}

#[tokio::test]
async fn run_wait_returns_structured_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/runs/r1/subscribe"))
        .respond_with(run_event("failed", "boom"))
        .mount(&server)
        .await;
    let client = HotClient::builder("token").base_url(server.uri()).build();
    let error = client
        .runs()
        .wait("r1", RunWaitOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::RunWaitFailed { message, run }
            if message == "boom" && run["status"] == "failed"
    ));
}
