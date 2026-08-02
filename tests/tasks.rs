use hot_dev::{Error, HotClient, TaskWaitOptions};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn task_event(status: &str, result: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(
        format!(
            "data: {{\"type\":\"task:update\",\"task\":{{\"task_id\":\"t1\",\"status\":\"{status}\",\"result\":\"{result}\"}}}}\n\n"
        ),
        "text/event-stream",
    )
}

#[tokio::test]
async fn task_wait_returns_completed_durable_snapshot() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/tasks/t1/subscribe"))
        .respond_with(task_event("completed", "done"))
        .expect(1)
        .mount(&server)
        .await;
    let client = HotClient::builder("token").base_url(server.uri()).build();
    let task = client
        .tasks()
        .wait("t1", TaskWaitOptions::default())
        .await
        .unwrap();
    assert_eq!(task["status"], "completed");
    assert_eq!(task["result"], "done");
}

#[tokio::test]
async fn task_wait_returns_structured_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/tasks/t1/subscribe"))
        .respond_with(task_event("failed", "boom"))
        .mount(&server)
        .await;
    let client = HotClient::builder("token").base_url(server.uri()).build();
    let error = client
        .tasks()
        .wait("t1", TaskWaitOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::TaskFailed { message, task }
            if message == "boom" && task["status"] == "failed"
    ));
}
