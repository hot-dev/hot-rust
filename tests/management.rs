use futures_util::StreamExt;
use hot_dev::{
    Error, HotClient, HotManagementClient, RunWaitOptions, SubscribeWithEventOptions,
    TaskWaitOptions, WaitOptions,
};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn management_contract() {
    let server = MockServer::start().await;
    for (verb, route) in [
        ("GET", "/v1/management/children"),
        ("POST", "/v1/management/children"),
        ("PATCH", "/v1/management/children/child%2Fone/state"),
        ("POST", "/v1/management/children/child%2Fone/environments"),
        (
            "POST",
            "/v1/management/children/child%2Fone/environments/env%2Ftwo/api-keys",
        ),
        ("GET", "/v1/management/usage"),
    ] {
        let mut mock = Mock::given(method(verb))
            .and(path(route))
            .and(header("authorization", "Bearer hm_test"));
        if verb == "POST" {
            mock = mock.and(body_json(json!({"name":"sdk"})));
        }
        if verb == "PATCH" {
            mock = mock.and(body_json(json!({"state":"suspended"})));
        }
        if verb == "GET" && route.ends_with("children") {
            mock = mock
                .and(query_param("limit", "7"))
                .and(query_param("offset", "3"));
        }
        let response = if verb == "PATCH" {
            ResponseTemplate::new(204)
        } else {
            ResponseTemplate::new(200)
                .set_body_json(json!({"data":{"marker":"response"},"meta":{}}))
        };
        mock.respond_with(response).expect(1).mount(&server).await;
    }
    let client = HotManagementClient::builder("hm_test")
        .base_url(server.uri())
        .build();
    assert_eq!(
        client
            .list_children(&[("limit", "7"), ("offset", "3")])
            .await
            .unwrap()["marker"],
        "response"
    );
    client.create_child(json!({"name":"sdk"})).await.unwrap();
    client
        .set_child_state("child/one", "suspended")
        .await
        .unwrap();
    client
        .create_environment("child/one", json!({"name":"sdk"}))
        .await
        .unwrap();
    client
        .create_api_key("child/one", "env/two", json!({"name":"sdk"}))
        .await
        .unwrap();
    client.usage().await.unwrap();
}

#[tokio::test]
async fn authorization_failures_stop_reconnects() {
    for status in [401, 403] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/streams/subscribe-with-event"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"type\":\"event:published\",\"stream_id\":\"s\",\"event_id\":\"e\"}\n\n",
                "text/event-stream",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(json!({"error":{"message":"revoked"}})),
            )
            .expect(5)
            .mount(&server)
            .await;
        let client = HotClient::builder("test").base_url(server.uri()).build();
        let mut events = client
            .streams()
            .subscribe_with_event(json!({}), SubscribeWithEventOptions::default());
        events.next().await.unwrap().unwrap();
        let error = events.next().await.unwrap().unwrap_err();
        assert!(matches!(error,Error::Api(e) if e.status_code==status));
        assert!(
            matches!(client.tasks().wait("t",TaskWaitOptions::default()).await,Err(Error::Api(e)) if e.status_code==status)
        );
        assert!(
            matches!(client.runs().wait("r",RunWaitOptions::default()).await,Err(Error::Api(e)) if e.status_code==status)
        );
        assert!(
            matches!(client.streams().wait_for_run_result("s","e",WaitOptions::default()).await,Err(Error::Api(e)) if e.status_code==status)
        );
        // Use the raw result waiter on an existing stream; initial auth failure cannot publish work.
        assert!(
            matches!(client.streams().wait_for_run_result("s2","e2",WaitOptions::default()).await,Err(Error::Api(e)) if e.status_code==status)
        );
    }
}
