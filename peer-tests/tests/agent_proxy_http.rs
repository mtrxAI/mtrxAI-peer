mod common;

use peer::client_config::{ClientConfig, LlmServerEntry};
use peer::llm_proxy::{proxy_router, ProxyState};
use common::test_app_state;
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_proxy_state_with_backend(server_uri: &str) -> ProxyState {
    let (proxy_cmd_tx, _) = mpsc::channel(1);
    let (runtime_event_tx, room_notify_tx, swarm_notify_tx) = common::test_notify_channels();
    let shared_state = Arc::new(Mutex::new(test_app_state(proxy_cmd_tx)));
    let config = Arc::new(RwLock::new(ClientConfig {
        setup_complete: true,
        llm_servers: vec![LlmServerEntry {
            id: "test-ollama".to_string(),
            kind: "ollama".to_string(),
            url: server_uri.to_string(),
            label: None,
            attached: true,
            order: 0,
            source: "test".to_string(),
            api_type: None,
            models: Vec::new(),
            advertise_to_cluster: true,
        }],
        ..Default::default()
    }));
    ProxyState::new(
        shared_state.clone(),
        config.clone(),
        "127.0.0.1:8080".to_string(),
        11345,
        "127.0.0.1".to_string(),
        runtime_event_tx,
        room_notify_tx,
        swarm_notify_tx,
        true,
        common::test_tx_store(),
        common::test_peer_stats(),
        common::test_peer_registry(),
        common::test_moderation_channel(),
    )
}

async fn mount_local_llama(server: &MockServer, state: &ProxyState) {
    Mock::given(method("GET"))
        .and(path("/api/tags"))
        .respond_with(ResponseTemplate::new(200).set_body_json(common::ollama_tags_response()))
        .mount(server)
        .await;

    {
        let cfg = state.client_config.read().await.clone();
        state
            .llm_registry
            .inner
            .sync_from_config(&cfg)
            .await
            .unwrap();
        let catalog = state
            .llm_registry
            .inner
            .rebuild_catalog(peer::ollama_peer::gpu_probe_mode())
            .await
            .unwrap();
        let mut app = state.shared_state.lock().await;
        app.local_models = catalog.model_names.clone();
        app.local_models_full = catalog.models.clone();
    }
}

#[tokio::test]
async fn v1_chat_completions_rewrites_prose_prefixed_streaming_tool_call() {
    let server = MockServer::start().await;
    let sse = r#"{"choices":[{"delta":{"content":"run | {\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"},"finish_reason":null}]}
{"choices":[{"delta":{},"finish_reason":"stop"}]}"#;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse),
        )
        .mount(&server)
        .await;

    let state = test_proxy_state_with_backend(&server.uri());
    mount_local_llama(&server, &state).await;

    let mut request = common::cursor_style_request();
    if let Some(obj) = request.as_object_mut() {
        obj.insert("model".to_string(), Value::String("llama3".to_string()));
        obj.insert("stream".to_string(), Value::Bool(true));
    }

    let app = proxy_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&request).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("tool_calls"),
        "expected rewritten tool_calls in SSE, got: {text}"
    );
    assert!(
        !text.contains("\"name\": \"run_in_terminal\"") || text.contains("\"function\""),
        "raw pseudo tool JSON should not be returned as content: {text}"
    );
}

#[tokio::test]
async fn v1_chat_completions_rewrites_prose_prefixed_non_streaming_tool_call() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "I'll verify the build | {\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"cargo build\"}}"
                },
                "finish_reason": "stop"
            }]
        })))
        .mount(&server)
        .await;

    let state = test_proxy_state_with_backend(&server.uri());
    mount_local_llama(&server, &state).await;

    let mut request = common::cursor_style_request();
    if let Some(obj) = request.as_object_mut() {
        obj.insert("model".to_string(), Value::String("llama3".to_string()));
        obj.insert("stream".to_string(), Value::Bool(false));
    }

    let app = proxy_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(serde_json::to_string(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        parsed["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "run_in_terminal"
    );
    assert_eq!(parsed["choices"][0]["finish_reason"], "tool_calls");
}
