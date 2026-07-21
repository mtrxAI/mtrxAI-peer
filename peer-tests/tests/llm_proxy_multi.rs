use http_body_util::BodyExt;
use peer::client_config::{ClientConfig, LlmServerEntry};
use peer::llm_proxy::{proxy_router, ProxyState};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

async fn proxy_with_two_backends() -> (ProxyState, MockServer, MockServer) {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{ "id": "model-a", "object": "model" }]
        })))
        .mount(&server_a)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{ "id": "model-b", "object": "model" }]
        })))
        .mount(&server_b)
        .await;

    let (proxy_cmd_tx, _) = mpsc::channel(1);
    let (runtime_event_tx, room_notify_tx, swarm_notify_tx) = common::test_notify_channels();
    let shared_state = Arc::new(Mutex::new(common::test_app_state(proxy_cmd_tx)));

    let config = ClientConfig {
        setup_complete: true,
        llm_servers: vec![
            LlmServerEntry {
                id: "a".to_string(),
                kind: "vllm".to_string(),
                url: server_a.uri(),
                label: None,
                attached: true,
                order: 0,
                source: "test".to_string(),
                api_type: None,
                models: Vec::new(),
                advertise_to_cluster: true,
            },
            LlmServerEntry {
                id: "b".to_string(),
                kind: "tensorrt".to_string(),
                url: server_b.uri(),
                label: None,
                attached: true,
                order: 1,
                source: "test".to_string(),
                api_type: None,
                models: Vec::new(),
                advertise_to_cluster: true,
            },
        ],
        ..Default::default()
    };

    let state = ProxyState::new(
        shared_state,
        Arc::new(RwLock::new(config.clone())),
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
        &config,
    );

    state
        .llm_registry
        .inner
        .sync_from_config(&config)
        .await
        .unwrap();
    let catalog = state
        .llm_registry
        .inner
        .rebuild_catalog(peer::ollama_client::gpu_probe_mode())
        .await
        .unwrap();
    {
        let mut app = state.shared_state.lock().await;
        app.local_models = catalog.model_names.clone();
        app.local_models_full = catalog.models.clone();
    }

    (state, server_a, server_b)
}

#[tokio::test]
async fn merged_v1_models_includes_both_backends() {
    let (state, _a, _b) = proxy_with_two_backends().await;
    let app = proxy_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/v1/models")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ids: Vec<_> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m.get("id").and_then(|v| v.as_str()))
        .collect();
    assert!(ids.contains(&"model-a"));
    assert!(ids.contains(&"model-b"));
}

#[tokio::test]
async fn resolve_backend_routes_per_model() {
    let (state, server_a, server_b) = proxy_with_two_backends().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{ "message": { "role": "assistant", "content": "from-a" } }]
        })))
        .expect(1)
        .mount(&server_a)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{ "message": { "role": "assistant", "content": "from-b" } }]
        })))
        .expect(1)
        .mount(&server_b)
        .await;

    let app = proxy_router(state);
    for (model, expected) in [("model-a", "from-a"), ("model-b", "from-b")] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::to_string(&json!({
                            "model": model,
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let content = parsed["choices"][0]["message"]["content"].as_str().unwrap();
        assert_eq!(content, expected);
    }
}
