mod common;

use http_body_util::BodyExt;
use peer::client_config::{ClientConfig, CustomModelEntry, LlmServerEntry};
use peer::llm_proxy::{proxy_router, ProxyState};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tower::ServiceExt;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn custom_proxy_state(server: &MockServer, api_key: Option<&str>) -> ProxyState {
    let tx_store = common::test_tx_store();
    if let Some(key) = api_key {
        tx_store
            .put_server_api_key("custom-1", key, Some("peer-1"), Some("svc-1"))
            .await
            .unwrap();
    }

    let (proxy_cmd_tx, _) = mpsc::channel(1);
    let (runtime_event_tx, room_notify_tx, swarm_notify_tx) = common::test_notify_channels();
    let shared_state = Arc::new(Mutex::new(common::test_app_state(proxy_cmd_tx)));

    let config = ClientConfig {
        setup_complete: true,
        peer_id: Some("peer-1".to_string()),
        service_id: Some("svc-1".to_string()),
        llm_servers: vec![LlmServerEntry {
            id: "custom-1".to_string(),
            kind: "custom".to_string(),
            url: server.uri(),
            label: Some("Test custom".to_string()),
            attached: true,
            order: 0,
            source: "test".to_string(),
            api_type: Some("chat-completions".to_string()),
            models: vec![CustomModelEntry {
                id: "my-model".to_string(),
                name: Some("My Model".to_string()),
                url: None,
                tool_calling: Some(true),
                vision: Some(false),
                max_input_tokens: Some(128000),
                max_output_tokens: Some(16000),
            }],
            advertise_to_cluster: true,
        }],
        ..Default::default()
    };

    let state = ProxyState::new(
        shared_state.clone(),
        Arc::new(RwLock::new(config.clone())),
        "127.0.0.1:8080".to_string(),
        11345,
        "127.0.0.1".to_string(),
        runtime_event_tx,
        room_notify_tx,
        swarm_notify_tx,
        true,
        tx_store,
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
        let mut app = shared_state.lock().await;
        app.local_models = catalog.model_names.clone();
        app.local_models_full = catalog.models.clone();
    }

    state
}

#[tokio::test]
async fn custom_static_models_listed_without_upstream_models_endpoint() {
    let server = MockServer::start().await;
    let state = custom_proxy_state(&server, None).await;
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
    assert!(ids.contains(&"my-model"));
}

#[tokio::test]
async fn custom_chat_forwards_with_bearer_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer sk-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{ "message": { "role": "assistant", "content": "custom-ok" } }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let state = custom_proxy_state(&server, Some("sk-secret")).await;
    let app = proxy_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&json!({
                        "model": "my-model",
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
    assert_eq!(parsed["choices"][0]["message"]["content"], "custom-ok");
}

#[tokio::test]
async fn attach_rejects_custom_without_models() {
    let registry = peer::llm_registry::LlmServerRegistry::new(
        Arc::new(RwLock::new(reqwest::Client::new())),
        common::test_tx_store(),
    );
    let mut cfg = ClientConfig::default();
    cfg.llm_servers.push(LlmServerEntry {
        id: "empty-custom".to_string(),
        kind: "custom".to_string(),
        url: "http://127.0.0.1:9999".to_string(),
        label: None,
        attached: false,
        order: 0,
        source: "test".to_string(),
        api_type: Some("chat-completions".to_string()),
        models: Vec::new(),
        advertise_to_cluster: true,
    });

    let err = registry.attach(&mut cfg, "empty-custom").await.unwrap_err();
    assert!(err.to_string().contains("at least one model"));
}

#[tokio::test]
async fn removing_server_deletes_stored_api_key() {
    let tx_store = common::test_tx_store();
    tx_store
        .put_server_api_key("srv-x", "key-123", Some("peer"), Some("svc"))
        .await
        .unwrap();
    assert!(tx_store.has_server_api_key("srv-x").await.unwrap());

    let registry = peer::llm_registry::LlmServerRegistry::new(
        Arc::new(RwLock::new(reqwest::Client::new())),
        tx_store.clone(),
    );
    let mut cfg = ClientConfig::default();
    cfg.llm_servers.push(LlmServerEntry {
        id: "srv-x".to_string(),
        kind: "custom".to_string(),
        url: "http://127.0.0.1:1".to_string(),
        label: None,
        attached: false,
        order: 0,
        source: "test".to_string(),
        api_type: None,
        models: vec![CustomModelEntry {
            id: "m1".to_string(),
            name: None,
            url: None,
            tool_calling: None,
            vision: None,
            max_input_tokens: None,
            max_output_tokens: None,
        }],
        advertise_to_cluster: true,
    });

    registry.remove_server(&mut cfg, "srv-x").await.unwrap();
    assert!(!tx_store.has_server_api_key("srv-x").await.unwrap());
}
