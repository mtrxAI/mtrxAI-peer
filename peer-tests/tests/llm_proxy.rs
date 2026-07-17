mod common;

use peer::client_config::{ClientConfig, LlmServerEntry};
use peer::llm_proxy::{proxy_router, run_proxy_server, ProxyState};
use common::test_app_state;
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tower::ServiceExt;

fn test_proxy_state_with_llm_url(setup_complete: bool, llm_url: &str) -> ProxyState {
    let (proxy_cmd_tx, _) = mpsc::channel(1);
    let (runtime_event_tx, room_notify_tx, swarm_notify_tx) = common::test_notify_channels();
    let shared_state = Arc::new(Mutex::new(test_app_state(proxy_cmd_tx)));
    let config = Arc::new(RwLock::new(ClientConfig {
        setup_complete,
        llm_servers: vec![LlmServerEntry {
            id: "test-ollama".to_string(),
            kind: "ollama".to_string(),
            url: llm_url.to_string(),
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
        shared_state,
        config,
        "127.0.0.1:8080".to_string(),
        11345,
        "127.0.0.1".to_string(),
        runtime_event_tx,
        room_notify_tx,
        swarm_notify_tx,
        setup_complete,
        common::test_tx_store(),
        common::test_peer_stats(),
        common::test_peer_registry(),
        common::test_moderation_channel(),
    )
}

fn test_proxy_state(setup_complete: bool) -> ProxyState {
    test_proxy_state_with_llm_url(setup_complete, "http://127.0.0.1:11434")
}

#[tokio::test]
async fn health_returns_ok_without_backend() {
    let app = proxy_router(test_proxy_state(false));
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["setup_complete"], false);
    assert!(json["llm_url"].is_null());
}

#[tokio::test]
async fn health_reports_setup_complete() {
    let app = proxy_router(test_proxy_state(true));
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["setup_complete"], true);
    assert_eq!(json["attached_servers"], 0);
    assert_eq!(json["llm_ready"], false);
}

#[tokio::test]
async fn proxy_starts_when_attached_llm_unreachable() {
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let state = test_proxy_state_with_llm_url(true, "http://127.0.0.1:61999");
    {
        let cfg = state.client_config.read().await.clone();
        state
            .llm_registry
            .inner
            .sync_from_config(&cfg)
            .await
            .unwrap();
    }

    let server = tokio::spawn(async move {
        run_proxy_server("127.0.0.1", port, state)
            .await
            .expect("proxy should start even when LLM is down");
    });

    let url = format!("http://127.0.0.1:{port}/health");
    let client = reqwest::peer::new();
    let mut ready = false;
    for _ in 0..20 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                ready = true;
                let json: Value = resp.json().await.unwrap();
                assert_eq!(json["status"], "ok");
                assert_eq!(json["llm_ready"], false);
                assert!(json["attached_servers"].as_u64().unwrap_or(0) > 0);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    server.abort();
    assert!(ready, "expected /health to respond while attached LLM is unreachable");
}

#[tokio::test]
async fn api_chat_routes_network_only_model_without_404() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/tags"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(common::ollama_tags_response()),
        )
        .mount(&server)
        .await;

    let (proxy_cmd_tx, mut proxy_cmd_rx) =
        mpsc::channel::<peer::shared::ProxyRequestCommand>(1);
    tokio::spawn(async move {
        while let Some(cmd) = proxy_cmd_rx.recv().await {
            if cmd.model == "remote-model" {
                let _ = cmd
                    .response_tx
                    .send(Ok(
                        r#"{"message":{"role":"assistant","content":"remote-ok"}}"#.to_string(),
                    ))
                    .await;
            }
        }
    });

    let shared_state = Arc::new(Mutex::new(common::test_app_state(proxy_cmd_tx)));
    let config = Arc::new(RwLock::new(ClientConfig {
        setup_complete: true,
        llm_servers: vec![LlmServerEntry {
            id: "test-ollama".to_string(),
            kind: "ollama".to_string(),
            url: server.uri(),
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
    let (runtime_event_tx, room_notify_tx, swarm_notify_tx) = common::test_notify_channels();
    let state = ProxyState::new(
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
    );

    {
        let cfg = config.read().await.clone();
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
        let mut app = shared_state.lock().await;
        app.local_models = catalog.model_names.clone();
        app.local_models_full = catalog.models.clone();
        app.network_models = vec![serde_json::json!({
            "name": "remote-model",
            "_cluster_id": "cluster-1"
        })];
    }

    let app = proxy_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/chat")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::to_string(&serde_json::json!({
                        "model": "remote-model",
                        "messages": [{ "role": "user", "content": "hi" }],
                        "stream": false
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), axum::http::StatusCode::NOT_FOUND);
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["message"]["content"], "remote-ok");
}
