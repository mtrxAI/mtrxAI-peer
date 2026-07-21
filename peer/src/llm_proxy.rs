use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    middleware,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::Level;

use crate::agent_compat::{
    apply_ollama_body_defaults_with_default, chat_completion_to_responses,
    chat_response_content_type, log_request_summary, log_response_summary,
    normalize_chat_request_with_default, parse_chat_response_body, rewrite_chat_response,
    AgentDebugSnapshot, AgentResponseFormat, SseTransformState,
};
use crate::client_config::ClientConfig;
use crate::llm_registry::RegistryHandle;
use crate::shared::{NetworkMode, PeerRegistry, ProxyRequestCommand, RuntimeEventTx, SharedState};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn cluster_id_for_model(network_models: &[Value], model: &str) -> Option<String> {
    network_models
        .iter()
        .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(model))
        .and_then(|m| {
            m.get("_cluster_id")
                .or_else(|| {
                    m.get("_clusters")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| {
                            arr.iter()
                                .max_by_key(|c| {
                                    c.get("peer_count").and_then(|v| v.as_u64()).unwrap_or(0)
                                })
                                .and_then(|c| c.get("cluster_id"))
                        })
                })
                .or_else(|| m.get("_room_id"))
                .and_then(|r| r.as_str())
                .map(String::from)
        })
}

fn swarm_id_for_model(network_models: &[Value], model: &str) -> Option<String> {
    network_models
        .iter()
        .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(model))
        .and_then(|m| {
            m.get("_swarm_id")
                .or_else(|| {
                    m.get("_swarms").and_then(|s| s.as_array()).and_then(|arr| {
                        arr.iter()
                            .max_by_key(|s| {
                                s.get("peer_count").and_then(|v| v.as_u64()).unwrap_or(0)
                            })
                            .and_then(|s| s.get("swarm_id"))
                    })
                })
                .and_then(|r| r.as_str())
                .map(String::from)
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkScope {
    Cluster,
    Swarm,
}

fn network_scope_for_model(
    network_models: &[Value],
    model: &str,
    p2p_mode: NetworkMode,
) -> Option<NetworkScope> {
    let has_cluster = cluster_id_for_model(network_models, model).is_some();
    let has_swarm = swarm_id_for_model(network_models, model).is_some();
    match p2p_mode {
        NetworkMode::Cluster => has_cluster.then_some(NetworkScope::Cluster),
        NetworkMode::Swarm => has_swarm.then_some(NetworkScope::Swarm),
        NetworkMode::Both => match (has_cluster, has_swarm) {
            (_, true) => Some(NetworkScope::Swarm),
            (true, false) => Some(NetworkScope::Cluster),
            (false, false) => None,
        },
    }
}

#[derive(Clone)]
pub struct ProxyState {
    pub llm_registry: RegistryHandle,
    pub http_client: Client,
    pub ollama_http_client: Arc<tokio::sync::RwLock<Client>>,
    pub shared_state: SharedState,
    pub agent_debug: Arc<Mutex<AgentDebugSnapshot>>,
    pub client_config: Arc<tokio::sync::RwLock<ClientConfig>>,
    pub lobby_host: String,
    pub proxy_port: u16,
    pub proxy_bind: String,
    pub runtime_event_tx: RuntimeEventTx,
    pub cluster_notify_tx: tokio::sync::mpsc::Sender<()>,
    pub swarm_notify_tx: tokio::sync::mpsc::Sender<()>,
    pub setup_complete: Arc<AtomicBool>,
    pub llm_ready: Arc<AtomicBool>,
    pub cluster_state_version: Arc<AtomicU64>,
    pub tx_store: Arc<crate::tx_db::TxStore>,
    pub peer_stats: crate::peer_stats::PeerStatsTrackerHandle,
    pub peer_registry: PeerRegistry,
    pub peer_moderation_tx: tokio::sync::mpsc::Sender<crate::shared::PeerModerationAction>,
}

impl ProxyState {
    pub fn new(
        shared_state: SharedState,
        client_config: Arc<tokio::sync::RwLock<ClientConfig>>,
        lobby_host: String,
        proxy_port: u16,
        proxy_bind: String,
        runtime_event_tx: RuntimeEventTx,
        cluster_notify_tx: tokio::sync::mpsc::Sender<()>,
        swarm_notify_tx: tokio::sync::mpsc::Sender<()>,
        setup_complete: bool,
        tx_store: Arc<crate::tx_db::TxStore>,
        peer_stats: crate::peer_stats::PeerStatsTrackerHandle,
        peer_registry: PeerRegistry,
        peer_moderation_tx: tokio::sync::mpsc::Sender<crate::shared::PeerModerationAction>,
        bootstrap_config: &ClientConfig,
    ) -> Self {
        let insecure_tls = crate::client_config::effective_ollama_tls_insecure(bootstrap_config);
        let ollama_http_client = crate::lobby_url::build_ollama_http_client(insecure_tls)
            .unwrap_or_else(|_| reqwest::Client::new());
        let ollama_http_client = Arc::new(tokio::sync::RwLock::new(ollama_http_client));
        Self {
            llm_registry: RegistryHandle::new(ollama_http_client.clone(), tx_store.clone()),
            http_client: crate::lobby_url::build_lobby_http_client()
                .unwrap_or_else(|_| reqwest::Client::new()),
            ollama_http_client,
            shared_state,
            agent_debug: Arc::new(Mutex::new(AgentDebugSnapshot::default())),
            client_config,
            lobby_host,
            proxy_port,
            proxy_bind,
            runtime_event_tx,
            cluster_notify_tx,
            swarm_notify_tx,
            setup_complete: Arc::new(AtomicBool::new(setup_complete)),
            llm_ready: Arc::new(AtomicBool::new(false)),
            cluster_state_version: Arc::new(AtomicU64::new(0)),
            tx_store,
            peer_stats,
            peer_registry,
            peer_moderation_tx,
        }
    }

    pub fn bump_cluster_state(&self) {
        self.cluster_state_version.fetch_add(1, Ordering::SeqCst);
    }

    pub fn stats_request_start(&self, req_id: &str, peer_id: &str, model: &str, bytes_sent: u64) {
        if let Ok(mut guard) = self.peer_stats.lock() {
            guard.on_request_start(req_id, peer_id, model, bytes_sent);
        }
    }

    pub fn stats_stream_progress(&self, req_id: &str, bytes_received: u64, partial_tokens: u32) {
        if let Ok(mut guard) = self.peer_stats.lock() {
            guard.on_stream_progress(req_id, bytes_received, partial_tokens);
        }
    }

    pub fn stats_request_complete(&self, req_id: &str, tokens: u32, bytes_received: u64) {
        if let Ok(mut guard) = self.peer_stats.lock() {
            guard.on_request_complete(req_id, tokens, bytes_received);
        }
    }

    pub fn stats_request_error(&self, req_id: &str) {
        if let Ok(mut guard) = self.peer_stats.lock() {
            guard.on_request_error(req_id);
        }
    }

    pub fn stats_remove_peer(&self, peer_id: &str) {
        if let Ok(mut guard) = self.peer_stats.lock() {
            guard.remove_peer(peer_id);
        }
    }

    pub async fn refresh_ollama_tls_client(&self) -> anyhow::Result<()> {
        let (insecure_tls, config) = {
            let cfg = self.client_config.read().await;
            (
                crate::client_config::effective_ollama_tls_insecure(&cfg),
                cfg.clone(),
            )
        };
        let client = crate::lobby_url::build_ollama_http_client(insecure_tls)?;
        *self.ollama_http_client.write().await = client.clone();
        self.llm_registry.replace_http_client(client).await;
        self.llm_registry.inner.sync_from_config(&config).await?;
        Ok(())
    }

    pub async fn backend_url(&self) -> Option<String> {
        self.llm_registry
            .inner
            .first_attached_backend()
            .await
            .map(|b| b.base_url().to_string())
    }

    pub async fn forward_chat_stream(
        &self,
        path: &str,
        body: &serde_json::Value,
        model: &str,
    ) -> Result<
        std::pin::Pin<
            Box<dyn futures_util::Stream<Item = Result<axum::body::Bytes, reqwest::Error>> + Send>,
        >,
        String,
    > {
        let backend = self
            .llm_registry
            .inner
            .resolve_backend(model)
            .await
            .or(self.llm_registry.inner.first_attached_backend().await)
            .ok_or_else(|| "backend not configured".to_string())?;

        // Remote proxy clients send Ollama `/api/chat`. OpenAI-compat backends (inference-cell /
        // llama.cpp) only expose `/v1/chat/completions` — match the local HTTP translation path.
        let translate_ollama_chat = !backend.supports_ollama_native() && path == "/api/chat";

        let (forward_path, forward_body, model_label) = if translate_ollama_chat {
            let mut normalized = body.clone();
            let m_norm = model.strip_suffix(".gguf").unwrap_or(model);
            normalized["model"] = serde_json::Value::String(m_norm.to_string());
            (
                "/v1/chat/completions".to_string(),
                normalized,
                m_norm.to_string(),
            )
        } else {
            (path.to_string(), body.clone(), model.to_string())
        };

        let url = format!("{}{}", backend.base_url(), forward_path);
        println!("Backend POST {} (model={})", url, model_label);
        let res = backend
            .forward_post(&forward_path, &forward_body)
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            let preview: String = text.chars().take(500).collect();
            return Err(format!("upstream {status}: {preview}"));
        }

        let ct = res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let mut stream_up = res.bytes_stream();
        let model_name = model.to_string();

        Ok(Box::pin(async_stream::stream! {
            if !translate_ollama_chat {
                while let Some(item) = stream_up.next().await {
                    yield item;
                }
                return;
            }

            if !ct.contains("text/event-stream") {
                // Non-streaming JSON completion → single Ollama chat object.
                let mut buf = Vec::new();
                while let Some(item) = stream_up.next().await {
                    match item {
                        Ok(chunk) => buf.extend_from_slice(&chunk),
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }
                let json: serde_json::Value =
                    serde_json::from_slice(&buf).unwrap_or(serde_json::Value::Null);
                let content = json
                    .get("choices")
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|c0| c0.get("message"))
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("");
                let out = serde_json::json!({
                    "model": model_name,
                    "created_at": chrono::Utc::now().to_rfc3339(),
                    "message": { "role": "assistant", "content": content },
                    "done": true,
                });
                yield Ok(axum::body::Bytes::from(
                    serde_json::to_vec(&out).unwrap_or_default(),
                ));
                return;
            }

            let mut buf = String::new();
            while let Some(item) = stream_up.next().await {
                let chunk = match item {
                    Ok(c) => c,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(idx) = buf.find('\n') {
                    let line = buf[..idx].trim().to_string();
                    buf = buf[idx + 1..].to_string();
                    if !line.starts_with("data:") {
                        continue;
                    }
                    let payload = line.trim_start_matches("data:").trim();
                    if payload == "[DONE]" {
                        let done = serde_json::json!({"model": model_name, "done": true});
                        yield Ok(axum::body::Bytes::from(
                            serde_json::to_string(&done).unwrap_or_default() + "\n",
                        ));
                        return;
                    }
                    let parsed: serde_json::Value =
                        serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
                    let delta = parsed
                        .get("choices")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|c0| c0.get("delta"))
                        .and_then(|d| d.get("content"))
                        .and_then(|c| c.as_str())
                        .unwrap_or("");
                    if delta.is_empty() {
                        continue;
                    }
                    let out = serde_json::json!({
                        "model": model_name,
                        "message": { "role": "assistant", "content": delta },
                        "done": false
                    });
                    yield Ok(axum::body::Bytes::from(
                        serde_json::to_string(&out).unwrap_or_default() + "\n",
                    ));
                }
            }
            let done = serde_json::json!({"model": model_name, "done": true});
            yield Ok(axum::body::Bytes::from(
                serde_json::to_string(&done).unwrap_or_default() + "\n",
            ));
        }))
    }
}

async fn require_backend_for_model(
    state: &ProxyState,
    model: Option<&str>,
) -> Result<Arc<crate::llm_backend::LlmBackend>, (StatusCode, String)> {
    if !state.setup_complete.load(Ordering::Relaxed) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Setup not complete — open the web UI at /".to_string(),
        ));
    }
    if let Some(model) = model {
        if let Some(backend) = state.llm_registry.inner.resolve_backend(model).await {
            return Ok(backend);
        }
        return Err((
            StatusCode::NOT_FOUND,
            format!("No attached server hosts model '{}'", model),
        ));
    }
    state
        .llm_registry
        .inner
        .first_attached_backend()
        .await
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "No LLM servers attached".to_string(),
        ))
}

async fn require_backend(
    state: &ProxyState,
) -> Result<Arc<crate::llm_backend::LlmBackend>, (StatusCode, String)> {
    require_backend_for_model(state, None).await
}

fn not_ollama_backend() -> (StatusCode, String) {
    (
        StatusCode::NOT_IMPLEMENTED,
        "This endpoint requires an Ollama backend".to_string(),
    )
}

#[derive(Deserialize)]
pub struct HealthQuery {
    #[serde(default)]
    pub check: Option<String>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub llm_url: Option<String>,
    pub llm_backend: Option<String>,
    pub attached_servers: usize,
    pub models_count: Option<usize>,
    pub setup_complete: bool,
    pub llm_ready: bool,
}

pub async fn run_proxy_server(host: &str, port: u16, state: ProxyState) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    if state.setup_complete.load(Ordering::Relaxed) {
        if state.llm_registry.inner.attached_count().await > 0 {
            match state.llm_registry.inner.health_check_attached().await {
                Ok(()) => {
                    let count = state.llm_registry.inner.attached_count().await;
                    state.llm_ready.store(true, Ordering::Relaxed);
                    println!("✅ Proxy connected to {} LLM server(s)", count);
                }
                Err(e) => {
                    state.llm_ready.store(false, Ordering::Relaxed);
                    eprintln!(
                        "⚠️ LLM server(s) unreachable at startup — will retry in background: {}",
                        e
                    );
                }
            }
        }
    } else {
        println!(
            "⏳ Setup incomplete — open http://{}:{}/ to finish configuration",
            host, port
        );
    }

    let addr = format!("{}:{}", host, port);
    let addr: SocketAddr = addr.parse()?;

    let listener = TcpListener::bind(addr).await?;

    println!("\n╔══════════════════════════════════════╗");
    println!("║     mtrxAI Client Server Starting      ║");
    println!("╚══════════════════════════════════════╝\n");
    println!("🌐 Listening on http://{}", addr);
    println!("🖥️  Web UI: http://{}:{}/", host, port);
    if let Some(url) = state.backend_url().await {
        println!("📡 LLM backend: {}", url);
    }

    let ui_state = state.clone();
    let proxy_routes = proxy_router(state);

    let app = crate::api::router(ui_state)
        .merge(proxy_routes)
        .layer(middleware::from_fn(
            crate::security::local_auth::local_proxy_auth_middleware,
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO)),
        );

    println!("✅ Client server is ready!\n");

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("Server error: {}", e))
}

pub fn proxy_router(state: ProxyState) -> Router {
    Router::new()
        .route("/api/tags", get(list_models))
        .route("/api/tags/", get(list_models))
        .route("/api/chat", post(chat_completion))
        .route("/api/generate", post(generate))
        .route("/api/embed", post(embed))
        .route("/api/embeddings", post(embeddings))
        .route("/api/pull", post(pull_model))
        .route("/api/push", post(push_model))
        .route("/api/create", post(create_model))
        .route("/api/delete", post(delete_model))
        .route("/api/show", post(show_model_info))
        .route("/api/version", get(version_info))
        .route("/v1/models", get(v1_models))
        .route("/v1/embeddings", post(v1_embeddings))
        .route("/v1/chat/completions", post(v1_chat_completions))
        .route("/v1/responses", post(v1_responses))
        .route("/debug/agent", get(debug_agent))
        .route("/health", get(health_check))
        .with_state(state)
}

async fn debug_agent(State(state): State<ProxyState>) -> Json<AgentDebugSnapshot> {
    Json(state.agent_debug.lock().await.clone())
}

async fn health_check(
    State(state): State<ProxyState>,
    Query(params): Query<HealthQuery>,
) -> Result<Json<HealthResponse>, (StatusCode, String)> {
    let attached = state.llm_registry.inner.attached_count().await;
    let first_backend = state.llm_registry.inner.first_attached_backend().await;

    let models_count = if params.check.is_none() || params.check.as_deref() == Some("llm") {
        state
            .llm_registry
            .inner
            .merged_catalog()
            .await
            .map(|c| c.model_names.len())
    } else {
        Some(0)
    };

    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        llm_url: first_backend.as_ref().map(|b| b.base_url().to_string()),
        llm_backend: first_backend.as_ref().map(|b| b.kind_str().to_string()),
        attached_servers: attached,
        models_count,
        setup_complete: state.setup_complete.load(Ordering::Relaxed),
        llm_ready: state.llm_ready.load(Ordering::Relaxed),
    }))
}

async fn list_models(State(state): State<ProxyState>) -> Result<Response, (StatusCode, String)> {
    if !state.setup_complete.load(Ordering::Relaxed) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Setup not complete — open the web UI at /".to_string(),
        ));
    }

    let hidden: std::collections::HashSet<String> = {
        let cfg = state.client_config.read().await;
        cfg.hidden_models.iter().cloned().collect()
    };

    let has_backend = state
        .llm_registry
        .inner
        .first_attached_backend()
        .await
        .is_some();

    if !has_backend {
        let st = state.shared_state.lock().await;
        let models: Vec<Value> = st
            .network_models
            .iter()
            .filter(|m| {
                m.get("name")
                    .and_then(|n| n.as_str())
                    .map(|n| !hidden.contains(n))
                    .unwrap_or(false)
            })
            .map(crate::network_catalog::ollama_tag_from_network_model)
            .collect();
        let json = serde_json::json!({ "models": models });
        let final_bytes = serde_json::to_vec(&json).unwrap_or_default();
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(final_bytes.into())
            .unwrap());
    }

    match state.llm_registry.inner.synthesize_merged_tags().await {
        Ok(mut json) => {
            if let Some(models_array) = json.get_mut("models").and_then(|m| m.as_array_mut()) {
                let st = state.shared_state.lock().await;
                let local_names: std::collections::HashSet<_> =
                    st.local_models.iter().cloned().collect();
                let local_by_name: std::collections::HashMap<_, _> = st
                    .local_models_full
                    .iter()
                    .filter_map(|m| {
                        m.get("name")
                            .and_then(|n| n.as_str())
                            .map(|n| (n.to_string(), m.clone()))
                    })
                    .collect();

                for model in models_array.iter_mut() {
                    if let Some(name) = model.get("name").and_then(|n| n.as_str()) {
                        if let Some(enriched) = local_by_name.get(name) {
                            *model = enriched.clone();
                        }
                    }
                }

                for network_model in &st.network_models {
                    if let Some(name) = network_model.get("name").and_then(|n| n.as_str()) {
                        if !local_names.contains(name) {
                            models_array.push(
                                crate::network_catalog::ollama_tag_from_network_model(
                                    network_model,
                                ),
                            );
                        }
                    }
                }

                models_array.retain(|model| {
                    model
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|n| !hidden.contains(n))
                        .unwrap_or(true)
                });
            }

            let final_bytes = serde_json::to_vec(&json).unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(final_bytes.into())
                .unwrap())
        }
        Err(e) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Failed to reach LLM backend: {}", e),
        )),
    }
}

async fn handle_agent_chat_request(
    state: &ProxyState,
    body: String,
    response_format: AgentResponseFormat,
) -> Result<Response, (StatusCode, String)> {
    let default_predict = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_predict(&cfg)
    };
    let default_ctx = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_ctx(&cfg)
    };
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let (normalized_body, mut req_summary) =
        normalize_chat_request_with_default(parsed, default_predict, default_ctx);
    req_summary.response_format = response_format;
    log_request_summary(&req_summary);

    {
        let mut debug = state.agent_debug.lock().await;
        debug.request = Some(req_summary.clone());
        debug.response = None;
    }

    let stream = normalized_body
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);
    let body_json = serde_json::to_string(&normalized_body).unwrap_or(body);

    let mut is_remote = false;
    let mut actual_model = String::new();

    if let Some(model_val) = normalized_body.get("model").and_then(|m| m.as_str()) {
        actual_model = model_val.to_string();
        let lock = state.shared_state.lock().await;
        if !lock.local_models.contains(&actual_model) {
            is_remote = true;
        }
    }

    if is_remote {
        return forward_agent_remote(
            state,
            &actual_model,
            normalized_body,
            stream,
            response_format,
        )
        .await;
    }

    forward_agent_local(state, &body_json, &actual_model, stream, response_format).await
}

async fn dispatch_remote_proxy(
    state: &SharedState,
    client_config: &Arc<tokio::sync::RwLock<ClientConfig>>,
    actual_model: &str,
    path: String,
    body: Value,
) -> Result<tokio::sync::mpsc::Receiver<Result<String, String>>, String> {
    let (scope, cluster_id, swarm_id, p2p_mode) = {
        let lock = state.lock().await;
        let p2p_mode = client_config.read().await.p2p_mode;
        let scope = network_scope_for_model(&lock.network_models, actual_model, p2p_mode)
            .ok_or_else(|| {
                format!("No network advertises model {actual_model} for p2p_mode {p2p_mode:?}")
            })?;
        let cluster_id = if scope == NetworkScope::Cluster {
            cluster_id_for_model(&lock.network_models, actual_model)
        } else {
            None
        };
        let swarm_id = if scope == NetworkScope::Swarm {
            swarm_id_for_model(&lock.network_models, actual_model)
        } else {
            None
        };
        (scope, cluster_id, swarm_id, p2p_mode)
    };

    if crate::security::redact_logs() {
        crate::security::log_redact::remote_proxy_route(actual_model, &format!("{scope:?}"));
    } else {
        println!(
            "🔀 Remote proxy for '{}' via {:?} (p2p_mode={:?})",
            actual_model, scope, p2p_mode
        );
    }

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let cmd = ProxyRequestCommand {
        target_peer: None,
        model: actual_model.to_string(),
        path,
        body,
        response_tx: tx,
        cluster_id,
        swarm_id,
    };

    let req_tx = {
        let lock = state.lock().await;
        match scope {
            NetworkScope::Cluster => lock.proxy_request_tx.clone(),
            NetworkScope::Swarm => lock.swarm_proxy_request_tx.clone(),
        }
    };

    req_tx
        .send(cmd)
        .await
        .map_err(|_| "Failed to route remote proxy request".to_string())?;
    Ok(rx)
}

async fn forward_agent_remote(
    state: &ProxyState,
    actual_model: &str,
    normalized_body: Value,
    stream: bool,
    response_format: AgentResponseFormat,
) -> Result<Response, (StatusCode, String)> {
    if crate::security::redact_logs() {
        crate::security::log_redact::agent_remote_route(actual_model);
    } else {
        println!(
            "🌐 Local model not found: '{}'. Routing agent request to remote peer...",
            actual_model
        );
    }

    let mut rx = dispatch_remote_proxy(
        &state.shared_state,
        &state.client_config,
        actual_model,
        "/v1/chat/completions".to_string(),
        normalized_body,
    )
    .await
    .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;

    if stream {
        let agent_debug = state.agent_debug.clone();
        let stream = async_stream::stream! {
            let mut sse_state = SseTransformState::with_response_format(response_format);
            let mut line_buf = String::new();
            let mut done_sent = false;
            while let Some(res) = rx.recv().await {
                match res {
                    Ok(chunk) => {
                        line_buf.push_str(&chunk);
                        while let Some(pos) = line_buf.find('\n') {
                            let line = line_buf.drain(..=pos).collect::<String>();
                            let trimmed = line.trim_end();
                            if trimmed == "data: [DONE]" {
                                done_sent = true;
                            }
                            for out_line in sse_state.process_line(trimmed) {
                                if out_line.trim() == "data: [DONE]" {
                                    done_sent = true;
                                }
                                yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
                            }
                        }
                    }
                    Err(e) => {
                        yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("data: {{\"error\":\"{}\"}}\n\n", e)));
                        break;
                    }
                }
            }
            if !line_buf.trim().is_empty() {
                let trimmed = line_buf.trim_end().to_string();
                if trimmed == "data: [DONE]" {
                    done_sent = true;
                }
                for out_line in sse_state.process_line(&trimmed) {
                    if out_line.trim() == "data: [DONE]" {
                        done_sent = true;
                    }
                    yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
                }
            }
            for out_line in sse_state.finalize_stream(done_sent) {
                yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
            }
            if let Some(summary) = sse_state.take_summary() {
                log_response_summary(&summary);
                let mut debug = agent_debug.lock().await;
                debug.response = Some(summary);
            }
        };

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", chat_response_content_type(true, None))
            .body(Body::from_stream(stream))
            .unwrap());
    }

    let mut full = String::new();
    while let Some(res) = rx.recv().await {
        match res {
            Ok(chunk) => full.push_str(&chunk),
            Err(e) => {
                return Err((StatusCode::BAD_GATEWAY, e));
            }
        }
    }

    let parsed = parse_chat_response_body(&full);
    let (rewritten, resp_summary) = rewrite_chat_response(parsed);
    log_response_summary(&resp_summary);
    {
        let mut debug = state.agent_debug.lock().await;
        debug.response = Some(resp_summary);
    }

    let output = match response_format {
        AgentResponseFormat::ResponsesApi => chat_completion_to_responses(rewritten),
        AgentResponseFormat::ChatCompletions => rewritten,
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(serde_json::to_vec(&output).unwrap_or_default().into())
        .unwrap())
}

async fn forward_agent_local(
    state: &ProxyState,
    body: &str,
    model: &str,
    stream: bool,
    response_format: AgentResponseFormat,
) -> Result<Response, (StatusCode, String)> {
    let backend = require_backend_for_model(state, Some(model)).await?;
    let path = backend.chat_path_for_model(model);

    let resp = backend
        .forward_post_raw(&path, body.to_string())
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach LLM backend: {}", e),
            )
        })?;

    let status = resp.status().as_u16();
    if status < 200 || status >= 300 {
        let bytes = resp.bytes().await.unwrap_or_default();
        let axum_status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return Err((axum_status, String::from_utf8_lossy(&bytes).into_owned()));
    }

    let upstream_ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if stream {
        let agent_debug = state.agent_debug.clone();
        let byte_stream = resp.bytes_stream();
        let stream = async_stream::stream! {
            let mut sse_state = SseTransformState::with_response_format(response_format);
            let mut line_buf = String::new();
            let mut done_sent = false;
            let mut byte_stream = byte_stream;
            while let Some(chunk_result) = byte_stream.next().await {
                match chunk_result {
                    Ok(bytes) => {
                        line_buf.push_str(&String::from_utf8_lossy(&bytes));
                        while let Some(pos) = line_buf.find('\n') {
                            let line = line_buf.drain(..=pos).collect::<String>();
                            let trimmed = line.trim_end();
                            if trimmed == "data: [DONE]" {
                                done_sent = true;
                            }
                            for out_line in sse_state.process_line(trimmed) {
                                yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        break;
                    }
                }
            }
            if !line_buf.trim().is_empty() {
                for out_line in sse_state.process_line(line_buf.trim_end()) {
                    yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
                }
            }
            for out_line in sse_state.finalize_stream(done_sent) {
                yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{}\n", out_line)));
            }
            if let Some(summary) = sse_state.take_summary() {
                log_response_summary(&summary);
                let mut debug = agent_debug.lock().await;
                debug.response = Some(summary);
            }
        };

        return Ok(Response::builder()
            .status(status)
            .header(
                "Content-Type",
                chat_response_content_type(true, upstream_ct.as_deref()),
            )
            .body(Body::from_stream(stream))
            .unwrap());
    }

    let bytes = resp.bytes().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("Failed to read LLM response: {}", e),
        )
    })?;
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let (rewritten, resp_summary) = rewrite_chat_response(parsed);
    log_response_summary(&resp_summary);
    {
        let mut debug = state.agent_debug.lock().await;
        debug.response = Some(resp_summary);
    }

    let output = match response_format {
        AgentResponseFormat::ResponsesApi => chat_completion_to_responses(rewritten),
        AgentResponseFormat::ChatCompletions => rewritten,
    };

    Ok(Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(serde_json::to_vec(&output).unwrap_or_default().into())
        .unwrap())
}

async fn handle_proxy_request(
    state: &ProxyState,
    path: &str,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let default_predict = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_predict(&cfg)
    };
    let default_ctx = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_ctx(&cfg)
    };
    let body = apply_ollama_body_defaults_with_default(body, default_predict, default_ctx);
    let mut is_remote = false;
    let mut actual_model = String::new();
    let mut is_stream = false;

    if let Ok(req_body) = serde_json::from_str::<serde_json::Value>(&body) {
        is_stream = req_body
            .get("stream")
            .and_then(|s| s.as_bool())
            .unwrap_or(false);
        if let Some(model_val) = req_body.get("model").and_then(|m| m.as_str()) {
            actual_model = model_val.to_string();
            let lock = state.shared_state.lock().await;
            if !lock.local_models.contains(&actual_model) {
                is_remote = true;
            }
        }
    }

    if is_remote {
        println!(
            "🌐 Local model not found: '{}'. Routing to remote peer...",
            actual_model
        );

        let mut rx = dispatch_remote_proxy(
            &state.shared_state,
            &state.client_config,
            &actual_model,
            path.to_string(),
            serde_json::from_str(&body).unwrap_or(serde_json::Value::Null),
        )
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;

        let stream = async_stream::stream! {
            println!("📡 Waiting for remote peer response chunks for model '{}'...", actual_model);
            let mut got_first_chunk = false;
            while let Some(res) = rx.recv().await {
                match res {
                    Ok(chunk) => {
                        if !got_first_chunk {
                            println!("✅ Received first response chunk from remote peer");
                            got_first_chunk = true;
                        }
                        yield Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk));
                    }
                    Err(e) => {
                        println!("❌ Error received from remote peer: {}", e);
                        yield Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("{{\"error\":\"{}\"}}", e)));
                        break;
                    }
                }
            }
            println!("🏁 Finished streaming response from remote peer");
        };

        let stream_body = Body::from_stream(stream);
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/x-ndjson")
            .body(stream_body)
            .unwrap());
    }

    // Route local
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = match model.as_deref() {
        Some(m) => {
            state
                .llm_registry
                .inner
                .resolve_ollama_backend_for_model(m)
                .await
        }
        None => state.llm_registry.inner.first_ollama_backend().await,
    }
    .ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "No LLM servers attached".to_string(),
    ))?;
    match backend.forward_post_raw(path, body).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let upstream_ct = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            if status >= 200 && status < 300 {
                if is_stream {
                    let stream = resp.bytes_stream();
                    let stream_body = Body::from_stream(stream);

                    return Ok(Response::builder()
                        .status(status)
                        .header("Content-Type", "application/x-ndjson")
                        .body(stream_body)
                        .unwrap());
                }

                let bytes = resp.bytes().await.map_err(|e| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("Failed to read LLM response: {}", e),
                    )
                })?;

                let response_ct = if upstream_ct.contains("application/json") {
                    "application/json"
                } else {
                    "application/x-ndjson"
                };
                return Ok(Response::builder()
                    .status(status)
                    .header("Content-Type", response_ct)
                    .body(bytes.into())
                    .unwrap());
            } else {
                let bytes = resp.bytes().await.unwrap_or_default();
                let axum_status =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                Err((axum_status, String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
        Err(e) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Failed to reach LLM backend: {}", e),
        )),
    }
}

async fn chat_completion(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    // `/api/chat` is the Ollama-native chat endpoint. Open WebUI uses this.
    // If the selected backend is OpenAI-compatible (e.g. inference-cell), translate the request
    // to `/v1/chat/completions` and translate the response back to an Ollama-shaped response.

    let parsed =
        serde_json::from_str::<serde_json::Value>(&body).unwrap_or(serde_json::Value::Null);
    let model = parsed
        .get("model")
        .and_then(|m| m.as_str())
        .map(str::to_string);
    let stream = parsed
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);

    // Preserve remote-peer routing, but base it on registry resolution (more reliable than
    // `local_models` during startup / refresh races).
    if let Some(m) = model.as_deref() {
        if state.llm_registry.inner.resolve_backend(m).await.is_none() {
            return handle_proxy_request(&state, "/api/chat", body).await;
        }
    }

    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if backend.supports_ollama_native() {
        return handle_proxy_request(&state, "/api/chat", body).await;
    }

    forward_ollama_chat_via_openai(&state, backend, parsed, model.as_deref(), stream).await
}

async fn forward_ollama_chat_via_openai(
    state: &ProxyState,
    backend: Arc<crate::llm_backend::LlmBackend>,
    ollama_body: serde_json::Value,
    model: Option<&str>,
    stream: bool,
) -> Result<Response, (StatusCode, String)> {
    // Convert minimal Ollama chat request to OpenAI chat request.
    // We keep `messages` and `model`; other Ollama-specific fields are ignored.
    let default_predict = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_predict(&cfg)
    };
    let default_ctx = {
        let cfg = state.client_config.read().await;
        crate::client_config::effective_default_num_ctx(&cfg)
    };

    // Apply defaults + normalize to OpenAI-ish body used elsewhere in the proxy.
    let body_str = apply_ollama_body_defaults_with_default(
        serde_json::to_string(&ollama_body).unwrap_or_default(),
        default_predict,
        default_ctx,
    );
    let mut normalized =
        serde_json::from_str::<serde_json::Value>(&body_str).unwrap_or(serde_json::Value::Null);
    normalized["stream"] = serde_json::Value::Bool(stream);

    // Ollama uses "model" already; OpenAI compat expects it too.
    // For inference-cell/llama.cpp router mode, the model id is usually the preset name
    // (often the filename without `.gguf`). Accept both, but prefer the preset form.
    if let Some(m) = model {
        let m_norm = m.strip_suffix(".gguf").unwrap_or(m);
        normalized["model"] = serde_json::Value::String(m_norm.to_string());
    }

    let body = serde_json::to_string(&normalized).unwrap_or_default();
    let upstream = backend
        .forward_post_raw("/v1/chat/completions", body)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach LLM backend: {e}"),
            )
        })?;

    let status = upstream.status();
    let ct = upstream
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !status.is_success() {
        let text = upstream.text().await.unwrap_or_default();
        let axum_status =
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        // Return JSON so Open WebUI can parse it.
        let body = serde_json::json!({ "error": { "message": text, "type": "upstream_error" } });
        return Ok(Response::builder()
            .status(axum_status)
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default().into())
            .unwrap());
    }

    if !stream {
        let json: serde_json::Value = upstream.json().await.unwrap_or(serde_json::json!({}));
        let content = json
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|c0| c0.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();

        let out = serde_json::json!({
            "model": model.unwrap_or("model"),
            "created_at": chrono::Utc::now().to_rfc3339(),
            "message": { "role": "assistant", "content": content },
            "done": true,
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&out).unwrap_or_default().into())
            .unwrap());
    }

    // Streaming: OpenAI compat returns SSE (`text/event-stream`).
    // Convert to Ollama-style NDJSON stream.
    if !ct.contains("text/event-stream") {
        // Fallback: treat as non-streaming JSON.
        let bytes = upstream.bytes().await.unwrap_or_default();
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(bytes.into())
            .unwrap());
    }

    let mut stream_up = upstream.bytes_stream();
    let model_name = model.unwrap_or("model").to_string();
    let out_stream = async_stream::stream! {
        use futures_util::StreamExt;
        let mut buf = String::new();
        while let Some(item) = stream_up.next().await {
            let chunk = match item {
                Ok(c) => c,
                Err(_) => break,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(idx) = buf.find('\n') {
                let line = buf[..idx].trim().to_string();
                buf = buf[idx+1..].to_string();
                if !line.starts_with("data:") { continue; }
                let payload = line.trim_start_matches("data:").trim();
                if payload == "[DONE]" {
                    let done = serde_json::json!({"model": model_name, "done": true});
                    yield Ok::<_, std::io::Error>(axum::body::Bytes::from(serde_json::to_string(&done).unwrap() + "\n"));
                    return;
                }
                let parsed: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
                let delta = parsed
                    .get("choices")
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|c0| c0.get("delta"))
                    .and_then(|d| d.get("content"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("");
                if delta.is_empty() { continue; }
                let out = serde_json::json!({
                    "model": model_name,
                    "message": { "role": "assistant", "content": delta },
                    "done": false
                });
                yield Ok::<_, std::io::Error>(axum::body::Bytes::from(serde_json::to_string(&out).unwrap() + "\n"));
            }
        }
        let done = serde_json::json!({"model": model_name, "done": true});
        yield Ok::<_, std::io::Error>(axum::body::Bytes::from(serde_json::to_string(&done).unwrap() + "\n"));
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(out_stream))
        .unwrap())
}

async fn generate(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });

    if !state.setup_complete.load(Ordering::Relaxed) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Setup not complete — open the web UI at /".to_string(),
        ));
    }

    let is_remote = match &model {
        Some(m) => {
            let lock = state.shared_state.lock().await;
            !lock.local_models.contains(m)
        }
        None => false,
    };

    if !is_remote {
        let backend = match model.as_deref() {
            Some(m) => {
                state
                    .llm_registry
                    .inner
                    .resolve_ollama_backend_for_model(m)
                    .await
            }
            None => state.llm_registry.inner.first_ollama_backend().await,
        }
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "No LLM servers attached".to_string(),
        ))?;
        if !backend.supports_ollama_native() {
            return Err(not_ollama_backend());
        }
    }

    handle_proxy_request(&state, "/api/generate", body).await
}

async fn forward_local_json(
    state: &ProxyState,
    path: &str,
    body: String,
    model: Option<&str>,
) -> Result<Response, (StatusCode, String)> {
    let backend = require_backend_for_model(state, model).await?;
    match backend.forward_post_raw(path, body).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.unwrap_or_default();
            if status >= 200 && status < 300 {
                Ok(Response::builder()
                    .status(status)
                    .header("Content-Type", "application/json")
                    .body(bytes.into())
                    .unwrap())
            } else {
                let axum_status =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                Err((axum_status, String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
        Err(e) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Failed to reach LLM backend: {}", e),
        )),
    }
}

async fn embed(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if !backend.supports_ollama_native() {
        return Err(not_ollama_backend());
    }
    forward_local_json(&state, "/api/embed", body, model.as_deref()).await
}

async fn embeddings(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    let path = if backend.supports_ollama_native() {
        "/api/embeddings"
    } else {
        "/v1/embeddings"
    };
    forward_local_json(&state, path, body, model.as_deref()).await
}

async fn v1_embeddings(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    forward_local_json(&state, "/v1/embeddings", body, model.as_deref()).await
}

async fn pull_model(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("name")
                .or_else(|| json.get("model"))
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if !backend.supports_ollama_native() {
        return Err(not_ollama_backend());
    }
    forward_local_json(&state, "/api/pull", body, model.as_deref()).await
}

async fn push_model(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("name")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if !backend.supports_ollama_native() {
        return Err(not_ollama_backend());
    }
    forward_local_json(&state, "/api/push", body, model.as_deref()).await
}

async fn create_model(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("name")
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if !backend.supports_ollama_native() {
        return Err(not_ollama_backend());
    }
    forward_local_json(&state, "/api/create", body, model.as_deref()).await
}

async fn delete_model(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let model = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .or_else(|| json.get("name"))
                .and_then(|m| m.as_str())
                .map(str::to_string)
        });
    let backend = require_backend_for_model(&state, model.as_deref()).await?;
    if !backend.supports_ollama_native() {
        return Err(not_ollama_backend());
    }
    let model_name = model.as_deref().unwrap_or("");
    match backend.forward_delete_raw("/api/delete", model_name).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.unwrap_or_default();
            if status >= 200 && status < 300 {
                Ok(Response::builder()
                    .status(status)
                    .header("Content-Type", "application/json")
                    .body(bytes.into())
                    .unwrap())
            } else {
                let axum_status =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                Err((axum_status, String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
        Err(e) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Failed to reach LLM backend: {}", e),
        )),
    }
}

async fn show_model_info(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    let body_json: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::json!({}));
    let requested_model = body_json
        .get("name")
        .or_else(|| body_json.get("model"))
        .and_then(|n| n.as_str())
        .unwrap_or("");

    // Check if the model is remote
    let is_remote = {
        let st = state.shared_state.lock().await;
        !st.local_models.contains(&requested_model.to_string())
            && st
                .network_models
                .iter()
                .any(|m| m.get("name").and_then(|n| n.as_str()) == Some(requested_model))
    };

    if is_remote {
        let network_model = {
            let st = state.shared_state.lock().await;
            st.network_models
                .iter()
                .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(requested_model))
                .cloned()
        };

        if let Some(network_model) = network_model {
            let mut show_info = network_model
                .get("_show_info")
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or_else(|| crate::network_catalog::network_model_show_info(&network_model));
            if let Some(details) = show_info.get_mut("details").and_then(|d| d.as_object_mut()) {
                details.insert(
                    "format".to_string(),
                    serde_json::Value::String("remote".to_string()),
                );
            }

            let bytes = serde_json::to_vec(&show_info).unwrap_or_default();
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(bytes.into())
                .unwrap());
        }

        return Err((
            StatusCode::NOT_FOUND,
            "Model info not available for remote model".to_string(),
        ));
    }

    let backend = state
        .llm_registry
        .inner
        .resolve_ollama_backend_for_model(requested_model)
        .await
        .or(state.llm_registry.inner.first_attached_backend().await)
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "No LLM servers attached".to_string(),
        ))?;
    if backend.supports_ollama_native() {
        forward_local_json(&state, "/api/show", body, Some(requested_model)).await
    } else {
        let st = state.shared_state.lock().await;
        if let Some(model) = st
            .local_models_full
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(requested_model))
        {
            let bytes = serde_json::to_vec(model).unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(bytes.into())
                .unwrap())
        } else {
            Err((
                StatusCode::NOT_FOUND,
                format!("Model {} not found", requested_model),
            ))
        }
    }
}

async fn version_info(State(state): State<ProxyState>) -> Result<Response, (StatusCode, String)> {
    let backend = require_backend(&state).await?;
    if backend.supports_ollama_native() {
        match backend.forward_get("/api/version").await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let bytes = resp.bytes().await.unwrap_or_default();
                if status >= 200 && status < 300 {
                    Ok(Response::builder()
                        .status(status)
                        .header("Content-Type", "application/json")
                        .body(bytes.into())
                        .unwrap())
                } else {
                    let axum_status =
                        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                    Err((axum_status, String::from_utf8_lossy(&bytes).into_owned()))
                }
            }
            Err(e) => Err((
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach LLM backend: {}", e),
            )),
        }
    } else {
        let body = serde_json::json!({ "version": backend.kind_str() });
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default().into())
            .unwrap())
    }
}

async fn v1_models(State(state): State<ProxyState>) -> Result<Response, (StatusCode, String)> {
    if !state.setup_complete.load(Ordering::Relaxed) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Setup not complete — open the web UI at /".to_string(),
        ));
    }

    let hidden: std::collections::HashSet<String> = {
        let cfg = state.client_config.read().await;
        cfg.hidden_models.iter().cloned().collect()
    };

    let has_backend = state
        .llm_registry
        .inner
        .first_attached_backend()
        .await
        .is_some();

    if !has_backend {
        let st = state.shared_state.lock().await;
        let mut data = Vec::new();
        for network_model in &st.network_models {
            let Some(name) = network_model.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            if hidden.contains(name) {
                continue;
            }
            let owned_by = crate::network_catalog::network_scope_label(network_model)
                .map(|scope| match scope {
                    "swarm" => "swarm",
                    "cluster" => "cluster",
                    "both" => "network",
                    _ => "community",
                })
                .unwrap_or("community");
            let mut remote_model = serde_json::json!({
                "id": name,
                "name": name,
                "object": "model",
                "created": 0,
                "owned_by": owned_by,
            });
            crate::network_catalog::enrich_openai_remote_model(&mut remote_model, network_model);
            data.push(remote_model);
        }
        let json = serde_json::json!({ "object": "list", "data": data });
        let final_bytes = serde_json::to_vec(&json).unwrap_or_default();
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(final_bytes.into())
            .unwrap());
    }

    match state.llm_registry.inner.synthesize_merged_v1_models().await {
        Ok(mut json) => {
            if let Some(data_array) = json.get_mut("data").and_then(|d| d.as_array_mut()) {
                let st = state.shared_state.lock().await;
                let local_names: std::collections::HashSet<_> =
                    st.local_models.iter().cloned().collect();

                for network_model in &st.network_models {
                    if let Some(name) = network_model.get("name").and_then(|n| n.as_str()) {
                        if !local_names.contains(name) {
                            let owned_by =
                                crate::network_catalog::network_scope_label(network_model)
                                    .map(|scope| match scope {
                                        "swarm" => "swarm",
                                        "cluster" => "cluster",
                                        "both" => "network",
                                        _ => "community",
                                    })
                                    .unwrap_or("community");
                            let mut remote_model = serde_json::json!({
                                "id": name,
                                "name": name,
                                "object": "model",
                                "created": 0,
                                "owned_by": owned_by,
                            });
                            crate::network_catalog::enrich_openai_remote_model(
                                &mut remote_model,
                                network_model,
                            );
                            data_array.push(remote_model);
                        }
                    }
                }

                for local_model in &st.local_models_full {
                    if let Some(name) = local_model.get("name").and_then(|n| n.as_str()) {
                        for entry in data_array.iter_mut() {
                            if entry.get("id").and_then(|v| v.as_str()) == Some(name) {
                                if let Some(status) = local_model.get("_status") {
                                    entry["_status"] = status.clone();
                                }
                                break;
                            }
                        }
                    }
                }

                data_array.retain(|entry| {
                    let name = entry
                        .get("id")
                        .and_then(|v| v.as_str())
                        .or_else(|| entry.get("name").and_then(|v| v.as_str()));
                    name.map(|n| !hidden.contains(n)).unwrap_or(true)
                });
            }

            let final_bytes = serde_json::to_vec(&json).unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(final_bytes.into())
                .unwrap())
        }
        Err(e) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Failed to reach LLM backend: {}", e),
        )),
    }
}

async fn v1_chat_completions(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    handle_agent_chat_request(&state, body, AgentResponseFormat::ChatCompletions).await
}

async fn v1_responses(
    State(state): State<ProxyState>,
    body: String,
) -> Result<Response, (StatusCode, String)> {
    handle_agent_chat_request(&state, body, AgentResponseFormat::ResponsesApi).await
}
