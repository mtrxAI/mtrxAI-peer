use peer::shared::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub fn test_tx_store() -> Arc<peer::tx_db::TxStore> {
    Arc::new(peer::tx_db::TxStore::open_in_memory().unwrap())
}

pub fn test_peer_registry() -> peer::shared::PeerRegistry {
    Arc::new(tokio::sync::Mutex::new(HashMap::new()))
}

pub fn test_peer_stats() -> peer::peer_stats::PeerStatsTrackerHandle {
    peer::peer_stats::PeerStatsTrackerHandle::new(std::sync::Mutex::new(
        peer::peer_stats::PeerStatsTracker::new(),
    ))
}

pub fn test_moderation_channel() -> mpsc::Sender<peer::shared::PeerModerationAction> {
    let (tx, _rx) = mpsc::channel(8);
    tx
}

pub fn test_notify_channels() -> (
    peer::shared::RuntimeEventTx,
    tokio::sync::mpsc::Sender<()>,
    tokio::sync::mpsc::Sender<()>,
) {
    let (runtime_tx, _rx) = mpsc::channel(8);
    let (cluster_tx, _crx) = mpsc::channel(8);
    let (swarm_tx, _srx) = mpsc::channel(8);
    (runtime_tx, cluster_tx, swarm_tx)
}

pub fn test_app_state(proxy_cmd_tx: mpsc::Sender<peer::shared::ProxyRequestCommand>) -> AppState {
    let (model_start_tx, _) = mpsc::channel(1);
    let (swarm_proxy_tx, _) = mpsc::channel(1);
    let (swarm_model_start_tx, _) = mpsc::channel(1);
    let (connection_tx, _) = mpsc::channel(1);
    AppState {
        my_id: "peer-1".to_string(),
        peer_id: "peer-1".to_string(),
        service_id: "svc-1".to_string(),
        clusters: Vec::new(),
        swarms: Vec::new(),
        credit_balance: 0,
        local_models: Vec::new(),
        local_models_full: Vec::new(),
        network_models: Vec::new(),
        proxy_request_tx: proxy_cmd_tx,
        model_start_tx,
        swarm_proxy_request_tx: swarm_proxy_tx,
        swarm_model_start_tx,
        connection_tx,
        outgoing_model_requests: Vec::new(),
        incoming_model_offers: Vec::new(),
        incoming_connection_offers: Vec::new(),
        local_model_runs: Vec::new(),
        local_model_collisions: Vec::new(),
        peer_info: None,
        last_advertised_hash: None,
        show_info_cache: HashMap::new(),
        last_gpu_host: None,
        gpu_history: Default::default(),
        lobby_connected: false,
        peer_connections: Vec::new(),
        gpu_thermal_guard: Default::default(),
    }
}

pub fn cursor_style_request() -> Value {
    json!({
        "model": "llama3.2",
        "input": [{"role": "user", "content": "start containers"}],
        "stream": false,
        "store": true,
        "tools": [{
            "type": "function",
            "name": "run_in_terminal",
            "description": "Run command",
            "parameters": {
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
                "additionalProperties": false
            }
        }]
    })
}

pub fn text_tool_response() -> Value {
    json!({
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "{\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"docker-compose up -d\"}}"
            },
            "finish_reason": "stop"
        }]
    })
}

pub fn ollama_tags_response() -> Value {
    json!({
        "models": [
            {"name": "llama3", "modified_at": "2024-01-01T00:00:00Z"}
        ]
    })
}
