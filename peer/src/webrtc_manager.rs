use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use crate::ollama_client::{catalog_hash, gpu_probe_mode, model_poll_interval_secs, GpuProbeMode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::client_config::{cluster_accepts_jobs, cluster_is_connected, find_cluster};
use crate::network_scheduler::cluster_schedule_fields;
use crate::connect_allowance::ConnectionAllowanceStore;
use crate::shared::{
    peer_registry_key, ClusterStatus, ConnectionAction, GpuHostStatus, IncomingConnectionOfferState,
    ModelStartAction, ModelStartOfferState, ModelStartRequestState, PeerDirection, PeerInfo,
    PeerModerationAction, PeerRegistry, ProxyRequestCommand, SharedState, TrackedPeer,
};
use crate::agent_compat::{
    ensure_ollama_ctx_options_with_default, ensure_ollama_predict_options_with_default,
    ensure_stream_usage,
};
use crate::token_usage::{accumulate_and_parse_usage, parse_usage_from_buffer, TokenUsage};

static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(1);
const MAINTENANCE_CATALOG_HASH: &str = "__maintenance__";

/// Conservative wire-size cap (webrtc-rs on_message receive limit is 16 KiB).
const MAX_DC_WIRE_BYTES: usize = 8 * 1024;
/// Raw JSON body bytes per chunk; leave headroom for JSON envelope + escaping.
const MAX_DC_CHUNK_BYTES: usize = 3 * 1024;
/// Brief pause after `Open` before the first SCTP send (avoids early-write races).
const DC_OPEN_SETTLE_MS: u64 = 150;
const CONNECT_APPROVAL_TIMEOUT_SECS: u64 = 65;
const CONNECTION_ALLOWANCE_TTL_SECS: u64 = 60;
/// Poll interval while waiting for SCTP data channel `Open` after ICE connects.
const DC_OPEN_POLL_MS: u64 = 500;
/// Max wait for data channel (Docker ICE can be slow).
const DC_OPEN_MAX_POLLS: u32 = 60;

struct PendingIncomingProxyRequest {
    path: String,
    chunks: Vec<(u32, String)>,
}

struct ProxyExchangeMeta {
    remote_peer_id: String,
    model: String,
    path: String,
    started_at: std::time::Instant,
    response_buffer: String,
    bytes_sent: u64,
    bytes_received: u64,
}

fn chunk_utf8(s: &str, max_bytes: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < s.len() {
        let mut end = (start + max_bytes).min(s.len());
        while end > start && !s.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = (start + 1).min(s.len());
            while end < s.len() && !s.is_char_boundary(end) {
                end += 1;
            }
        }
        out.push(s[start..end].to_string());
        start = end;
    }
    out
}

async fn send_dc_message(dc: &RTCDataChannel, msg: &DataChannelMessage) -> anyhow::Result<()> {
    let text = serde_json::to_string(msg)?;
    if text.len() > MAX_DC_WIRE_BYTES {
        anyhow::bail!(
            "internal error: serialized message is {} bytes (limit {})",
            text.len(),
            MAX_DC_WIRE_BYTES
        );
    }
    dc.send_text(text).await?;
    Ok(())
}

async fn send_proxy_request(
    dc: &RTCDataChannel,
    req_id: &str,
    path: &str,
    body: &serde_json::Value,
) -> anyhow::Result<()> {
    let body_str = serde_json::to_string(body)?;
    let chunks = chunk_utf8(&body_str, MAX_DC_CHUNK_BYTES);
    crate::security::log_redact::proxy_request_wire(
        req_id,
        body_str.len(),
        chunks.len(),
        MAX_DC_CHUNK_BYTES,
    );

    send_dc_message(
        dc,
        &DataChannelMessage::ProxyRequestStart {
            req_id: req_id.to_string(),
            path: path.to_string(),
        },
    )
    .await?;

    for (seq, chunk) in chunks.into_iter().enumerate() {
        send_dc_message(
            dc,
            &DataChannelMessage::ProxyRequestChunk {
                req_id: req_id.to_string(),
                seq: seq as u32,
                chunk,
            },
        )
        .await?;
    }

    send_dc_message(
        dc,
        &DataChannelMessage::ProxyRequestEnd {
            req_id: req_id.to_string(),
        },
    )
    .await?;

    Ok(())
}

async fn send_proxy_response_chunk(
    dc: &RTCDataChannel,
    req_id: &str,
    chunk: &str,
) -> anyhow::Result<()> {
    for piece in chunk_utf8(chunk, MAX_DC_CHUNK_BYTES) {
        send_dc_message(
            dc,
            &DataChannelMessage::ProxyResponseChunk {
                req_id: req_id.to_string(),
                chunk: piece,
            },
        )
        .await?;
    }
    Ok(())
}

fn peer_connection_allowed(allow_unattested_peers: bool, peer_flags: u64) -> bool {
    if allow_unattested_peers {
        return true;
    }
    (peer_flags & mtrxai_attestation::ATTESTATION_MTRXAI_BUILD) != 0
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ProtocolMessage {
    Registered {
        name: String,
        #[serde(rename = "cluster_id", alias = "room_id")]
        cluster_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "cluster_name", alias = "room_name")]
        cluster_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required_attestation_flags: Option<u64>,
    },
    UpdatePeerInfo {
        lat: f64,
        lon: f64,
        asn: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        city: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        country: Option<String>,
    },
    UpdateModels {
        models: Vec<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_host: Option<GpuHostStatus>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        accepting_jobs: Option<bool>,
    },
    AvailableModels {
        models: Vec<serde_json::Value>,
    },
    GetPeersForModel {
        req_id: String,
        model: String,
    },
    PeersForModel {
        req_id: String,
        model: String,
        peers: Vec<String>,
    },
    RequestPeerConnect {
        req_id: String,
        model: String,
    },
    ConnectOffer {
        req_id: String,
        model: String,
        requested_by: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requested_by_attestation_flags: Option<u64>,
    },
    RespondConnectOffer {
        req_id: String,
        accept: bool,
    },
    ConnectUpdate {
        req_id: String,
        model: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_peer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_attestation_flags: Option<u64>,
    },
    Route {
        to: String,
        from: String,
        payload: serde_json::Value,
    },
    ReportTokenUsage {
        req_id: String,
        role: String,
        peer_id: String,
        remote_peer_id: String,
        model: String,
        path: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
        bytes_sent: u64,
        bytes_received: u64,
        duration_ms: u64,
    },
    RequestModelStart {
        req_id: String,
        model: String,
        #[serde(rename = "cluster_id", alias = "room_id")]
        cluster_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_host: Option<GpuHostStatus>,
    },
    ModelStartUpdate {
        req_id: String,
        model: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress_pct: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_peer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peers: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ModelStartOffer {
        req_id: String,
        model: String,
        requested_by: String,
        estimated_vram_mb: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk_size_mb: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_on_requester: Option<bool>,
    },
    RespondModelStart {
        req_id: String,
        accept: bool,
    },
    ReportModelStartProgress {
        req_id: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress_pct: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ReportPeer {
        target_peer_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    PeerReportAck {
        target_peer_id: String,
        report_count: u32,
        banned: bool,
    },
    PeerBanned {
        peer_id: String,
        report_count: u32,
    },
}

/// Messages sent over the WebRTC data channel for remote LLM chat protocol
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DataChannelMessage {
    /// Proxy request payload (from client proxy to server; used when under size limit)
    ProxyRequest {
        req_id: String,
        path: String,
        body: serde_json::Value,
    },
    /// Start of a chunked proxy request (large agent payloads)
    ProxyRequestStart {
        req_id: String,
        path: String,
    },
    /// Chunk of a chunked proxy request body (JSON string fragments)
    ProxyRequestChunk {
        req_id: String,
        seq: u32,
        chunk: String,
    },
    /// End of a chunked proxy request
    ProxyRequestEnd {
        req_id: String,
    },
    /// Proxy response chunk (from server to client proxy)
    ProxyResponseChunk { req_id: String, chunk: String },
    /// Proxy response complete
    ProxyResponseDone { req_id: String },
    /// Proxy response error
    ProxyResponseError { req_id: String, error: String },
    /// Keepalive / ping
    Ping,
    /// Keepalive / pong
    Pong,
}

pub struct PeerState {
    pub pc: Arc<RTCPeerConnection>,
    pub active_data_channel: Option<Arc<RTCDataChannel>>,
}

fn data_channel_is_open(dc: &Arc<RTCDataChannel>) -> bool {
    dc.ready_state() == RTCDataChannelState::Open
}

fn peer_connection_is_terminal(pc: &RTCPeerConnection) -> bool {
    matches!(
        pc.connection_state(),
        RTCPeerConnectionState::Failed
            | RTCPeerConnectionState::Closed
            | RTCPeerConnectionState::Disconnected
    )
}

async fn peer_data_channel_ready(ps_arc: &Arc<Mutex<PeerState>>) -> bool {
    let ps = ps_arc.lock().await;
    ps.active_data_channel
        .as_ref()
        .is_some_and(data_channel_is_open)
}

fn payload_cluster_matches(payload: &serde_json::Value, cluster_id: &str) -> bool {
    payload
        .get("cluster_id").or_else(|| payload.get("room_id"))
        .and_then(|v| v.as_str())
        .map(|id| id == cluster_id)
        .unwrap_or(false)
}

fn ws_send_disconnected(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("Sending after closing") || msg.contains("Connection reset") || msg.contains("broken pipe")
}

fn url_encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(out, "%{:02X}", b);
            }
        }
    }
    out
}

async fn send_progress(
    ws_write: &Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >>>,
    req_id: &str,
    status: &str,
    progress_pct: Option<u8>,
    message: Option<String>,
) -> anyhow::Result<()> {
    let msg = ProtocolMessage::ReportModelStartProgress {
        req_id: req_id.to_string(),
        status: status.to_string(),
        progress_pct,
        message,
    };
    let text = serde_json::to_string(&msg)?;
    ws_write.lock().await.send(Message::Text(text)).await?;
    Ok(())
}

async fn execute_model_start_task(
    ws_write: Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >>>,
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    _cluster_id: String,
    _gpu_probe: GpuProbeMode,
    req_id: &str,
    model: &str,
) -> anyhow::Result<()> {
    let backend = proxy_state
        .llm_registry
        .inner
        .first_ollama_backend()
        .await
        .ok_or_else(|| anyhow::anyhow!("No Ollama server attached for model start"))?;
    let ollama_url = backend.base_url().to_string();
    let client = proxy_state.ollama_http_client.read().await.clone();

    send_progress(
        &ws_write,
        req_id,
        "downloading",
        Some(0),
        Some("Starting model download".to_string()),
    )
    .await?;

    let ws_for_progress = ws_write.clone();
    let req_id_progress = req_id.to_string();
    crate::ollama_client::pull_model_with_progress(&client, &ollama_url, model, move |pct| {
        let ws = ws_for_progress.clone();
        let rid = req_id_progress.clone();
        tokio::spawn(async move {
            let _ = send_progress(&ws, &rid, "downloading", Some(pct), None).await;
        });
    })
    .await?;

    send_progress(
        &ws_write,
        req_id,
        "warming",
        Some(100),
        Some("Download complete; warming model".to_string()),
    )
    .await?;

    let show = crate::ollama_client::fetch_show_info(&client, &ollama_url, model)
        .await
        .ok();
    crate::ollama_client::warmup_model(&client, &ollama_url, model, show.as_ref()).await?;

    send_progress(
        &ws_write,
        req_id,
        "ready",
        Some(100),
        Some("Model is ready".to_string()),
    )
    .await?;

    let mut show_cache = shared_state.lock().await.show_info_cache.clone();
    if let Ok(snapshot) = backend.list_models(&mut show_cache, _gpu_probe).await {
        let merged = crate::llm_registry::MergedCatalog {
            models: snapshot.models.clone(),
            model_names: snapshot.model_names.clone(),
            gpu_host: snapshot.gpu_host.clone(),
            collisions: Vec::new(),
        };
        let cfg = proxy_state.client_config.read().await;
        let (advertised_models, _) =
            crate::llm_registry::filter_models_for_cluster_advertisement(&merged, &cfg);
        drop(cfg);

        let mut state = shared_state.lock().await;
        state.local_models = snapshot.model_names.clone();
        state.local_models_full = snapshot.models.clone();
        state.show_info_cache = show_cache;
        state.last_gpu_host = snapshot.gpu_host.clone();
        let accepting_jobs = true;
        let update_msg = ProtocolMessage::UpdateModels {
            models: advertised_models,
            gpu_host: snapshot.gpu_host.clone(),
            accepting_jobs: Some(accepting_jobs),
        };
        if let Ok(text) = serde_json::to_string(&update_msg) {
            let _ = ws_write.lock().await.send(Message::Text(text)).await;
        }
    }

    Ok(())
}

pub struct WebRTCManager {
    my_name: Arc<Mutex<String>>,
    cluster_id: String,
    cluster_name: Option<String>,
    peers: Arc<Mutex<HashMap<String, Arc<Mutex<PeerState>>>>>,
    shared_state: SharedState,
    proxy_cmd_rx: Option<mpsc::Receiver<ProxyRequestCommand>>,
    model_start_cmd_rx: Option<mpsc::Receiver<ModelStartAction>>,
    connection_cmd_rx: Option<mpsc::Receiver<ConnectionAction>>,
    peer_moderation_rx: Option<mpsc::Receiver<PeerModerationAction>>,
    ws_write: Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >>>,
    ws_read: Option<futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >>,
    pending_requests: Arc<Mutex<HashMap<String, mpsc::Sender<Result<String, String>>>>>,
    pending_encrypted_consumer:
        Arc<Mutex<HashMap<String, crate::cluster_dc_e2ee::EncryptedConsumerState>>>,
    pending_peer_lookups: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<Vec<String>>>>>,
    pending_connect_approvals:
        Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<Result<(String, u64), String>>>>>,
    connection_allowances: Arc<Mutex<ConnectionAllowanceStore>>,
    approved_proxy_models: Arc<Mutex<HashMap<String, String>>>,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    model_poll_secs: u64,
    gpu_probe: GpuProbeMode,
    active_exchanges: Arc<Mutex<HashMap<String, ProxyExchangeMeta>>>,
    peer_registry: PeerRegistry,
    cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    cluster_connected: Arc<Mutex<HashMap<String, bool>>>,
    last_cluster_state_version: AtomicU64,
    peer_attestation_hints: Arc<Mutex<HashMap<String, u64>>>,
    /// Peers currently going through outbound SDP offer / ICE setup.
    negotiating_peers: Arc<Mutex<HashSet<String>>>,
}

impl WebRTCManager {
    pub async fn new(
        my_name: String,
        cluster_id: String,
        cluster_name: Option<String>,
        lobby_server_host: String,
        shared_state: SharedState,
        proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
        model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
        connection_cmd_rx: mpsc::Receiver<ConnectionAction>,
        peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
        proxy_state: Arc<crate::llm_proxy::ProxyState>,
        peer_registry: PeerRegistry,
        cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
        swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
        cluster_connected: Arc<Mutex<HashMap<String, bool>>>,
    ) -> anyhow::Result<Self> {
        if !my_name.is_empty() {
            let mut cfg = proxy_state.client_config.write().await;
            crate::client_config::sync_peer_device_key_with_lobby(
                &proxy_state.http_client,
                &mut cfg,
                proxy_state.tx_store.as_ref(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("pre-connect device key sync failed: {e}"))?;
        }

        if my_name.is_empty() {
            anyhow::bail!("peer_id required for lobby WebSocket connection");
        }

        let cluster_param = url_encode_component(&cluster_id);
        let ws_base = crate::lobby_url::lobby_ws_base(&lobby_server_host);
        let mut ws_url = format!(
            "{}/ws?name={}&cluster_id={}",
            ws_base, my_name, cluster_param
        );
        let (timestamp, signature) =
            crate::security::build_ws_auth_query(proxy_state.tx_store.as_ref(), &my_name)?;
        ws_url.push_str("&timestamp=");
        ws_url.push_str(&url_encode_component(&timestamp));
        ws_url.push_str("&signature=");
        ws_url.push_str(&url_encode_component(&signature));
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (ws_write, ws_read) = ws_stream.split();
        let display_name = my_name.clone();
        let manager = WebRTCManager {
            my_name: Arc::new(Mutex::new(my_name)),
            cluster_id: cluster_id.clone(),
            cluster_name,
            peers: Arc::new(Mutex::new(HashMap::new())),
            shared_state,
            proxy_cmd_rx: Some(proxy_cmd_rx),
            model_start_cmd_rx: Some(model_start_cmd_rx),
            connection_cmd_rx: Some(connection_cmd_rx),
            peer_moderation_rx: Some(peer_moderation_rx),
            ws_write: Arc::new(Mutex::new(ws_write)),
            ws_read: Some(ws_read),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            pending_encrypted_consumer: Arc::new(Mutex::new(HashMap::new())),
            pending_peer_lookups: Arc::new(Mutex::new(HashMap::new())),
            pending_connect_approvals: Arc::new(Mutex::new(HashMap::new())),
            connection_allowances: Arc::new(Mutex::new(ConnectionAllowanceStore::new(
                std::time::Duration::from_secs(CONNECTION_ALLOWANCE_TTL_SECS),
            ))),
            approved_proxy_models: Arc::new(Mutex::new(HashMap::new())),
            proxy_state,
            model_poll_secs: model_poll_interval_secs(),
            gpu_probe: gpu_probe_mode(),
            active_exchanges: Arc::new(Mutex::new(HashMap::new())),
            peer_registry,
            cluster_network_models,
            swarm_network_models,
            cluster_connected,
            last_cluster_state_version: AtomicU64::new(0),
            peer_attestation_hints: Arc::new(Mutex::new(HashMap::new())),
            negotiating_peers: Arc::new(Mutex::new(HashSet::new())),
        };
        manager.set_cluster_connected(false).await;
        println!(
            "Connected to lobby server as '{}' (cluster: {}) — awaiting registration…",
            display_name, cluster_id
        );
        Ok(manager)
    }

    async fn track_peer(&self, peer_id: &str, direction: PeerDirection, attestation_flags: u64) {
        let key = peer_registry_key(peer_id, &self.cluster_id);
        self.peer_registry.lock().await.insert(
            key,
            TrackedPeer {
                peer_id: peer_id.to_string(),
                cluster_id: Some(self.cluster_id.clone()),
                swarm_id: None,
                direction,
                connected_at: std::time::Instant::now(),
                data_channel_open: false,
                attestation_flags,
            },
        );
        crate::cluster_manager::sync_peer_connections_with_store(
            &self.shared_state,
            &self.peer_registry,
            &self.proxy_state.tx_store,
            &self.proxy_state.peer_stats,
        )
        .await;
    }

    async fn is_peer_blocked(&self, peer_id: &str) -> bool {
        self.proxy_state
            .tx_store
            .is_peer_blocked(peer_id)
            .await
            .unwrap_or(false)
    }

    async fn close_peer_connection(&self, peer_id: &str) {
        self.negotiating_peers.lock().await.remove(peer_id);
        let ps_arc = {
            let mut peers = self.peers.lock().await;
            peers.remove(peer_id)
        };
        if let Some(ps_arc) = ps_arc {
            let ps = ps_arc.lock().await;
            let _ = ps.pc.close().await;
        }
        let key = peer_registry_key(peer_id, &self.cluster_id);
        self.peer_registry.lock().await.remove(&key);
        self.proxy_state.stats_remove_peer(peer_id);
        crate::cluster_manager::sync_peer_connections_with_store(
            &self.shared_state,
            &self.peer_registry,
            &self.proxy_state.tx_store,
            &self.proxy_state.peer_stats,
        )
        .await;
    }

    async fn get_peer_state(&self, peer_id: &str) -> Option<Arc<Mutex<PeerState>>> {
        self.peers.lock().await.get(peer_id).cloned()
    }

    async fn is_peer_session_ready(&self, peer_id: &str) -> bool {
        match self.get_peer_state(peer_id).await {
            Some(ps_arc) => peer_data_channel_ready(&ps_arc).await,
            None => false,
        }
    }

    async fn remove_stale_peer_if_needed(&self, peer_id: &str) -> bool {
        let Some(ps_arc) = self.get_peer_state(peer_id).await else {
            return false;
        };
        if peer_data_channel_ready(&ps_arc).await {
            return false;
        }
        let terminal = {
            let ps = ps_arc.lock().await;
            peer_connection_is_terminal(&ps.pc)
        };
        if terminal {
            println!("♻️ Removing stale WebRTC session with {peer_id}");
            self.close_peer_connection(peer_id).await;
            return true;
        }
        false
    }

    async fn is_negotiating(&self, peer_id: &str) -> bool {
        self.negotiating_peers.lock().await.contains(peer_id)
    }

    async fn try_begin_negotiation(&self, peer_id: &str) -> bool {
        let mut set = self.negotiating_peers.lock().await;
        if set.contains(peer_id) {
            false
        } else {
            set.insert(peer_id.to_string());
            true
        }
    }

    async fn end_negotiation(&self, peer_id: &str) {
        self.negotiating_peers.lock().await.remove(peer_id);
    }

    async fn replace_incomplete_session_if_needed(
        &self,
        peer_id: &str,
        connect_req_id: Option<&str>,
    ) {
        if connect_req_id.is_none() {
            return;
        }
        if self.is_peer_session_ready(peer_id).await {
            return;
        }
        if self.is_negotiating(peer_id).await {
            return;
        }
        if self.get_peer_state(peer_id).await.is_some() {
            println!("♻️ Replacing incomplete WebRTC session with {peer_id}");
            self.close_peer_connection(peer_id).await;
        }
    }

    async fn handle_peer_moderation(&self, action: PeerModerationAction) -> anyhow::Result<()> {
        match action {
            PeerModerationAction::CloseConnections { peer_id } => {
                self.close_peer_connection(&peer_id).await;
            }
            PeerModerationAction::Report { peer_id, reason } => {
                self.close_peer_connection(&peer_id).await;
                let msg = ProtocolMessage::ReportPeer {
                    target_peer_id: peer_id,
                    reason,
                };
                self.send_protocol_message(&msg).await?;
            }
        }
        Ok(())
    }

    async fn set_cluster_connected(&self, connected: bool) {
        self.cluster_connected
            .lock()
            .await
            .insert(self.cluster_id.clone(), connected);
        let connected_map = self.cluster_connected.lock().await.clone();
        let cfg = self.proxy_state.client_config.read().await;
        let thermal_active = self.shared_state.lock().await.gpu_thermal_guard.active;
        let clusters_cfg = cfg.clusters.clone();
        let registry = self.peer_registry.lock().await;
        let statuses: Vec<crate::shared::ClusterStatus> = clusters_cfg
            .into_iter()
            .map(|r| {
                let outbound = registry
                    .values()
                    .filter(|p| {
                        p.cluster_id.as_deref() == Some(r.cluster_id.as_str())
                            && p.direction == PeerDirection::Outbound
                    })
                    .count();
                let inbound = registry
                    .values()
                    .filter(|p| {
                        p.cluster_id.as_deref() == Some(r.cluster_id.as_str())
                            && p.direction == PeerDirection::Inbound
                    })
                    .count();
                let schedule = cluster_schedule_fields(&cfg, &r);
                ClusterStatus {
                    cluster_id: r.cluster_id.clone(),
                    name: r.name.clone(),
                    visibility: r.visibility.clone(),
                    lobby_connected: connected_map.get(&r.cluster_id).copied().unwrap_or(false),
                    outbound_peers: outbound,
                    inbound_peers: inbound,
                    accepting_jobs: cluster_accepts_jobs(&r),
                    connected: cluster_is_connected(&r),
                    network_models: Vec::new(),
                    schedule_enabled: schedule.schedule_enabled,
                    schedule_start: schedule.schedule_start,
                    schedule_end: schedule.schedule_end,
                    schedule_next_transition_at: schedule.schedule_next_transition_at,
                    schedule_inside_window: schedule.schedule_inside_window,
                    thermal_paused: thermal_active && !cluster_accepts_jobs(&r),
                }
            })
            .collect();
        let models_by_cluster = self.cluster_network_models.lock().await.clone();
        let mut state = self.shared_state.lock().await;
        state.clusters = statuses;
        for cluster in &mut state.clusters {
            cluster.network_models = models_by_cluster
                .get(&cluster.cluster_id)
                .cloned()
                .unwrap_or_default();
        }
    }

    async fn track_consumer_exchange(
        &self,
        req_id: &str,
        remote_peer_id: &str,
        model: &str,
        path: &str,
        bytes_sent: u64,
    ) {
        self.proxy_state.stats_request_start(req_id, remote_peer_id, model, bytes_sent);
        let mut exchanges = self.active_exchanges.lock().await;
        exchanges.insert(
            req_id.to_string(),
            ProxyExchangeMeta {
                remote_peer_id: remote_peer_id.to_string(),
                model: model.to_string(),
                path: path.to_string(),
                started_at: std::time::Instant::now(),
                response_buffer: String::new(),
                bytes_sent,
                bytes_received: 0,
            },
        );
    }

    async fn next_proxy_req_id(&self) -> String {
        let peer_id = self.shared_state.lock().await.peer_id.clone();
        let seq = NEXT_REQ_ID.fetch_add(1, Ordering::SeqCst);
        format!("{}:{}", peer_id, seq)
    }

    async fn send_peer_info(&self, peer_info: &PeerInfo) -> anyhow::Result<()> {
        let msg = ProtocolMessage::UpdatePeerInfo {
            lat: peer_info.lat,
            lon: peer_info.lon,
            asn: peer_info.asn.clone(),
            city: peer_info.city.clone(),
            country: peer_info.country.clone(),
        };
        let text = serde_json::to_string(&msg)?;
        self.ws_write.lock().await.send(Message::Text(text)).await?;
        Ok(())
    }

    async fn cluster_accepts_jobs_now(&self) -> bool {
        let cfg = self.proxy_state.client_config.read().await;
        find_cluster(&cfg, &self.cluster_id)
            .map(cluster_accepts_jobs)
            .unwrap_or(true)
    }

    async fn poll_and_advertise(&self, force: bool) -> anyhow::Result<()> {
        let version = self
            .proxy_state
            .cluster_state_version
            .load(Ordering::SeqCst);
        let prev = self.last_cluster_state_version.load(Ordering::SeqCst);
        let force = force || version != prev;

        if !self.cluster_accepts_jobs_now().await {
            let mut state = self.shared_state.lock().await;
            let last_hash = state.last_advertised_hash.clone();
            let gpu_host = state.last_gpu_host.clone();
            if !force && last_hash.as_deref() == Some(MAINTENANCE_CATALOG_HASH) {
                return Ok(());
            }
            state.last_advertised_hash = Some(MAINTENANCE_CATALOG_HASH.to_string());
            drop(state);

            let update_msg = ProtocolMessage::UpdateModels {
                models: Vec::new(),
                gpu_host,
                accepting_jobs: Some(false),
            };
            let text = serde_json::to_string(&update_msg)?;
            self.ws_write.lock().await.send(Message::Text(text)).await?;
            println!(
                "🛠️ Maintenance mode — cleared model advertisements (room: {})",
                self.cluster_id
            );
            return Ok(());
        }

        if self.proxy_state.llm_registry.inner.attached_count().await == 0 {
            return Err(anyhow::anyhow!("No LLM servers attached"));
        }

        let (last_hash, peer_info) = {
            let state = self.shared_state.lock().await;
            (
                state.last_advertised_hash.clone(),
                state.peer_info.clone(),
            )
        };

        let snapshot = self
            .proxy_state
            .llm_registry
            .inner
            .rebuild_catalog(self.gpu_probe)
            .await?;

        let cfg = self.proxy_state.client_config.read().await;
        let (advertised_models, _advertised_names) =
            crate::llm_registry::filter_models_for_cluster_advertisement(&snapshot, &cfg);
        drop(cfg);

        let hash = catalog_hash(&advertised_models, &snapshot.gpu_host);
        if !force && last_hash.as_deref() == Some(hash.as_str()) {
            return Ok(());
        }

        {
            let mut state = self.shared_state.lock().await;
            state.local_models = snapshot.model_names.clone();
            state.local_models_full = snapshot.models.clone();
            state.last_advertised_hash = Some(hash.clone());
            state.last_gpu_host = snapshot.gpu_host.clone();
            state.local_model_collisions = snapshot.collisions.clone();
        }
        self.last_cluster_state_version
            .store(version, Ordering::SeqCst);

        let accepting_jobs = self.cluster_accepts_jobs_now().await;
        let update_msg = ProtocolMessage::UpdateModels {
            models: advertised_models.clone(),
            gpu_host: snapshot.gpu_host.clone(),
            accepting_jobs: Some(accepting_jobs),
        };
        let text = serde_json::to_string(&update_msg)?;
        self.ws_write.lock().await.send(Message::Text(text)).await?;

        let loaded = advertised_models
            .iter()
            .filter(|m| {
                m.get("_status")
                    .and_then(|s| s.get("loaded"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .count();
        println!(
            "📤 Advertised {} models ({} loaded) to lobby{}",
            advertised_models.len(),
            loaded,
            peer_info
                .as_ref()
                .map(|p| format!(" from {}, {}", p.city.as_deref().unwrap_or("?"), p.country.as_deref().unwrap_or("?")))
                .unwrap_or_default()
        );
        Ok(())
    }

    async fn create_peer_connection() -> anyhow::Result<Arc<RTCPeerConnection>> {
        let api = APIBuilder::new().build();
        let config = RTCConfiguration {
            ice_servers: vec![webrtc::ice_transport::ice_server::RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };
        Ok(Arc::new(api.new_peer_connection(config).await?))
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        println!("\nWaiting for directory updates or peer initialization request...");

        let mut proxy_cmd_rx = self.proxy_cmd_rx.take().unwrap();
        let mut model_start_cmd_rx = self.model_start_cmd_rx.take().unwrap();
        let mut connection_cmd_rx = self.connection_cmd_rx.take().unwrap();
        let mut peer_moderation_rx = self.peer_moderation_rx.take().unwrap();
        let mut ws_read = self.ws_read.take().unwrap();
        let shared_self = Arc::new(self);

        let mut poll_interval =
            tokio::time::interval(std::time::Duration::from_secs(shared_self.model_poll_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        poll_interval.tick().await;

        loop {
            tokio::select! {
                Some(cmd) = proxy_cmd_rx.recv() => {
                    let manager = shared_self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_proxy_cmd(cmd).await {
                            println!("❌ Error handling proxy command: {}", e);
                        }
                    });
                }
                Some(action) = model_start_cmd_rx.recv() => {
                    let manager = shared_self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_model_start_action(action).await {
                            println!("❌ Error handling model start action: {}", e);
                        }
                    });
                }
                Some(action) = connection_cmd_rx.recv() => {
                    let manager = shared_self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_connection_action(action).await {
                            println!("❌ Error handling connection action: {}", e);
                        }
                    });
                }
                Some(action) = peer_moderation_rx.recv() => {
                    let manager = shared_self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = manager.handle_peer_moderation(action).await {
                            println!("❌ Error handling peer moderation action: {}", e);
                        }
                    });
                }
                msg = ws_read.next() => {
                    match msg {
                        Some(Ok(Message::Text(msg_text))) => {
                            if let Ok(parsed) = serde_json::from_str::<ProtocolMessage>(&msg_text) {
                                let manager = shared_self.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = manager.handle_protocol_message(parsed).await {
                                        println!("❌ Error handling protocol message: {}", e);
                                    }
                                });
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if let Some(frame) = frame {
                                eprintln!(
                                    "⚠️ Lobby closed WebSocket for cluster {}: {} ({})",
                                    shared_self.cluster_id, frame.code, frame.reason
                                );
                            } else {
                                eprintln!(
                                    "⚠️ Lobby closed WebSocket for cluster {}",
                                    shared_self.cluster_id
                                );
                            }
                            shared_self.set_cluster_connected(false).await;
                            return Ok(());
                        }
                        None => {
                            eprintln!(
                                "⚠️ Lobby WebSocket stream ended for cluster {}",
                                shared_self.cluster_id
                            );
                            shared_self.set_cluster_connected(false).await;
                            return Ok(());
                        }
                        _ => {}
                    }
                }
                _ = poll_interval.tick() => {
                    let manager = shared_self.clone();
                    match manager.poll_and_advertise(false).await {
                        Ok(()) => {}
                        Err(e) if ws_send_disconnected(&e) => {
                            eprintln!("⚠️ Lobby connection closed (room {})", manager.cluster_id);
                            manager.set_cluster_connected(false).await;
                            return Ok(());
                        }
                        Err(e) => eprintln!("⚠️ Model poll failed: {}", e),
                    }
                }
            }
        }
    }

    async fn send_protocol_message(&self, msg: &ProtocolMessage) -> anyhow::Result<()> {
        let text = serde_json::to_string(msg)?;
        self.ws_write.lock().await.send(Message::Text(text)).await?;
        Ok(())
    }

    async fn respond_to_model_start_offer(
        &self,
        req_id: String,
        accept: bool,
        model: Option<String>,
    ) -> anyhow::Result<()> {
        {
            let mut state = self.shared_state.lock().await;
            state.incoming_model_offers.retain(|o| o.req_id != req_id);
        }
        self.send_protocol_message(&ProtocolMessage::RespondModelStart {
            req_id: req_id.clone(),
            accept,
        })
        .await?;
        if accept {
            if let Some(model) = model {
                let ws_write = self.ws_write.clone();
                let shared_state = self.shared_state.clone();
                let proxy_state = self.proxy_state.clone();
                let cluster_id = self.cluster_id.clone();
                let gpu_probe = self.gpu_probe;
                let req_id_spawn = req_id.clone();
                let ws_fail = ws_write.clone();
                tokio::spawn(async move {
                    if let Err(e) = execute_model_start_task(
                        ws_write,
                        shared_state,
                        proxy_state,
                        cluster_id,
                        gpu_probe,
                        &req_id_spawn,
                        &model,
                    )
                    .await
                    {
                        eprintln!("Model start failed for {model}: {e}");
                        let fail_msg = ProtocolMessage::ReportModelStartProgress {
                            req_id: req_id_spawn,
                            status: "failed".to_string(),
                            progress_pct: None,
                            message: Some(e.to_string()),
                        };
                        if let Ok(text) = serde_json::to_string(&fail_msg) {
                            let _ = ws_fail.lock().await.send(Message::Text(text)).await;
                        }
                    }
                });
            }
        }
        Ok(())
    }

    async fn handle_model_start_action(&self, action: ModelStartAction) -> anyhow::Result<()> {
        match action {
            ModelStartAction::Request {
                req_id,
                model,
                cluster_id,
                swarm_id: _,
            } => {
                if cluster_id.as_deref() != Some(self.cluster_id.as_str()) {
                    return Ok(());
                }
                let gpu_host = self.shared_state.lock().await.last_gpu_host.clone();
                self.send_protocol_message(&ProtocolMessage::RequestModelStart {
                    req_id: req_id.clone(),
                    model: model.clone(),
                    cluster_id: self.cluster_id.clone(),
                    gpu_host,
                })
                .await?;
                println!(
                    "📤 Model start request sent: model={model} req_id={req_id} cluster={}",
                    self.cluster_id
                );
                let now = crate::gpu_history::unix_now();
                let mut state = self.shared_state.lock().await;
                state
                    .outgoing_model_requests
                    .retain(|r| r.req_id != req_id);
                state.outgoing_model_requests.push(ModelStartRequestState {
                    req_id,
                    model,
                    cluster_id: Some(self.cluster_id.clone()),
                    swarm_id: None,
                    status: "submitted".to_string(),
                    progress_pct: None,
                    provider_peer: None,
                    peers: None,
                    message: Some("Request submitted to lobby".to_string()),
                    updated_at_unix: now,
                });
            }
            ModelStartAction::Respond {
                req_id,
                accept,
                cluster_id,
                swarm_id: _,
            } => {
                if cluster_id.as_deref() != Some(self.cluster_id.as_str()) {
                    return Ok(());
                }
                let model = {
                    let state = self.shared_state.lock().await;
                    state
                        .incoming_model_offers
                        .iter()
                        .find(|o| o.req_id == req_id)
                        .map(|o| o.model.clone())
                };
                self.respond_to_model_start_offer(
                    req_id,
                    accept,
                    if accept { model } else { None },
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn grant_connection_allowance(
        &self,
        req_id: &str,
        requester_peer_id: &str,
        model: &str,
    ) {
        self.connection_allowances.lock().await.grant(
            req_id.to_string(),
            requester_peer_id.to_string(),
            model.to_string(),
        );
    }

    async fn respond_to_connect_offer(
        &self,
        req_id: String,
        accept: bool,
        requester_peer_id: Option<String>,
        model: Option<String>,
    ) -> anyhow::Result<()> {
        {
            let mut state = self.shared_state.lock().await;
            state
                .incoming_connection_offers
                .retain(|o| o.req_id != req_id);
        }
        if accept {
            if let (Some(requester), Some(model)) = (requester_peer_id, model) {
                self.grant_connection_allowance(&req_id, &requester, &model)
                    .await;
            }
        }
        self.send_protocol_message(&ProtocolMessage::RespondConnectOffer {
            req_id,
            accept,
        })
        .await
    }

    async fn handle_connection_action(&self, action: ConnectionAction) -> anyhow::Result<()> {
        match action {
            ConnectionAction::Respond {
                req_id,
                accept,
                cluster_id,
            } => {
                if cluster_id.as_deref() != Some(self.cluster_id.as_str()) {
                    return Ok(());
                }
                let offer = {
                    let state = self.shared_state.lock().await;
                    state
                        .incoming_connection_offers
                        .iter()
                        .find(|o| o.req_id == req_id)
                        .cloned()
                };
                let Some(offer) = offer else {
                    return Ok(());
                };
                self.respond_to_connect_offer(
                    req_id,
                    accept,
                    if accept {
                        Some(offer.requested_by.clone())
                    } else {
                        None
                    },
                    if accept { Some(offer.model.clone()) } else { None },
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn request_brokered_peer(&self, model: &str) -> Result<(String, String, u64), String> {
        println!("🔗 Requesting provider approval for model: {model}");
        let req_id = format!("connect_{}", NEXT_REQ_ID.fetch_add(1, Ordering::SeqCst));
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_connect_approvals
            .lock()
            .await
            .insert(req_id.clone(), tx);

        let req_msg = ProtocolMessage::RequestPeerConnect {
            req_id: req_id.clone(),
            model: model.to_string(),
        };
        self.ws_write
            .lock()
            .await
            .send(Message::Text(serde_json::to_string(&req_msg).map_err(|e| e.to_string())?))
            .await
            .map_err(|e| e.to_string())?;

        match tokio::time::timeout(
            std::time::Duration::from_secs(CONNECT_APPROVAL_TIMEOUT_SECS),
            rx,
        )
        .await
        {
            Ok(Ok(Ok((provider_peer, flags)))) => {
                println!("✅ Provider approved connection: {provider_peer}");
                Ok((provider_peer, req_id, flags))
            }
            Ok(Ok(Err(message))) => Err(message),
            Ok(Err(_)) => Err("Connection approval channel closed".to_string()),
            Err(_) => {
                self.pending_connect_approvals.lock().await.remove(&req_id);
                Err("Timed out waiting for provider approval".to_string())
            }
        }
    }

    async fn handle_proxy_cmd(&self, cmd: ProxyRequestCommand) -> anyhow::Result<()> {
        if let Some(ref cmd_cluster) = cmd.cluster_id {
            if cmd_cluster != &self.cluster_id {
                return Ok(());
            }
        }

        let target_peer = if let Some(ref peer) = cmd.target_peer {
            if self.is_peer_blocked(peer).await {
                let _ = cmd
                    .response_tx
                    .send(Err(format!("Peer {peer} is blocked")))
                    .await;
                return Ok(());
            }
            peer.clone()
        } else {
            match self.request_brokered_peer(&cmd.model).await {
                Ok((peer, req_id, provider_flags)) => {
                    let allow_unattested = self
                        .proxy_state
                        .client_config
                        .read()
                        .await
                        .allow_unattested_peers;
                    if !peer_connection_allowed(allow_unattested, provider_flags) {
                        let _ = cmd
                            .response_tx
                            .send(Err("Provider is not attested".to_string()))
                            .await;
                        return Ok(());
                    }
                    self.peer_attestation_hints
                        .lock()
                        .await
                        .insert(peer.clone(), provider_flags);
                    return self
                        .connect_and_proxy(cmd, peer, Some(req_id), provider_flags)
                        .await;
                }
                Err(e) => {
                    let _ = cmd.response_tx.send(Err(e)).await;
                    return Ok(());
                }
            }
        };

        self.connect_and_proxy(cmd, target_peer, None, 0).await
    }

    async fn connect_and_proxy(
        &self,
        cmd: ProxyRequestCommand,
        target_peer: String,
        connect_req_id: Option<String>,
        attestation_flags: u64,
    ) -> anyhow::Result<()> {
        self.remove_stale_peer_if_needed(&target_peer).await;
        self.replace_incomplete_session_if_needed(&target_peer, connect_req_id.as_deref())
            .await;

        let pc_exists = {
            let peers = self.peers.lock().await;
            peers.contains_key(&target_peer)
        };

        if !pc_exists && !self.is_negotiating(&target_peer).await {
            let Some(connect_req_id) = connect_req_id else {
                let _ = cmd
                    .response_tx
                    .send(Err(
                        "Missing connection approval for new WebRTC session".to_string(),
                    ))
                    .await;
                return Ok(());
            };
            if !self.try_begin_negotiation(&target_peer).await {
                println!(
                    "⏳ WebRTC negotiation already in progress for {}",
                    target_peer
                );
            } else {
                let create_result = async {
                    println!(
                        "🔌 Creating direct WebRTC session link targeting: {}",
                        target_peer
                    );
                    let pc = Self::create_peer_connection().await?;
                    let data_channel = pc.create_data_channel("secure-chat", None).await?;

                    setup_data_channel_handlers(
                        data_channel.clone(),
                        self.pending_requests.clone(),
                        self.pending_encrypted_consumer.clone(),
                        self.proxy_state.clone(),
                        self.active_exchanges.clone(),
                        self.ws_write.clone(),
                        self.shared_state.clone(),
                        target_peer.clone(),
                        false,
                        self.cluster_id.clone(),
                        None,
                    );

                    println!("📝 Generating SDP offer for peer: {}", target_peer);
                    let offer = pc.create_offer(None).await?;
                    pc.set_local_description(offer.clone()).await?;

                    let mut gather_complete = pc.gathering_complete_promise().await;
                    let _ = gather_complete.recv().await;

                    let final_offer = pc.local_description().await.unwrap();

                    let route_payload = serde_json::json!({
                        "sdp_type": "offer",
                        "sdp": final_offer.sdp,
                        "cluster_id": self.cluster_id,
                        "connect_req_id": connect_req_id,
                    });
                    let msg = ProtocolMessage::Route {
                        to: target_peer.clone(),
                        from: self.my_name.lock().await.clone(),
                        payload: route_payload,
                    };
                    println!("📤 Sending SDP offer via WebSocket to: {}", target_peer);
                    self.ws_write
                        .lock()
                        .await
                        .send(Message::Text(serde_json::to_string(&msg)?))
                        .await?;

                    let peer_state = Arc::new(Mutex::new(PeerState {
                        pc,
                        active_data_channel: Some(data_channel),
                    }));

                    let mut peers = self.peers.lock().await;
                    peers.insert(target_peer.clone(), peer_state);
                    drop(peers);
                    self.track_peer(&target_peer, PeerDirection::Outbound, attestation_flags)
                        .await;
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                self.end_negotiation(&target_peer).await;
                create_result?;
            }
        } else if self.is_peer_session_ready(&target_peer).await {
            println!("⚡ Reusing existing WebRTC session with: {}", target_peer);
        } else {
            println!(
                "⏳ WebRTC session to {} exists but data channel not open yet",
                target_peer
            );
        }

        let ps_arc = {
            let peers = self.peers.lock().await;
            peers.get(&target_peer).cloned()
        };

        if let Some(ps_arc) = ps_arc {
            // Depending on timings, data channel may queue messages if not fully established
            let ps = ps_arc.lock().await;
            let mut is_open = false;
            if let Some(dc) = &ps.active_data_channel {
                if dc.ready_state() == RTCDataChannelState::Open {
                    is_open = true;
                }
            }

            if is_open {
                let dc = ps.active_data_channel.as_ref().unwrap();
                println!("✅ Data channel active for {}. Preparing proxy request...", target_peer);
                let req_id = self.next_proxy_req_id().await;
                let body_bytes = serde_json::to_string(&cmd.body)
                    .unwrap_or_default()
                    .len() as u64;

                self.track_consumer_exchange(
                    &req_id,
                    &target_peer,
                    &cmd.model,
                    &cmd.path,
                    body_bytes,
                )
                .await;

                {
                    let mut reqs = self.pending_requests.lock().await;
                    reqs.insert(req_id.clone(), cmd.response_tx);
                }

                println!("📤 Sending proxy request (ID: {}) to peer: {}", req_id, target_peer);
                tokio::time::sleep(std::time::Duration::from_millis(DC_OPEN_SETTLE_MS)).await;
                if let Err(e) = self
                    .send_cluster_proxy_on_dc(dc, &req_id, &cmd.path, &cmd.body)
                    .await
                {
                    println!("❌ Failed to send proxy request to {}: {}", target_peer, e);
                    self.proxy_state.stats_request_error(&req_id);
                } else {
                    println!("✅ Proxy request sent successfully");
                }
            } else {
                println!("⏳ Data channel not active yet for {}. Waiting for channel to open...", target_peer);
                // Queue the request for when the channel opens
                let req_id = self.next_proxy_req_id().await;
                let body_bytes = serde_json::to_string(&cmd.body)
                    .unwrap_or_default()
                    .len() as u64;

                self.track_consumer_exchange(
                    &req_id,
                    &target_peer,
                    &cmd.model,
                    &cmd.path,
                    body_bytes,
                )
                .await;

                {
                    let mut reqs = self.pending_requests.lock().await;
                    reqs.insert(req_id.clone(), cmd.response_tx.clone());
                }

                // If the channel is not ready, we need to wait for it.
                // We lock the peer state on each poll to check both if the channel has been set and if it's open.
                let ps_inner = ps_arc.clone();
                let my_target_peer = target_peer.clone();
                let proxy_state_delayed = self.proxy_state.clone();
                let pending_encrypted_delayed = self.pending_encrypted_consumer.clone();
                let cluster_id_delayed = self.cluster_id.clone();
                let my_name_delayed = self.my_name.clone();
                let req_id_delayed = req_id.clone();
                let path_delayed = cmd.path.clone();
                let body_delayed = cmd.body.clone();
                let peers_cleanup = self.peers.clone();
                let peer_registry_cleanup = self.peer_registry.clone();
                let shared_state_cleanup = self.shared_state.clone();
                let proxy_state_cleanup = self.proxy_state.clone();
                let cluster_id_cleanup = self.cluster_id.clone();

                tokio::spawn(async move {
                    let mut attempts = 0;
                    while attempts < DC_OPEN_MAX_POLLS {
                        tokio::time::sleep(std::time::Duration::from_millis(DC_OPEN_POLL_MS)).await;

                        let ps_locked = ps_inner.lock().await;
                        if let Some(dc_clone) = &ps_locked.active_data_channel {
                            if dc_clone.ready_state() == RTCDataChannelState::Open {
                                let dc = dc_clone.clone();
                                drop(ps_locked);
                                println!("✅ Data channel to {} is now OPEN! Sending delayed proxy request...", my_target_peer);
                                tokio::time::sleep(std::time::Duration::from_millis(DC_OPEN_SETTLE_MS)).await;
                                let send_result = if crate::cluster_dc_e2ee::should_use_cluster_e2ee() {
                                    let peer_id = my_name_delayed.lock().await.clone();
                                    match crate::cluster_dc_e2ee::send_encrypted_proxy_request(
                                        &dc,
                                        &proxy_state_delayed,
                                        &peer_id,
                                        &req_id_delayed,
                                        &path_delayed,
                                        &body_delayed,
                                        &cluster_id_delayed,
                                        None,
                                    )
                                    .await
                                    {
                                        Ok(state) => {
                                            pending_encrypted_delayed
                                                .lock()
                                                .await
                                                .insert(req_id_delayed.clone(), state);
                                            Ok(())
                                        }
                                        Err(e) => Err(e),
                                    }
                                } else {
                                    send_proxy_request(
                                        &dc,
                                        &req_id_delayed,
                                        &path_delayed,
                                        &body_delayed,
                                    )
                                    .await
                                };
                                if let Err(e) = send_result {
                                    println!("❌ Failed to send delayed proxy request: {}", e);
                                    proxy_state_delayed.stats_request_error(&req_id_delayed);
                                    let _ = cmd
                                        .response_tx
                                        .send(Err(format!("Failed to send: {}", e)))
                                        .await;
                                } else {
                                    println!("✅ Delayed proxy request sent successfully");
                                }
                                return;
                            }
                        }
                        attempts += 1;
                    }
                    println!("❌ Timeout waiting for data channel to open to {}", my_target_peer);
                    if let Some(ps_arc) = peers_cleanup.lock().await.get(&my_target_peer) {
                        let ps = ps_arc.lock().await;
                        println!(
                            "   peer_state={:?} ice_state={:?} dc={:?}",
                            ps.pc.connection_state(),
                            ps.pc.ice_connection_state(),
                            ps.active_data_channel
                                .as_ref()
                                .map(|dc| dc.ready_state())
                        );
                    }
                    let ps_arc = peers_cleanup.lock().await.remove(&my_target_peer);
                    if let Some(ps_arc) = ps_arc {
                        let ps = ps_arc.lock().await;
                        let _ = ps.pc.close().await;
                    }
                    let key = peer_registry_key(&my_target_peer, &cluster_id_cleanup);
                    peer_registry_cleanup.lock().await.remove(&key);
                    proxy_state_cleanup.stats_remove_peer(&my_target_peer);
                    crate::cluster_manager::sync_peer_connections_with_store(
                        &shared_state_cleanup,
                        &peer_registry_cleanup,
                        &proxy_state_cleanup.tx_store,
                        &proxy_state_cleanup.peer_stats,
                    )
                    .await;
                    proxy_state_delayed.stats_request_error(&req_id_delayed);
                    let _ = cmd
                        .response_tx
                        .send(Err("Timeout waiting for WebRTC connection".to_string()))
                        .await;
                });
            }
        }
        Ok(())
    }

    async fn send_cluster_proxy_on_dc(
        &self,
        dc: &RTCDataChannel,
        req_id: &str,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if crate::cluster_dc_e2ee::should_use_cluster_e2ee() {
            let peer_id = self.my_name.lock().await.clone();
            let state = crate::cluster_dc_e2ee::send_encrypted_proxy_request(
                dc,
                &self.proxy_state,
                &peer_id,
                req_id,
                path,
                body,
                &self.cluster_id,
                None,
            )
            .await?;
            self.pending_encrypted_consumer
                .lock()
                .await
                .insert(req_id.to_string(), state);
        } else {
            send_proxy_request(dc, req_id, path, body).await?;
        }
        Ok(())
    }

    async fn handle_protocol_message(&self, msg: ProtocolMessage) -> anyhow::Result<()> {
        match msg {
            ProtocolMessage::Registered {
                name,
                cluster_id,
                cluster_name,
                required_attestation_flags,
            } => {
                println!(
                    "Successfully registered on server as {} (cluster: {})",
                    name, cluster_id
                );
                *self.my_name.lock().await = name.clone();

                {
                    let mut state = self.shared_state.lock().await;
                    state.my_id = name.clone();
                }
                if let Some(flags) = required_attestation_flags {
                    let mut cfg = self.proxy_state.client_config.write().await;
                    if let Some(cluster) =
                        crate::client_config::find_cluster_mut(&mut cfg, &cluster_id)
                    {
                        cluster.required_attestation_flags = Some(flags);
                    }
                    let _ = crate::client_config::save_client_config(&cfg);
                }
                self.set_cluster_connected(true).await;

                if let Some(peer_info) = self.shared_state.lock().await.peer_info.clone() {
                    if let Err(e) = self.send_peer_info(&peer_info).await {
                        eprintln!("⚠️ Failed to send peer info: {}", e);
                    }
                }

                if let Err(e) = self.poll_and_advertise(true).await {
                    eprintln!("⚠️ Failed initial model advertisement: {}", e);
                }
                let _ = cluster_name;
            }
            ProtocolMessage::AvailableModels { models } => {
                self.cluster_network_models
                    .lock()
                    .await
                    .insert(self.cluster_id.clone(), models.clone());
                crate::network_catalog::sync_unified_network_models(
                    &self.shared_state,
                    &self.cluster_network_models,
                    &self.swarm_network_models,
                )
                .await;

                println!("\n=== Available Network Models (room: {}) ===", self.cluster_id);
                if models.is_empty() {
                    println!("(No models available. Open another terminal)");
                } else {
            for model in &models {
                        let name = model.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                        let peer_count = model
                            .get("_peer_count")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(1);
                        let loaded_count = model
                            .get("_loaded_count")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let status = if loaded_count > 0 {
                            format!("{loaded_count}/{peer_count} loaded")
                        } else {
                            format!("{peer_count} peer(s)")
                        };
                        println!(" -> {} [{}]", name, status);
                    }
                }
            }
            ProtocolMessage::PeersForModel { req_id, model: _, peers } => {
                if let Some(tx) = self.pending_peer_lookups.lock().await.remove(&req_id) {
                    let _ = tx.send(peers);
                }
            }
            ProtocolMessage::ConnectUpdate {
                req_id,
                model: _,
                status,
                provider_peer,
                message,
                provider_attestation_flags,
            } => {
                match status.as_str() {
                    "approved" => {
                        if let Some(tx) =
                            self.pending_connect_approvals.lock().await.remove(&req_id)
                        {
                            if let Some(peer) = provider_peer {
                                let flags = provider_attestation_flags.unwrap_or(0);
                                let allow_unattested = self
                                    .proxy_state
                                    .client_config
                                    .read()
                                    .await
                                    .allow_unattested_peers;
                                if !peer_connection_allowed(allow_unattested, flags) {
                                    let _ = tx.send(Err(
                                        "Provider is not attested".to_string(),
                                    ));
                                } else {
                                    self.peer_attestation_hints
                                        .lock()
                                        .await
                                        .insert(peer.clone(), flags);
                                    let _ = tx.send(Ok((peer, flags)));
                                }
                            } else {
                                let _ = tx.send(Err(
                                    message
                                        .unwrap_or_else(|| "Missing provider peer".to_string()),
                                ));
                            }
                        }
                    }
                    "failed" | "rejected" | "timeout" => {
                        if let Some(tx) =
                            self.pending_connect_approvals.lock().await.remove(&req_id)
                        {
                            let _ = tx.send(Err(message.unwrap_or_else(|| status.clone())));
                        }
                    }
                    _ => {
                        // pending_approval and other interim updates — keep the waiter registered
                    }
                }
            }
            ProtocolMessage::ConnectOffer {
                req_id,
                model,
                requested_by,
                requested_by_attestation_flags,
            } => {
                let requester_flags = requested_by_attestation_flags.unwrap_or(0);
                self.peer_attestation_hints
                    .lock()
                    .await
                    .insert(requested_by.clone(), requester_flags);
                println!(
                    "📥 Connect offer: model={model} req_id={req_id} from={requested_by}"
                );
                let allow_unattested = self
                    .proxy_state
                    .client_config
                    .read()
                    .await
                    .allow_unattested_peers;
                let auto_approve = self
                    .proxy_state
                    .client_config
                    .read()
                    .await
                    .auto_approve_inference_connections;
                let accepts_jobs = self.cluster_accepts_jobs_now().await;
                let attestation_ok =
                    peer_connection_allowed(allow_unattested, requester_flags);

                if auto_approve {
                    let accept = accepts_jobs && attestation_ok;
                    if accept {
                        println!("✅ Auto-approved inference connection for {model}");
                    } else if !attestation_ok {
                        println!(
                            "🛡️ Auto-rejected connect offer — requester is not attested (room: {})",
                            self.cluster_id
                        );
                    } else {
                        println!(
                            "🛠️ Auto-rejected connect offer — maintenance mode (room: {})",
                            self.cluster_id
                        );
                    }
                    self.respond_to_connect_offer(
                        req_id,
                        accept,
                        if accept {
                            Some(requested_by)
                        } else {
                            None
                        },
                        if accept { Some(model) } else { None },
                    )
                    .await?;
                } else if !attestation_ok {
                    println!(
                        "🛡️ Rejected connect offer — requester is not attested (room: {})",
                        self.cluster_id
                    );
                    self.respond_to_connect_offer(req_id, false, None, None)
                        .await?;
                } else {
                    let now = crate::gpu_history::unix_now();
                    let mut state = self.shared_state.lock().await;
                    state
                        .incoming_connection_offers
                        .retain(|o| o.req_id != req_id);
                    state.incoming_connection_offers.push(IncomingConnectionOfferState {
                        req_id,
                        model,
                        cluster_id: Some(self.cluster_id.clone()),
                        requested_by,
                        received_at_unix: now,
                    });
                }
            }
            ProtocolMessage::RequestPeerConnect { .. }
            | ProtocolMessage::RespondConnectOffer { .. } => {}
            ProtocolMessage::UpdateModels { .. } => {}
            ProtocolMessage::UpdatePeerInfo { .. } => {}
            ProtocolMessage::GetPeersForModel { .. } => {
                // Sent by clients to the server, so clients never receive it
            }
            ProtocolMessage::ReportTokenUsage { .. } => {
                // Sent by clients to the server; ignore if echoed back
            }
            ProtocolMessage::ModelStartUpdate {
                req_id,
                model,
                status,
                progress_pct,
                provider_peer,
                peers,
                message,
            } => {
                let now = crate::gpu_history::unix_now();
                let mut state = self.shared_state.lock().await;
                if let Some(existing) = state
                    .outgoing_model_requests
                    .iter_mut()
                    .find(|r| r.req_id == req_id)
                {
                    existing.model = model.clone();
                    existing.status = status.clone();
                    existing.progress_pct = progress_pct;
                    existing.provider_peer = provider_peer.clone();
                    existing.peers = peers.clone();
                    existing.message = message.clone();
                    existing.updated_at_unix = now;
                } else {
                    state.outgoing_model_requests.push(ModelStartRequestState {
                        req_id: req_id.clone(),
                        model,
                        cluster_id: Some(self.cluster_id.clone()),
                        swarm_id: None,
                        status,
                        progress_pct,
                        provider_peer,
                        peers,
                        message,
                        updated_at_unix: now,
                    });
                }
            }
            ProtocolMessage::ModelStartOffer {
                req_id,
                model,
                requested_by,
                estimated_vram_mb,
                disk_size_mb,
                run_on_requester,
            } => {
                let self_run = run_on_requester.unwrap_or(false);
                println!(
                    "📥 Model start offer: model={model} req_id={req_id} from={requested_by} vram={estimated_vram_mb}MB self={self_run}"
                );
                let auto_approve = self
                    .proxy_state
                    .client_config
                    .read()
                    .await
                    .auto_approve_run_model_request;
                let accepts_jobs = self.cluster_accepts_jobs_now().await;

                if auto_approve {
                    let accept = accepts_jobs;
                    if accept {
                        println!("✅ Auto-approved model start offer for {model}");
                    } else {
                        println!(
                            "🛠️ Auto-rejected model start offer — maintenance mode (room: {})",
                            self.cluster_id
                        );
                    }
                    self.respond_to_model_start_offer(
                        req_id,
                        accept,
                        if accept { Some(model) } else { None },
                    )
                    .await?;
                } else {
                    let now = crate::gpu_history::unix_now();
                    let mut state = self.shared_state.lock().await;
                    state
                        .incoming_model_offers
                        .retain(|o| o.req_id != req_id);
                    state.incoming_model_offers.push(ModelStartOfferState {
                        req_id,
                        model,
                        cluster_id: Some(self.cluster_id.clone()),
                        swarm_id: None,
                        requested_by,
                        estimated_vram_mb,
                        disk_size_mb,
                        run_on_requester: Some(self_run),
                        received_at_unix: now,
                    });
                }
            }
            ProtocolMessage::RequestModelStart { .. }
            | ProtocolMessage::RespondModelStart { .. }
            | ProtocolMessage::ReportModelStartProgress { .. } => {}
            ProtocolMessage::PeerReportAck {
                target_peer_id,
                report_count,
                banned,
            } => {
                println!(
                    "📋 Report acknowledged for {target_peer_id}: {report_count} reports (banned={banned})"
                );
            }
            ProtocolMessage::PeerBanned {
                peer_id,
                report_count,
            } => {
                println!(
                    "🚫 Peer {peer_id} banned by lobby after {report_count} reports — blocking locally"
                );
                let _ = self
                    .proxy_state
                    .tx_store
                    .block_peer(&peer_id, "system", Some("banned by lobby"))
                    .await;
                self.close_peer_connection(&peer_id).await;
            }
            ProtocolMessage::ReportPeer { .. } => {}
            ProtocolMessage::Route {
                to: _,
                from,
                payload,
            } => {
                let sdp_type = payload
                    .get("sdp_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if sdp_type == "offer" {
                    if !payload_cluster_matches(&payload, &self.cluster_id) {
                        println!(
                            "⚠️ Rejected offer from '{}' — room_id mismatch",
                            from
                        );
                        return Ok(());
                    }
                    if self.is_peer_blocked(&from).await {
                        println!("🚫 Rejected offer from '{}' — peer is blocked", from);
                        return Ok(());
                    }
                    if !self.cluster_accepts_jobs_now().await {
                        println!(
                            "🛠️ Rejected offer from '{}' — maintenance mode (room: {})",
                            from, self.cluster_id
                        );
                        return Ok(());
                    }
                    let requester_flags = self
                        .peer_attestation_hints
                        .lock()
                        .await
                        .get(&from)
                        .copied()
                        .unwrap_or(0);
                    let allow_unattested = self
                        .proxy_state
                        .client_config
                        .read()
                        .await
                        .allow_unattested_peers;
                    if !peer_connection_allowed(allow_unattested, requester_flags) {
                        println!(
                            "🛡️ Rejected offer from '{}' — requester is not attested",
                            from
                        );
                        return Ok(());
                    }
                    let connect_req_id = payload
                        .get("connect_req_id")
                        .and_then(|v| v.as_str());
                    let Some(connect_req_id) = connect_req_id else {
                        println!(
                            "🚫 Rejected offer from '{}' — missing connect_req_id",
                            from
                        );
                        return Ok(());
                    };
                    let approved_model = self
                        .connection_allowances
                        .lock()
                        .await
                        .validate_and_consume(connect_req_id, &from);
                    let Some(approved_model) = approved_model else {
                        println!(
                            "🚫 Rejected offer from '{}' — invalid or expired connect allowance",
                            from
                        );
                        return Ok(());
                    };
                    self.approved_proxy_models
                        .lock()
                        .await
                        .insert(from.clone(), approved_model);
                    let sdp = payload.get("sdp").and_then(|v| v.as_str()).unwrap_or("");
                    println!("\n📩 Received incoming WebRTC session offer from '{}'!", from);

                    let pc = Self::create_peer_connection().await?;

                    let ps_for_dc = Arc::new(Mutex::new(PeerState {
                        pc: pc.clone(),
                        active_data_channel: None,
                    }));

                    let mut peers = self.peers.lock().await;
                    peers.insert(from.clone(), ps_for_dc.clone());
                    drop(peers);
                    self.track_peer(&from, PeerDirection::Inbound, requester_flags)
                        .await;

                    let pending_reqs = self.pending_requests.clone();
                    let pending_encrypted_outer = self.pending_encrypted_consumer.clone();
                    let ps_inner_dc = ps_for_dc.clone();
                    let proxy_state_outer = self.proxy_state.clone();
                    let active_exchanges_outer = self.active_exchanges.clone();
                    let ws_write_outer = self.ws_write.clone();
                    let shared_state_outer = self.shared_state.clone();
                    let consumer_peer_id = from.clone();
                    let cluster_id_for_dc = self.cluster_id.clone();
                    let approved_models_outer = self.approved_proxy_models.clone();

                    pc.on_data_channel(Box::new(move |d| {
                        let pending_reqs_clone = pending_reqs.clone();
                        let pending_encrypted_clone = pending_encrypted_outer.clone();
                        let ps_inner_clone = ps_inner_dc.clone();
                        let d_for_storage = d.clone();
                        let proxy_state_inner = proxy_state_outer.clone();
                        let active_exchanges_inner = active_exchanges_outer.clone();
                        let ws_write_inner = ws_write_outer.clone();
                        let shared_state_inner = shared_state_outer.clone();
                        let consumer_id = consumer_peer_id.clone();
                        let cluster_id = cluster_id_for_dc.clone();
                        let approved_models_inner = approved_models_outer.clone();
                        Box::pin(async move {
                            println!("\n🚀 Dynamic Data Channel successfully mapped: '{}'", d_for_storage.label());
                            setup_data_channel_handlers(
                                d_for_storage.clone(),
                                pending_reqs_clone,
                                pending_encrypted_clone,
                                proxy_state_inner,
                                active_exchanges_inner,
                                ws_write_inner,
                                shared_state_inner,
                                consumer_id,
                                true,
                                cluster_id,
                                Some(approved_models_inner),
                            );
                            let mut ps_mut = ps_inner_clone.lock().await;
                            ps_mut.active_data_channel = Some(d_for_storage);
                        })
                    }));

                    // Imposta l'offerta remota ricevuta
                    pc.set_remote_description(RTCSessionDescription::offer(sdp.to_string())?).await?;
                    
                    // Crea la risposta locale (Answer)
                    let answer = pc.create_answer(None).await?;
                    pc.set_local_description(answer.clone()).await?;

                    // 🌟 AGGIUNGI QUESTO QUI (Nel blocco del Ricevitore) 🌟
                    // Attende che la scansione della rete (ICE gathering) sia completata
                    let mut gather_complete = pc.gathering_complete_promise().await;
                    let _ = gather_complete.recv().await;

                    // Recupera la descrizione locale DEFINITIVA che ora contiene tutti gli IP di rete
                    let final_answer = pc.local_description().await.unwrap();

                    // Invia final_answer.sdp invece di answer.sdp
                    let reply_payload = serde_json::json!({
                        "sdp_type": "answer",
                        "sdp": final_answer.sdp,
                        "cluster_id": self.cluster_id,
                    });
                    let reply_msg = ProtocolMessage::Route {
                        to: from.clone(),
                        from: self.my_name.lock().await.clone(),
                        payload: reply_payload,
                    };
                    
                    self.ws_write.lock().await
                        .send(Message::Text(serde_json::to_string(&reply_msg)?))
                        .await?;
                } else if sdp_type == "answer" {
                    if !payload_cluster_matches(&payload, &self.cluster_id) {
                        println!(
                            "⚠️ Rejected answer from '{}' — room_id mismatch",
                            from
                        );
                        return Ok(());
                    }
                    let sdp = payload.get("sdp").and_then(|v| v.as_str()).unwrap_or("");
                    println!("\n📡 Handle answer response from: '{}'", from);

                    let ps_arc = {
                        let peers = self.peers.lock().await;
                        peers.get(&from).cloned()
                    };

                    if let Some(ps_arc) = ps_arc {
                        let pc = ps_arc.lock().await.pc.clone();
                        pc.set_remote_description(RTCSessionDescription::answer(sdp.to_string())?)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn record_token_usage_local(
    tx_store: Arc<crate::tx_db::TxStore>,
    req_id: String,
    role: &str,
    peer_id: String,
    remote_peer_id: String,
    model: String,
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
) {
    let role = role.to_string();
    tokio::spawn(async move {
        if let Err(e) = tx_store
            .record_local_report(
                req_id,
                &role,
                &peer_id,
                remote_peer_id,
                model,
                prompt_tokens,
                completion_tokens,
                total_tokens,
            )
            .await
        {
            eprintln!("tx_db record error: {}", e);
        }
    });
}

fn setup_data_channel_handlers(
    dc: Arc<RTCDataChannel>,
    pending_requests: Arc<Mutex<HashMap<String, mpsc::Sender<Result<String, String>>>>>,
    pending_encrypted_consumer:
        Arc<Mutex<HashMap<String, crate::cluster_dc_e2ee::EncryptedConsumerState>>>,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    active_exchanges: Arc<Mutex<HashMap<String, ProxyExchangeMeta>>>,
    ws_write: Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >>>,
    shared_state: SharedState,
    remote_peer_id: String,
    is_provider: bool,
    cluster_id: String,
    approved_proxy_models: Option<Arc<Mutex<HashMap<String, String>>>>,
) {
    use crate::p2p_protocol::StreamMessage;

    let pending_incoming =
        Arc::new(Mutex::new(HashMap::<String, PendingIncomingProxyRequest>::new()));
    let pending_encrypted_incoming = Arc::new(Mutex::new(
        HashMap::<String, crate::cluster_dc_e2ee::PendingEncryptedIncomingRequest>::new(),
    ));
    let pending_encrypted_streams = Arc::new(Mutex::new(
        HashMap::<String, crate::cluster_dc_e2ee::PendingEncryptedStream>::new(),
    ));
    let dc_label = dc.label().to_owned();
    let dc_clone = dc.clone();

    dc.on_open(Box::new(move || {
        println!("\n🚀 WebRTC Data Channel Open! ({})", dc_label);
        Box::pin(async {})
    }));

    dc.on_message(Box::new(move |msg| {
        let pending_reqs = pending_requests.clone();
        let pending_encrypted = pending_encrypted_consumer.clone();
        let pending_incoming_reqs = pending_incoming.clone();
        let pending_encrypted_incoming_reqs = pending_encrypted_incoming.clone();
        let pending_encrypted_streams_msg = pending_encrypted_streams.clone();
        let dc_ctx = dc_clone.clone();
        let proxy_state_msg_clone = proxy_state.clone();
        let active_exchanges_msg = active_exchanges.clone();
        let ws_write_msg = ws_write.clone();
        let shared_state_msg = shared_state.clone();
        let remote_peer_msg = remote_peer_id.clone();
        let is_provider_msg = is_provider;
        let cluster_id_msg = cluster_id.clone();
        let approved_models_msg = approved_proxy_models.clone();

        Box::pin(async move {
            if let Ok(stream_msg) = serde_json::from_slice::<StreamMessage>(&msg.data) {
                match (&stream_msg, is_provider_msg) {
                    (StreamMessage::EncryptedProxyRequest { .. }, true) => {
                        let local_peer_id =
                            shared_state_msg.lock().await.peer_id.clone();
                        crate::cluster_dc_e2ee::dispatch_cluster_encrypted_proxy_request(
                            stream_msg,
                            proxy_state_msg_clone.clone(),
                            dc_ctx.clone(),
                            remote_peer_msg.clone(),
                            local_peer_id,
                            ws_write_msg.clone(),
                            cluster_id_msg.clone(),
                            approved_models_msg.clone(),
                        )
                        .await;
                        return;
                    }
                    (
                        StreamMessage::EncryptedProxyRequestStart {
                            req_id,
                            path,
                            room_id,
                            consumer_ephemeral_pk,
                            aad_version,
                            auth,
                        },
                        true,
                    ) => {
                        crate::cluster_dc_e2ee::handle_encrypted_request_start(
                            &pending_encrypted_incoming_reqs,
                            req_id.clone(),
                            path.clone(),
                            room_id.clone(),
                            consumer_ephemeral_pk.clone(),
                            *aad_version,
                            auth.clone(),
                        )
                        .await;
                        return;
                    }
                    (
                        StreamMessage::EncryptedProxyRequestChunk {
                            req_id,
                            seq,
                            nonce,
                            ciphertext,
                            ..
                        },
                        true,
                    ) => {
                        crate::cluster_dc_e2ee::handle_encrypted_request_chunk(
                            &pending_encrypted_incoming_reqs,
                            req_id,
                            *seq,
                            nonce.clone(),
                            ciphertext.clone(),
                        )
                        .await;
                        return;
                    }
                    (StreamMessage::EncryptedProxyRequestEnd { req_id }, true) => {
                        let local_peer_id =
                            shared_state_msg.lock().await.peer_id.clone();
                        crate::cluster_dc_e2ee::handle_encrypted_request_end(
                            &pending_encrypted_incoming_reqs,
                            proxy_state_msg_clone.clone(),
                            dc_ctx.clone(),
                            remote_peer_msg.clone(),
                            local_peer_id,
                            ws_write_msg.clone(),
                            cluster_id_msg.clone(),
                            approved_models_msg.clone(),
                            req_id.clone(),
                        )
                        .await;
                        return;
                    }
                    (
                        StreamMessage::EncryptedProxyResponse {
                            req_id,
                            error,
                            stream,
                            ..
                        },
                        false,
                    ) => {
                        if stream == &Some(true) && error.is_none() {
                            if let Some(state) =
                                pending_encrypted.lock().await.get(req_id).cloned()
                            {
                                pending_encrypted_streams_msg.lock().await.insert(
                                    req_id.clone(),
                                    crate::cluster_dc_e2ee::init_pending_encrypted_stream(&state),
                                );
                            }
                            return;
                        }
                        if let Some(state) = pending_encrypted.lock().await.remove(req_id) {
                            match crate::cluster_dc_e2ee::decrypt_encrypted_response(
                                &proxy_state_msg_clone,
                                &state,
                                req_id,
                                &stream_msg,
                            )
                            .await
                            {
                                Ok(body) => {
                                    let reqs = pending_reqs.lock().await;
                                    if let Some(tx) = reqs.get(req_id) {
                                        let _ = tx.send(Ok(body)).await;
                                    }
                                }
                                Err(e) => {
                                    let reqs = pending_reqs.lock().await;
                                    if let Some(tx) = reqs.get(req_id) {
                                        let _ = tx.send(Err(e.to_string())).await;
                                    }
                                }
                            }
                        }
                        return;
                    }
                    (
                        StreamMessage::EncryptedProxyStreamChunk {
                            req_id,
                            seq,
                            nonce,
                            ciphertext,
                            done,
                            ..
                        },
                        false,
                    ) => {
                        let outcome = crate::cluster_dc_e2ee::handle_encrypted_stream_chunk(
                            &pending_encrypted_streams_msg,
                            &proxy_state_msg_clone,
                            req_id,
                            *seq,
                            nonce.clone(),
                            ciphertext.clone(),
                            *done,
                        )
                        .await;

                        match outcome {
                            Ok(crate::cluster_dc_e2ee::EncryptedStreamChunkOutcome::Waiting) => {}
                            Ok(
                                crate::cluster_dc_e2ee::EncryptedStreamChunkOutcome::Progress {
                                    parts,
                                },
                            ) => {
                                for part in parts {
                                    let chunk_len = part.len() as u64;
                                    let partial_tokens = {
                                        let mut exchanges = active_exchanges_msg.lock().await;
                                        if let Some(meta) = exchanges.get_mut(req_id) {
                                            meta.bytes_received += chunk_len;
                                            accumulate_and_parse_usage(
                                                &mut meta.response_buffer,
                                                &part,
                                            )
                                            .map(|u| u.total_tokens)
                                            .unwrap_or(0)
                                        } else {
                                            0
                                        }
                                    };
                                    proxy_state_msg_clone.stats_stream_progress(
                                        req_id,
                                        chunk_len,
                                        partial_tokens,
                                    );
                                    let reqs = pending_reqs.lock().await;
                                    if let Some(tx) = reqs.get(req_id) {
                                        let _ = tx.send(Ok(part)).await;
                                    }
                                }
                            }
                            Ok(
                                crate::cluster_dc_e2ee::EncryptedStreamChunkOutcome::Finished {
                                    parts,
                                },
                            ) => {
                                for part in parts {
                                    let chunk_len = part.len() as u64;
                                    let partial_tokens = {
                                        let mut exchanges = active_exchanges_msg.lock().await;
                                        if let Some(meta) = exchanges.get_mut(req_id) {
                                            meta.bytes_received += chunk_len;
                                            accumulate_and_parse_usage(
                                                &mut meta.response_buffer,
                                                &part,
                                            )
                                            .map(|u| u.total_tokens)
                                            .unwrap_or(0)
                                        } else {
                                            0
                                        }
                                    };
                                    proxy_state_msg_clone.stats_stream_progress(
                                        req_id,
                                        chunk_len,
                                        partial_tokens,
                                    );
                                    let reqs = pending_reqs.lock().await;
                                    if let Some(tx) = reqs.get(req_id) {
                                        let _ = tx.send(Ok(part)).await;
                                    }
                                }
                                pending_encrypted.lock().await.remove(req_id);
                                let meta = {
                                    let mut exchanges = active_exchanges_msg.lock().await;
                                    exchanges.remove(req_id)
                                };
                                if let Some(meta) = meta {
                                    let usage = parse_usage_from_buffer(&meta.response_buffer)
                                        .unwrap_or(TokenUsage {
                                            prompt_tokens: 0,
                                            completion_tokens: 0,
                                            total_tokens: 0,
                                        });
                                    proxy_state_msg_clone.stats_request_complete(
                                        req_id,
                                        usage.total_tokens,
                                        meta.bytes_received,
                                    );
                                    let peer_id = shared_state_msg.lock().await.peer_id.clone();
                                    let remote_peer_id = meta.remote_peer_id.clone();
                                    let model = meta.model.clone();
                                    record_token_usage_local(
                                        proxy_state_msg_clone.tx_store.clone(),
                                        req_id.clone(),
                                        "consumer",
                                        peer_id.clone(),
                                        remote_peer_id.clone(),
                                        model.clone(),
                                        usage.prompt_tokens,
                                        usage.completion_tokens,
                                        usage.total_tokens,
                                    );
                                    let report = ProtocolMessage::ReportTokenUsage {
                                        req_id: req_id.clone(),
                                        role: "consumer".to_string(),
                                        peer_id,
                                        remote_peer_id,
                                        model,
                                        path: meta.path,
                                        prompt_tokens: usage.prompt_tokens,
                                        completion_tokens: usage.completion_tokens,
                                        total_tokens: usage.total_tokens,
                                        bytes_sent: meta.bytes_sent,
                                        bytes_received: meta.bytes_received,
                                        duration_ms: meta.started_at.elapsed().as_millis() as u64,
                                    };
                                    if let Ok(text) = serde_json::to_string(&report) {
                                        let _ = ws_write_msg
                                            .lock()
                                            .await
                                            .send(Message::Text(text))
                                            .await;
                                    }
                                }
                                let mut reqs = pending_reqs.lock().await;
                                reqs.remove(req_id);
                            }
                            Err(e) => {
                                pending_encrypted.lock().await.remove(req_id);
                                pending_encrypted_streams_msg.lock().await.remove(req_id);
                                let mut reqs = pending_reqs.lock().await;
                                if let Some(tx) = reqs.remove(req_id) {
                                    let _ = tx.send(Err(e)).await;
                                }
                            }
                        }
                        return;
                    }
                    _ => {}
                }
            }

            if let Ok(dc_msg) = serde_json::from_slice::<DataChannelMessage>(&msg.data) {
                if is_provider_msg && crate::cluster_dc_e2ee::should_use_cluster_e2ee() {
                    match &dc_msg {
                        DataChannelMessage::ProxyRequest { req_id, .. }
                        | DataChannelMessage::ProxyRequestStart { req_id, .. }
                        | DataChannelMessage::ProxyRequestChunk { req_id, .. }
                        | DataChannelMessage::ProxyRequestEnd { req_id } => {
                            let err_msg = DataChannelMessage::ProxyResponseError {
                                req_id: req_id.clone(),
                                error: "Plaintext proxy rejected; upgrade to E2EE v2".to_string(),
                            };
                            let _ = send_dc_message(&dc_ctx, &err_msg).await;
                            return;
                        }
                        _ => {}
                    }
                }
                match dc_msg {
                    DataChannelMessage::ProxyRequest {
                        req_id,
                        path,
                        body,
                    } => {
                        dispatch_proxy_request(
                            req_id,
                            path,
                            body,
                            proxy_state_msg_clone.clone(),
                            dc_ctx.clone(),
                            remote_peer_msg.clone(),
                            ws_write_msg.clone(),
                            shared_state_msg.clone(),
                            cluster_id_msg.clone(),
                            approved_models_msg.clone(),
                        );
                    }
                    DataChannelMessage::ProxyRequestStart { req_id, path } => {
                        pending_incoming_reqs.lock().await.insert(
                            req_id.clone(),
                            PendingIncomingProxyRequest {
                                path,
                                chunks: Vec::new(),
                            },
                        );
                    }
                    DataChannelMessage::ProxyRequestChunk { req_id, seq, chunk } => {
                        if let Some(pending) =
                            pending_incoming_reqs.lock().await.get_mut(&req_id)
                        {
                            pending.chunks.push((seq, chunk));
                        }
                    }
                    DataChannelMessage::ProxyRequestEnd { req_id } => {
                        let pending = pending_incoming_reqs.lock().await.remove(&req_id);
                        if let Some(mut pending) = pending {
                            pending.chunks.sort_by_key(|(seq, _)| *seq);
                            let body_str: String =
                                pending.chunks.into_iter().map(|(_, c)| c).collect();
                            match serde_json::from_str::<serde_json::Value>(&body_str) {
                                Ok(body) => {
                                    crate::security::log_redact::proxy_request_received(
                                        &req_id,
                                        &pending.path,
                                        Some(body_str.len()),
                                    );
                                    dispatch_proxy_request(
                                        req_id,
                                        pending.path,
                                        body,
                                        proxy_state_msg_clone.clone(),
                                        dc_ctx.clone(),
                                        remote_peer_msg.clone(),
                                        ws_write_msg.clone(),
                                        shared_state_msg.clone(),
                                        cluster_id_msg.clone(),
                                        approved_models_msg.clone(),
                                    );
                                }
                                Err(err) => {
                                    println!(
                                        "❌ Failed to reassemble chunked proxy request [{}]: {}",
                                        req_id, err
                                    );
                                }
                            }
                        }
                    }
                    DataChannelMessage::ProxyResponseChunk { req_id, chunk } => {
                        if !is_provider_msg {
                            let chunk_len = chunk.len() as u64;
                            let partial_tokens = {
                                let mut exchanges = active_exchanges_msg.lock().await;
                                if let Some(meta) = exchanges.get_mut(&req_id) {
                                    meta.bytes_received += chunk_len;
                                    accumulate_and_parse_usage(&mut meta.response_buffer, &chunk)
                                        .map(|u| u.total_tokens)
                                        .unwrap_or(0)
                                } else {
                                    0
                                }
                            };
                            proxy_state_msg_clone.stats_stream_progress(
                                &req_id,
                                chunk_len,
                                partial_tokens,
                            );
                        }

                        let reqs = pending_reqs.lock().await;
                        if let Some(tx) = reqs.get(&req_id) {
                            let _ = tx.send(Ok(chunk)).await;
                        }
                    }
                    DataChannelMessage::ProxyResponseDone { req_id } => {
                        if !is_provider_msg {
                            let meta = {
                                let mut exchanges = active_exchanges_msg.lock().await;
                                exchanges.remove(&req_id)
                            };

                            if let Some(meta) = meta {
                                let usage = parse_usage_from_buffer(&meta.response_buffer)
                                    .unwrap_or(TokenUsage {
                                        prompt_tokens: 0,
                                        completion_tokens: 0,
                                        total_tokens: 0,
                                    });

                                proxy_state_msg_clone.stats_request_complete(
                                    &req_id,
                                    usage.total_tokens,
                                    meta.bytes_received,
                                );

                                let peer_id = shared_state_msg.lock().await.peer_id.clone();
                                let remote_peer_id = meta.remote_peer_id.clone();
                                let model = meta.model.clone();
                                record_token_usage_local(
                                    proxy_state_msg_clone.tx_store.clone(),
                                    req_id.clone(),
                                    "consumer",
                                    peer_id.clone(),
                                    remote_peer_id.clone(),
                                    model.clone(),
                                    usage.prompt_tokens,
                                    usage.completion_tokens,
                                    usage.total_tokens,
                                );
                                let report = ProtocolMessage::ReportTokenUsage {
                                    req_id: req_id.clone(),
                                    role: "consumer".to_string(),
                                    peer_id,
                                    remote_peer_id,
                                    model,
                                    path: meta.path,
                                    prompt_tokens: usage.prompt_tokens,
                                    completion_tokens: usage.completion_tokens,
                                    total_tokens: usage.total_tokens,
                                    bytes_sent: meta.bytes_sent,
                                    bytes_received: meta.bytes_received,
                                    duration_ms: meta.started_at.elapsed().as_millis() as u64,
                                };
                                if let Ok(text) = serde_json::to_string(&report) {
                                    let _ = ws_write_msg
                                        .lock()
                                        .await
                                        .send(Message::Text(text))
                                        .await;
                                }
                            }
                        }

                        let mut reqs = pending_reqs.lock().await;
                        reqs.remove(&req_id);
                    }
                    DataChannelMessage::ProxyResponseError { req_id, error } => {
                        proxy_state_msg_clone.stats_request_error(&req_id);
                        let mut reqs = pending_reqs.lock().await;
                        if let Some(tx) = reqs.remove(&req_id) {
                            let _ = tx.send(Err(error)).await;
                        }
                    }
                    DataChannelMessage::Ping => {
                        let _ = dc_ctx
                            .send_text(serde_json::to_string(&DataChannelMessage::Pong).unwrap())
                            .await;
                    }
                    DataChannelMessage::Pong => {}
                }
            } else {
                println!(
                    "\n📬 Raw Payload Received: {}",
                    String::from_utf8_lossy(&msg.data)
                );
            }
        })
    }));
}

fn dispatch_proxy_request(
    req_id: String,
    path: String,
    body: serde_json::Value,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    dc: Arc<RTCDataChannel>,
    consumer_peer_id: String,
    ws_write: Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >>>,
    shared_state: SharedState,
    cluster_id: String,
    approved_proxy_models: Option<Arc<Mutex<HashMap<String, String>>>>,
) {
    crate::security::log_redact::proxy_request_received(&req_id, &path, None);

    tokio::spawn(async move {
        let request_bytes = serde_json::to_string(&body).unwrap_or_default().len() as u64;
        let model = body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        proxy_state.stats_request_start(&req_id, &consumer_peer_id, &model, request_bytes);

        if proxy_state
            .tx_store
            .is_peer_blocked(&consumer_peer_id)
            .await
            .unwrap_or(false)
        {
            println!(
                "Rejected proxy request [{}] — consumer {} is blocked",
                req_id, consumer_peer_id
            );
            proxy_state.stats_request_error(&req_id);
            let err_msg = DataChannelMessage::ProxyResponseError {
                req_id: req_id.clone(),
                error: "Peer is blocked".to_string(),
            };
            let _ = send_dc_message(&dc, &err_msg).await;
            return;
        }

        let accepting = {
            let cfg = proxy_state.client_config.read().await;
            find_cluster(&cfg, &cluster_id)
                .map(cluster_accepts_jobs)
                .unwrap_or(true)
        };
        if !accepting {
            println!(
                "Rejected proxy request [{}] — maintenance mode (cluster: {})",
                req_id, cluster_id
            );
            proxy_state.stats_request_error(&req_id);
            let err_msg = DataChannelMessage::ProxyResponseError {
                req_id: req_id.clone(),
                error: "Peer is in maintenance mode".to_string(),
            };
            let _ = send_dc_message(&dc, &err_msg).await;
            return;
        }

        if let Some(models) = approved_proxy_models.as_ref() {
            if let Some(expected) = models.lock().await.remove(&consumer_peer_id) {
                if expected != model {
                    println!(
                        "Rejected proxy request [{}] — model mismatch (expected {expected}, got {model})",
                        req_id
                    );
                    proxy_state.stats_request_error(&req_id);
                    let err_msg = DataChannelMessage::ProxyResponseError {
                        req_id: req_id.clone(),
                        error: "Model does not match approved connection".to_string(),
                    };
                    let _ = send_dc_message(&dc, &err_msg).await;
                    return;
                }
            }
        }

        let started_at = std::time::Instant::now();
        let mut body = body;
        ensure_stream_usage(&mut body);
        let default_predict = {
            let cfg = proxy_state.client_config.read().await;
            crate::client_config::effective_default_num_predict(&cfg)
        };
        let default_ctx = {
            let cfg = proxy_state.client_config.read().await;
            crate::client_config::effective_default_num_ctx(&cfg)
        };
        ensure_ollama_predict_options_with_default(&mut body, default_predict);
        ensure_ollama_ctx_options_with_default(&mut body, default_ctx);

        println!(
            "Forwarding proxy request [{}] model={} path={} to backend...",
            req_id, model, path
        );

        match proxy_state.forward_chat_stream(&path, &body, &model).await {
            Ok(mut stream) => {
                use futures_util::StreamExt;

                let mut response_buffer = String::new();
                let mut bytes_received = 0u64;

                while let Some(Ok(chunk_bytes)) = stream.next().await {
                    if let Ok(chunk_str) = String::from_utf8(chunk_bytes.to_vec()) {
                        bytes_received += chunk_str.len() as u64;
                        let partial_tokens = accumulate_and_parse_usage(&mut response_buffer, &chunk_str)
                            .map(|u| u.total_tokens)
                            .unwrap_or(0);
                        proxy_state.stats_stream_progress(
                            &req_id,
                            chunk_str.len() as u64,
                            partial_tokens,
                        );
                        let _ = send_proxy_response_chunk(&dc, &req_id, &chunk_str).await;
                    }
                }

                let done_msg = DataChannelMessage::ProxyResponseDone {
                    req_id: req_id.clone(),
                };
                let _ = send_dc_message(&dc, &done_msg).await;
                println!("✅ Successfully streamed response for [{}]", req_id);

                let usage = parse_usage_from_buffer(&response_buffer).unwrap_or(TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                });

                proxy_state.stats_request_complete(
                    &req_id,
                    usage.total_tokens,
                    bytes_received,
                );

                let peer_id = shared_state.lock().await.peer_id.clone();
                crate::token_usage::publish_token_usage_report(
                    proxy_state.tx_store.clone(),
                    ws_write,
                    req_id,
                    "provider",
                    peer_id,
                    consumer_peer_id,
                    model,
                    path,
                    usage,
                    bytes_received,
                    request_bytes,
                    started_at.elapsed().as_millis() as u64,
                );
            }
            Err(err) => {
                println!(
                    "❌ Ollama proxy forwarding failed for [{}]: {}",
                    req_id, err
                );
                proxy_state.stats_request_error(&req_id);
                let error_msg = DataChannelMessage::ProxyResponseError {
                    req_id: req_id.clone(),
                    error: format!("Proxy Error: {}", err),
                };
                let _ = send_dc_message(&dc, &error_msg).await;
            }
        }
    });
}
