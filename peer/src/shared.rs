use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    #[default]
    Cluster,
    Swarm,
    Both,
}

#[derive(Debug)]
pub struct ProxyRequestCommand {
    pub target_peer: Option<String>,
    pub model: String,
    pub path: String,
    pub body: serde_json::Value,
    pub response_tx: mpsc::Sender<Result<String, String>>,
    pub cluster_id: Option<String>,
    pub swarm_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ModelStartAction {
    Request {
        req_id: String,
        model: String,
        cluster_id: Option<String>,
        swarm_id: Option<String>,
    },
    Respond {
        req_id: String,
        accept: bool,
        cluster_id: Option<String>,
        swarm_id: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum ConnectionAction {
    Respond {
        req_id: String,
        accept: bool,
        cluster_id: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncomingConnectionOfferState {
    pub req_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    pub requested_by: String,
    pub received_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelStartRequestState {
    pub req_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarm_id: Option<String>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub updated_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelStartOfferState {
    pub req_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarm_id: Option<String>,
    pub requested_by: String,
    pub estimated_vram_mb: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_size_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_on_requester: Option<bool>,
    pub received_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalModelRunState {
    pub job_id: String,
    pub model: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub updated_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerInfo {
    pub lat: f64,
    pub lon: f64,
    pub asn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuDeviceInfo {
    pub index: u8,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pci_bus_id: Option<String>,
    pub utilization_pct: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_utilization_pct: Option<u8>,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature_c: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_draw_w: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_limit_w: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_speed_pct: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuHostStatus {
    pub available: bool,
    pub utilization_pct: u8,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub memory_free_mb: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cuda_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature_c: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_utilization_pct: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_draw_w: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub devices: Option<Vec<GpuDeviceInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuSample {
    pub ts_unix: u64,
    pub utilization_pct: u8,
    pub memory_used_mb: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_utilization_pct: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature_c: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_draw_w: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GpuHistory {
    pub short: Vec<GpuSample>,
    pub long: Vec<GpuSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GpuThermalGuardStatus {
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_temp_c: Option<u8>,
    pub threshold_c: u8,
    pub caused_pause: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    pub cluster_id: String,
    pub name: Option<String>,
    pub visibility: Option<String>,
    pub lobby_connected: bool,
    pub outbound_peers: usize,
    pub inbound_peers: usize,
    pub accepting_jobs: bool,
    pub connected: bool,
    pub network_models: Vec<serde_json::Value>,
    pub schedule_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_next_transition_at: Option<u64>,
    pub schedule_inside_window: bool,
    pub thermal_paused: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmStatus {
    pub swarm_id: String,
    pub name: Option<String>,
    pub p2p_connected: bool,
    pub outbound_peers: usize,
    pub inbound_peers: usize,
    pub accepting_jobs: bool,
    pub connected: bool,
    pub network_models: Vec<serde_json::Value>,
    pub schedule_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_next_transition_at: Option<u64>,
    pub schedule_inside_window: bool,
    pub thermal_paused: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerTrafficStats {
    pub tokens_per_sec: f64,
    pub requests_per_min: f64,
    pub session_tokens: u64,
    pub lifetime_tokens: u64,
    pub total_requests: u32,
    pub active_requests: u32,
    pub error_count: u32,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub avg_duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_model: Option<String>,
    pub token_mismatch_count: u32,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerConnectionView {
    pub peer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarm_id: Option<String>,
    pub direction: String,
    pub since_secs: u64,
    pub data_channel_open: bool,
    pub blocked: bool,
    #[serde(default)]
    pub attestation_flags: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<PeerTrafficStats>,
}

pub struct AppState {
    pub my_id: String,
    pub peer_id: String,
    pub service_id: String,
    pub clusters: Vec<ClusterStatus>,
    pub swarms: Vec<SwarmStatus>,
    pub credit_balance: i64,
    pub swarm_proxy_request_tx: mpsc::Sender<ProxyRequestCommand>,
    pub swarm_model_start_tx: mpsc::Sender<ModelStartAction>,
    pub local_models: Vec<String>,
    pub local_models_full: Vec<serde_json::Value>,
    pub network_models: Vec<serde_json::Value>,
    pub proxy_request_tx: mpsc::Sender<ProxyRequestCommand>,
    pub model_start_tx: mpsc::Sender<ModelStartAction>,
    pub connection_tx: mpsc::Sender<ConnectionAction>,
    pub outgoing_model_requests: Vec<ModelStartRequestState>,
    pub incoming_model_offers: Vec<ModelStartOfferState>,
    pub incoming_connection_offers: Vec<IncomingConnectionOfferState>,
    pub local_model_runs: Vec<LocalModelRunState>,
    pub local_model_collisions: Vec<crate::llm_registry::ModelCollision>,
    pub peer_info: Option<PeerInfo>,
    pub last_advertised_hash: Option<String>,
    pub show_info_cache: HashMap<String, serde_json::Value>,
    pub last_gpu_host: Option<GpuHostStatus>,
    pub gpu_history: GpuHistory,
    pub lobby_connected: bool,
    pub peer_connections: Vec<PeerConnectionView>,
    pub gpu_thermal_guard: GpuThermalGuardStatus,
}

pub type SharedState = Arc<Mutex<AppState>>;

#[derive(Debug)]
pub enum RuntimeEvent {
    SetupComplete,
    SetupInvalidated,
    SwarmsChanged,
}

pub type RuntimeEventTx = mpsc::Sender<RuntimeEvent>;

#[derive(Debug, Clone)]
pub struct TrackedPeer {
    pub peer_id: String,
    pub cluster_id: Option<String>,
    pub swarm_id: Option<String>,
    pub direction: PeerDirection,
    pub connected_at: Instant,
    pub data_channel_open: bool,
    pub attestation_flags: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerDirection {
    Inbound,
    Outbound,
}

pub type PeerRegistry = Arc<Mutex<HashMap<String, TrackedPeer>>>;

pub fn peer_registry_key(peer_id: &str, cluster_id: &str) -> String {
    format!("{peer_id}@cluster:{cluster_id}")
}

pub fn swarm_peer_registry_key(peer_id: &str, swarm_id: &str) -> String {
    format!("{peer_id}@swarm:{swarm_id}")
}

#[derive(Debug, Clone)]
pub enum PeerModerationAction {
    CloseConnections { peer_id: String },
    Report { peer_id: String, reason: Option<String> },
}
