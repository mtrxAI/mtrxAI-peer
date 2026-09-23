use serde::{Deserialize, Serialize};
use std::fs;
use uuid::Uuid;

pub const CLIENT_CONFIG_PATH: &str = "client_config.json";
pub const LEGACY_PEER_CONFIG_PATH: &str = "peer_config.json";
pub const DEFAULT_PUBLIC_CLUSTER: &str = "europe";

pub const PUBLIC_CONTINENTS: &[&str] = &[
    "africa",
    "americas",
    "antarctica",
    "asia",
    "europe",
    "oceania",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterMembership {
    #[serde(rename = "cluster_id", alias = "room_id")]
    pub cluster_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepting_jobs: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connected: Option<bool>,
    /// Room password used to derive E2EE keys (stored locally only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_e2ee: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_tee: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_attestation_flags: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_end: Option<String>,
}

pub fn cluster_accepts_jobs(cluster: &ClusterMembership) -> bool {
    cluster.accepting_jobs.unwrap_or(true)
}

pub fn cluster_is_connected(cluster: &ClusterMembership) -> bool {
    cluster.connected.unwrap_or(true)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwarmMembership {
    pub swarm_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub p2p_token: String,
    #[serde(default)]
    pub bootnodes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepting_jobs: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connected: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_e2ee: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_tee: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_attestation_flags: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_end: Option<String>,
}

pub fn swarm_accepts_jobs(swarm: &SwarmMembership) -> bool {
    swarm.accepting_jobs.unwrap_or(true)
}

pub fn swarm_is_connected(swarm: &SwarmMembership) -> bool {
    swarm.connected.unwrap_or(true)
}

pub fn parse_network_mode(raw: &str) -> crate::shared::NetworkMode {
    match raw.trim().to_ascii_lowercase().as_str() {
        "swarm" => crate::shared::NetworkMode::Swarm,
        "both" => crate::shared::NetworkMode::Both,
        _ => crate::shared::NetworkMode::Cluster,
    }
}

pub fn network_mode_from_env() -> crate::shared::NetworkMode {
    std::env::var("MTRXAI_P2P_MODE")
        .map(|v| parse_network_mode(&v))
        .unwrap_or(crate::shared::NetworkMode::Cluster)
}

pub fn default_bootnodes_from_env() -> Vec<String> {
    std::env::var("MTRXAI_BOOTNODES")
        .ok()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

pub async fn fetch_bootnodes_from_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> Vec<String> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/public/p2p/bootnodes");
    let Ok(resp) = http_client.get(&url).send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    #[derive(Deserialize)]
    struct BootnodesResponse {
        bootnodes: Vec<String>,
    }
    resp.json::<BootnodesResponse>()
        .await
        .map(|b| b.bootnodes)
        .unwrap_or_default()
}

pub async fn resolve_swarm_bootnodes(
    http_client: &reqwest::Client,
    lobby_host: &str,
    membership: &SwarmMembership,
) -> Vec<String> {
    if !membership.bootnodes.is_empty() {
        return membership.bootnodes.clone();
    }
    let from_env = default_bootnodes_from_env();
    if !from_env.is_empty() {
        return from_env;
    }
    fetch_bootnodes_from_lobby(http_client, lobby_host).await
}

const DEFAULT_GOOGLE_STUN: &str = "stun:stun.l.google.com:19302";

/// Parse `MTRXAI_ICE_SERVERS` JSON array override (dev/standalone).
pub fn default_ice_servers_from_env() -> Vec<mtrxai_protocol::IceServerConfig> {
    let Ok(raw) = std::env::var("MTRXAI_ICE_SERVERS") else {
        return Vec::new();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(trimmed).unwrap_or_else(|e| {
        eprintln!("⚠️ MTRXAI_ICE_SERVERS JSON parse failed: {e}");
        Vec::new()
    })
}

pub fn default_google_stun_servers() -> Vec<mtrxai_protocol::IceServerConfig> {
    vec![mtrxai_protocol::IceServerConfig {
        urls: vec![DEFAULT_GOOGLE_STUN.to_string()],
        username: None,
        credential: None,
    }]
}

#[derive(Debug, Deserialize)]
struct IceServersResponse {
    ice_servers: Vec<mtrxai_protocol::IceServerConfig>,
}

pub async fn fetch_public_ice_servers_from_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> Vec<mtrxai_protocol::IceServerConfig> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/public/webrtc/ice-servers");
    let Ok(resp) = http_client.get(&url).send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    resp.json::<IceServersResponse>()
        .await
        .map(|b| b.ice_servers)
        .unwrap_or_default()
}

/// Authenticated ICE fetch (includes short-lived TURN credentials when lobby has TURN enabled).
pub async fn fetch_ice_servers_from_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    peer_id: &str,
    tx_store: &crate::tx_db::TxStore,
) -> Vec<mtrxai_protocol::IceServerConfig> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/webrtc/ice-servers");
    let Ok((timestamp, signature)) = crate::security::build_ws_auth_query(tx_store, peer_id) else {
        return Vec::new();
    };
    let Ok(resp) = http_client
        .get(&url)
        .query(&[
            ("peer_id", peer_id),
            ("timestamp", timestamp.as_str()),
            ("signature", signature.as_str()),
        ])
        .send()
        .await
    else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    resp.json::<IceServersResponse>()
        .await
        .map(|b| b.ice_servers)
        .unwrap_or_default()
}

/// Resolve ICE servers: env override → authenticated lobby → public STUN → Google STUN.
pub async fn resolve_webrtc_ice_servers(
    http_client: &reqwest::Client,
    lobby_host: &str,
    peer_id: Option<&str>,
    tx_store: Option<&crate::tx_db::TxStore>,
) -> Vec<mtrxai_protocol::IceServerConfig> {
    let from_env = default_ice_servers_from_env();
    if !from_env.is_empty() {
        return from_env;
    }
    if let (Some(peer_id), Some(store)) = (peer_id.filter(|id| !id.trim().is_empty()), tx_store) {
        let auth = fetch_ice_servers_from_lobby(http_client, lobby_host, peer_id, store).await;
        if !auth.is_empty() {
            return auth;
        }
    }
    let public = fetch_public_ice_servers_from_lobby(http_client, lobby_host).await;
    if !public.is_empty() {
        return public;
    }
    default_google_stun_servers()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomModelEntry {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(
        default,
        rename = "toolCalling",
        skip_serializing_if = "Option::is_none"
    )]
    pub tool_calling: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision: Option<bool>,
    #[serde(
        default,
        rename = "maxInputTokens",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_input_tokens: Option<u64>,
    #[serde(
        default,
        rename = "maxOutputTokens",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<u64>,
}

impl CustomModelEntry {
    pub fn display_name(&self) -> String {
        self.name
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(self.id.as_str())
            .to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmServerEntry {
    pub id: String,
    pub kind: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default)]
    pub attached: bool,
    #[serde(default)]
    pub order: u32,
    #[serde(default = "default_server_source")]
    pub source: String,
    #[serde(default, rename = "apiType", skip_serializing_if = "Option::is_none")]
    pub api_type: Option<String>,
    #[serde(default)]
    pub models: Vec<CustomModelEntry>,
    /// When false, models on this server stay local and are not advertised to clusters.
    #[serde(default = "default_true")]
    pub advertise_to_cluster: bool,
}

pub fn is_custom_server_kind(kind: &str) -> bool {
    matches!(kind.to_lowercase().as_str(), "custom" | "customendpoint")
}

pub fn is_inference_cell_kind(kind: &str) -> bool {
    matches!(
        kind.to_lowercase().as_str(),
        "inference-cell" | "inference_cell"
    )
}

pub fn validate_custom_server_entry(entry: &LlmServerEntry) -> Result<(), String> {
    if !is_custom_server_kind(&entry.kind) {
        return Ok(());
    }
    if entry.models.is_empty() {
        return Err("custom endpoint requires at least one model".to_string());
    }
    for model in &entry.models {
        if model.id.trim().is_empty() {
            return Err("each custom model must have a non-empty id".to_string());
        }
    }
    if let Some(api_type) = entry.api_type.as_deref() {
        if !api_type.is_empty() && api_type != "chat-completions" {
            return Err(format!(
                "unsupported apiType '{}': only chat-completions is supported",
                api_type
            ));
        }
    }
    Ok(())
}

fn default_server_source() -> String {
    "manual".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    #[serde(default)]
    pub peer_id: Option<String>,
    #[serde(default)]
    pub service_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, rename = "clusters", alias = "rooms")]
    pub clusters: Vec<ClusterMembership>,
    #[serde(default)]
    pub swarms: Vec<SwarmMembership>,
    #[serde(default)]
    pub p2p_mode: crate::shared::NetworkMode,
    #[serde(default, rename = "cluster", alias = "room", skip_serializing)]
    pub cluster: Option<String>,
    #[serde(default = "default_lobby_host")]
    pub lobby_host: String,
    #[serde(default)]
    pub setup_complete: bool,
    #[serde(default, skip_serializing)]
    pub llm_backend: Option<String>,
    #[serde(default, skip_serializing)]
    pub llm_url: Option<String>,
    #[serde(default)]
    pub llm_servers: Vec<LlmServerEntry>,
    /// Model names excluded from `/api/tags`, `/v1/models`, and cluster/swarm advertisement.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hidden_models: Vec<String>,
    #[serde(default = "default_auto_approve_run_model_request")]
    pub auto_approve_run_model_request: bool,
    #[serde(default = "default_auto_approve_inference_connections")]
    pub auto_approve_inference_connections: bool,
    #[serde(default)]
    pub allow_unattested_peers: bool,
    /// Accept self-signed / invalid TLS for local LLM backends (e.g. inference-cell).
    #[serde(default = "default_ollama_tls_insecure")]
    pub ollama_tls_insecure: bool,
    /// Default Ollama `options.num_predict` cap (overrides env `MTRXAI_DEFAULT_NUM_PREDICT` when set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_num_predict: Option<u64>,
    /// Default Ollama `options.num_ctx` window (overrides env `MTRXAI_DEFAULT_NUM_CTX` when set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_num_ctx: Option<u64>,
    /// Optional bearer token for local HTTP proxy auth (`MTRXAI_PROXY_TOKEN` override).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_tee_for_inference: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_cc_mode: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_schedule_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_schedule_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_schedule_end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_thermal_guard_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_thermal_threshold_c: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_thermal_duration_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_thermal_cooldown_c: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_thermal_auto_resume: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GpuThermalSettings {
    pub enabled: bool,
    pub threshold_c: u8,
    pub duration_secs: u64,
    pub cooldown_c: u8,
    pub auto_resume: bool,
}

pub fn effective_gpu_thermal_settings(cfg: &ClientConfig) -> GpuThermalSettings {
    GpuThermalSettings {
        enabled: cfg.gpu_thermal_guard_enabled.unwrap_or(true),
        threshold_c: cfg.gpu_thermal_threshold_c.unwrap_or(85),
        duration_secs: cfg.gpu_thermal_duration_secs.unwrap_or(60),
        cooldown_c: cfg.gpu_thermal_cooldown_c.unwrap_or(75),
        auto_resume: cfg.gpu_thermal_auto_resume.unwrap_or(true),
    }
}

fn default_auto_approve_run_model_request() -> bool {
    false
}

fn default_auto_approve_inference_connections() -> bool {
    true
}

fn default_ollama_tls_insecure() -> bool {
    true
}

fn default_lobby_host() -> String {
    "127.0.0.1:8080".to_string()
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            peer_id: None,
            service_id: None,
            service_name: None,
            clusters: Vec::new(),
            swarms: Vec::new(),
            p2p_mode: crate::shared::NetworkMode::Cluster,
            cluster: None,
            lobby_host: default_lobby_host(),
            setup_complete: false,
            llm_backend: None,
            llm_url: None,
            llm_servers: Vec::new(),
            hidden_models: Vec::new(),
            auto_approve_run_model_request: default_auto_approve_run_model_request(),
            auto_approve_inference_connections: default_auto_approve_inference_connections(),
            allow_unattested_peers: false,
            ollama_tls_insecure: default_ollama_tls_insecure(),
            default_num_predict: None,
            default_num_ctx: None,
            proxy_token: None,
            require_tee_for_inference: None,
            gpu_cc_mode: None,
            default_schedule_enabled: None,
            default_schedule_start: None,
            default_schedule_end: None,
            gpu_thermal_guard_enabled: None,
            gpu_thermal_threshold_c: None,
            gpu_thermal_duration_secs: None,
            gpu_thermal_cooldown_c: None,
            gpu_thermal_auto_resume: None,
        }
    }
}

/// Configured default, then `MTRXAI_OLLAMA_TLS_INSECURE` env var (dev override).
pub fn effective_ollama_tls_insecure(cfg: &ClientConfig) -> bool {
    if cfg.ollama_tls_insecure {
        return true;
    }
    match std::env::var("MTRXAI_OLLAMA_TLS_INSECURE") {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"),
        Err(_) => false,
    }
}

/// Configured default, then `MTRXAI_DEFAULT_NUM_PREDICT` env var.
pub fn effective_default_num_predict(cfg: &ClientConfig) -> Option<u64> {
    cfg.default_num_predict.or_else(|| {
        std::env::var("MTRXAI_DEFAULT_NUM_PREDICT")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n > 0)
    })
}

/// Configured default, then `MTRXAI_DEFAULT_NUM_CTX` env var.
pub fn effective_default_num_ctx(cfg: &ClientConfig) -> Option<u64> {
    cfg.default_num_ctx.or_else(|| {
        std::env::var("MTRXAI_DEFAULT_NUM_CTX")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n > 0)
    })
}

pub fn has_attached_llm_servers(config: &ClientConfig) -> bool {
    config.llm_servers.iter().any(|s| s.attached)
}

#[derive(Debug, Deserialize)]
pub struct RegisterPeerResponse {
    pub peer_id: Uuid,
    pub service_id: Uuid,
    pub service_name: String,
    pub credit_balance: i64,
    #[serde(default)]
    pub ice_servers: Option<Vec<mtrxai_protocol::IceServerConfig>>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterResponse {
    #[serde(rename = "cluster_id", alias = "room_id")]
    pub cluster_id: Uuid,
    pub name: Option<String>,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub continent: Option<String>,
    #[serde(default)]
    pub require_e2ee: Option<bool>,
    #[serde(default)]
    pub required_attestation_flags: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicClusterView {
    #[serde(rename = "cluster_id", alias = "room_id")]
    pub cluster_id: Uuid,
    pub name: Option<String>,
    pub continent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterListView {
    #[serde(rename = "cluster_id", alias = "room_id")]
    pub cluster_id: Uuid,
    pub name: Option<String>,
    pub visibility: String,
    pub continent: Option<String>,
    pub password_protected: bool,
    pub required_attestation_flags: u64,
}

pub fn client_config_path() -> String {
    std::env::var("MTRXAI_CONFIG_PATH").unwrap_or_else(|_| CLIENT_CONFIG_PATH.to_string())
}

pub fn load_client_config() -> ClientConfig {
    let path = client_config_path();
    let mut cfg = if let Ok(content) = fs::read_to_string(&path) {
        serde_json::from_str::<ClientConfig>(&content).unwrap_or_default()
    } else if let Ok(content) = fs::read_to_string(LEGACY_PEER_CONFIG_PATH) {
        if let Ok(legacy) = serde_json::from_str::<LegacyPeerConfig>(&content) {
            let setup_complete = !legacy.service_id.is_empty() && !legacy.peer_id.is_empty();
            ClientConfig {
                peer_id: Some(legacy.peer_id),
                service_id: Some(legacy.service_id),
                service_name: None,
                clusters: Vec::new(),
                swarms: Vec::new(),
                p2p_mode: crate::shared::NetworkMode::Cluster,
                cluster: None,
                lobby_host: default_lobby_host(),
                setup_complete,
                llm_backend: Some("ollama".to_string()),
                llm_url: Some("http://127.0.0.1:11434".to_string()),
                llm_servers: Vec::new(),
                hidden_models: Vec::new(),
                auto_approve_run_model_request: default_auto_approve_run_model_request(),
                auto_approve_inference_connections: default_auto_approve_inference_connections(),
                allow_unattested_peers: false,
                ollama_tls_insecure: default_ollama_tls_insecure(),
                default_num_predict: None,
                default_num_ctx: None,
                proxy_token: None,
                require_tee_for_inference: None,
                gpu_cc_mode: None,
                default_schedule_enabled: None,
                default_schedule_start: None,
                default_schedule_end: None,
                gpu_thermal_guard_enabled: None,
                gpu_thermal_threshold_c: None,
                gpu_thermal_duration_secs: None,
                gpu_thermal_cooldown_c: None,
                gpu_thermal_auto_resume: None,
            }
        } else {
            ClientConfig::default()
        }
    } else {
        ClientConfig::default()
    };
    crate::llm_registry::migrate_config(&mut cfg);
    cfg.lobby_host = crate::lobby_url::normalize_lobby_host(&cfg.lobby_host);
    cfg
}

#[derive(Debug, Deserialize)]
struct LegacyPeerConfig {
    peer_id: String,
    service_id: String,
}

/// Clear stale local peer identity when the lobby rejects stored credentials.
pub fn invalidate_local_peer_identity(config: &mut ClientConfig) {
    config.peer_id = None;
    config.service_id = None;
    config.service_name = None;
    config.setup_complete = false;
    config.clusters.clear();
    config.swarms.clear();
    config.cluster = None;
}

pub fn save_client_config(config: &ClientConfig) -> anyhow::Result<()> {
    let path = client_config_path();
    if let Some(parent) = std::path::Path::new(&path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let json = serde_json::to_string_pretty(config)?;
    fs::write(path, json)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct ServiceCreditsResponse {
    pub service_id: Uuid,
    pub balance: i64,
}

pub async fn fetch_service_credits(
    http_client: &reqwest::Client,
    lobby_host: &str,
    service_id: &str,
) -> Option<i64> {
    let service_id = Uuid::parse_str(service_id).ok()?;
    let url = crate::lobby_url::lobby_api_url(
        lobby_host,
        &format!("/api/public/services/{service_id}/credits"),
    );
    let resp = http_client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<ServiceCreditsResponse>()
        .await
        .ok()
        .map(|body| body.balance)
}

#[derive(Debug, Deserialize)]
pub struct LobbyConfigResponse {
    pub peer_registration: String,
}

pub fn is_invite_only_registration(config: &LobbyConfigResponse) -> bool {
    config.peer_registration == "invite_only" || config.peer_registration == "invitation"
}

pub fn is_register_mode(config: &LobbyConfigResponse) -> bool {
    config.peer_registration == "register"
}

pub fn requires_credentials_registration(config: &LobbyConfigResponse) -> bool {
    is_invite_only_registration(config) || is_register_mode(config)
}

pub async fn beta_signup_with_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    email: &str,
    service_name: &str,
    service_password: &str,
    terms_accepted: bool,
) -> anyhow::Result<()> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/beta/signup");
    let resp = http_client
        .post(&url)
        .json(&serde_json::json!({
            "email": email,
            "service_name": service_name,
            "service_password": service_password,
            "terms_accepted": terms_accepted,
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Beta signup failed ({}): {}", status, text);
    }
    Ok(())
}

pub async fn fetch_lobby_config(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> anyhow::Result<LobbyConfigResponse> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/public/config");
    let resp = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Lobby unreachable at {url}: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Lobby config fetch failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub fn apply_register_response(config: &mut ClientConfig, reg: &RegisterPeerResponse) {
    config.peer_id = Some(reg.peer_id.to_string());
    config.service_id = Some(reg.service_id.to_string());
    config.service_name = Some(reg.service_name.clone());
}

pub async fn register_with_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    peer_id: Option<&str>,
    service_name: Option<&str>,
    service_password: Option<&str>,
    create_new_service: bool,
    contact_email: Option<&str>,
    tx_store: &crate::tx_db::TxStore,
) -> anyhow::Result<RegisterPeerResponse> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/peers/register");
    let attestation =
        crate::attestation::maybe_build_attestation_proof(http_client, lobby_host).await?;
    let mut body = serde_json::json!({
        "peer_id": peer_id.and_then(|id| Uuid::parse_str(id).ok()),
        "service_name": service_name.filter(|s| !s.trim().is_empty()),
        "service_password": service_password.filter(|s| !s.trim().is_empty()),
        "create_new_service": create_new_service,
        "contact_email": contact_email.filter(|s| !s.trim().is_empty()),
    });
    if let Some(proof) = attestation {
        body["attestation"] = serde_json::to_value(proof)?;
    }

    let pk = crate::security::peer_public_key_hex_async(tx_store).await?;
    body["public_key"] = serde_json::Value::String(pk);
    if let Some(existing_peer) = peer_id.filter(|id| !id.trim().is_empty()) {
        match crate::security::build_peer_auth_proof_async(tx_store, existing_peer).await {
            Ok(auth) => {
                body["peer_auth"] = serde_json::to_value(auth)?;
            }
            Err(e) => {
                eprintln!("⚠️ peer auth proof failed: {e}");
            }
        }
    }

    let resp = http_client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Lobby unreachable at {url}: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Peer registration failed ({}): {}", status, text);
    }

    Ok(resp.json().await?)
}

/// Re-register with the lobby to refresh `last_seen_at` and sync the device public key
/// when the local signing key no longer matches the server (e.g. after tx_db reset).
pub async fn sync_peer_device_key_with_lobby(
    http_client: &reqwest::Client,
    config: &mut ClientConfig,
    tx_store: &crate::tx_db::TxStore,
) -> anyhow::Result<()> {
    let Some(peer_id) = config.peer_id.as_deref().filter(|id| !id.trim().is_empty()) else {
        return Ok(());
    };

    let (service_name, service_password) = service_credentials_for_registration();

    let reg = register_with_lobby(
        http_client,
        &config.lobby_host,
        Some(peer_id),
        service_name.as_deref(),
        service_password.as_deref(),
        false,
        None,
        tx_store,
    )
    .await?;

    apply_register_response(config, &reg);
    save_client_config(config)?;
    Ok(())
}

pub async fn list_public_clusters(
    http_client: &reqwest::Client,
    lobby_host: &str,
) -> anyhow::Result<Vec<PublicClusterView>> {
    let clusters = list_clusters_with_lobby(http_client, lobby_host, None, None).await?;
    Ok(clusters
        .into_iter()
        .map(|c| PublicClusterView {
            cluster_id: c.cluster_id,
            name: c.name,
            continent: c.continent,
        })
        .collect())
}

/// List clusters visible to the caller: public only, or public + peer-private when `peer_auth` is sent.
pub async fn list_clusters_with_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    peer_id: Option<&str>,
    tx_store: Option<&crate::tx_db::TxStore>,
) -> anyhow::Result<Vec<ClusterListView>> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/clusters");
    let mut req = http_client.get(&url);
    if let (Some(peer_id), Some(store)) = (peer_id.filter(|id| !id.trim().is_empty()), tx_store) {
        let (timestamp, signature) = crate::security::build_ws_auth_query(store, peer_id)?;
        req = req.query(&[
            ("peer_id", peer_id),
            ("timestamp", timestamp.as_str()),
            ("signature", signature.as_str()),
        ]);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("List clusters failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

/// Lightweight HTTP check that the lobby server is reachable (not cluster WebSocket state).
pub async fn probe_lobby_reachable(http_client: &reqwest::Client, lobby_host: &str) -> bool {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/public/clusters");
    match http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
    {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

pub async fn create_cluster_with_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    name: Option<&str>,
    visibility: &str,
    created_by_peer_id: Option<&str>,
    password: Option<&str>,
    required_attestation_flags: Option<u64>,
) -> anyhow::Result<ClusterResponse> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/clusters/create");
    let mut body = serde_json::json!({
        "name": name.filter(|s| !s.trim().is_empty()),
        "visibility": visibility,
        "created_by_peer_id": created_by_peer_id.and_then(|id| Uuid::parse_str(id).ok()),
        "password": password.filter(|s| !s.trim().is_empty()),
    });
    if let Some(flags) = required_attestation_flags {
        body["required_attestation_flags"] = serde_json::json!(flags);
    }
    let resp = http_client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Cluster creation failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub async fn join_public_cluster_by_name(
    http_client: &reqwest::Client,
    lobby_host: &str,
    name: &str,
    password: Option<&str>,
) -> anyhow::Result<ClusterResponse> {
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/clusters/join");
    let body = serde_json::json!({
        "name": name.trim(),
        "password": password.filter(|s| !s.trim().is_empty()),
    });
    let resp = http_client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Cluster join failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub async fn join_private_cluster(
    http_client: &reqwest::Client,
    lobby_host: &str,
    cluster_id: &str,
    name: &str,
    password: Option<&str>,
) -> anyhow::Result<ClusterResponse> {
    let cluster_uuid = Uuid::parse_str(cluster_id)?;
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/clusters/join");
    let body = serde_json::json!({
        "cluster_id": cluster_uuid,
        "name": name.trim(),
        "password": password.filter(|s| !s.trim().is_empty()),
    });
    let resp = http_client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Private cluster join failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub async fn join_cluster_with_lobby(
    http_client: &reqwest::Client,
    lobby_host: &str,
    cluster_id: &str,
    password: Option<&str>,
) -> anyhow::Result<ClusterResponse> {
    let cluster_uuid = Uuid::parse_str(cluster_id)?;
    let url = crate::lobby_url::lobby_api_url(lobby_host, "/api/clusters/join");
    let body = serde_json::json!({
        "cluster_id": cluster_uuid,
        "password": password.filter(|s| !s.trim().is_empty()),
    });
    let resp = http_client.post(&url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("Cluster join failed ({}): {}", status, text);
    }
    Ok(resp.json().await?)
}

pub fn default_public_cluster_name(config: &ClientConfig) -> String {
    std::env::var("MTRXAI_CLUSTER_NAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("MTRXAI_ROOM_NAME")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .or_else(|| config.cluster.clone())
        .unwrap_or_else(|| DEFAULT_PUBLIC_CLUSTER.to_string())
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn apply_env_identity(config: &mut ClientConfig) {
    if config.peer_id.is_none() {
        if let Some(peer_id) = env_non_empty("MTRXAI_PEER_ID") {
            config.peer_id = Some(peer_id);
        }
    }
    if config.service_id.is_none() {
        if let Some(service_id) = env_non_empty("MTRXAI_SERVICE_ID") {
            config.service_id = Some(service_id);
        }
    }
    if config.service_name.is_none() {
        if let Some(service_name) = env_non_empty("MTRXAI_SERVICE_NAME") {
            config.service_name = Some(service_name);
        }
    }
}

fn env_service_credentials() -> (Option<String>, Option<String>) {
    (
        env_non_empty("MTRXAI_SERVICE_NAME"),
        env_non_empty("MTRXAI_SERVICE_PASSWORD"),
    )
}

/// Service credentials for `POST /api/peers/register`.
/// Only returns values when **both** `MTRXAI_SERVICE_NAME` and `MTRXAI_SERVICE_PASSWORD` are set.
/// Persisted `service_name` in config is never used alone (reconnect uses `peer_auth` instead).
fn service_credentials_for_registration() -> (Option<String>, Option<String>) {
    let (name, password) = env_service_credentials();
    match (name, password) {
        (Some(n), Some(p)) => (Some(n), Some(p)),
        _ => (None, None),
    }
}

fn docker_setup_deferred_message() {
    let port = env_non_empty("MTRXAI_PROXY_PORT")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(11345);
    println!("⏳ Peer registration pending — open http://127.0.0.1:{port}/ to complete setup");
    println!(
        "   Or set MTRXAI_SERVICE_NAME + MTRXAI_SERVICE_PASSWORD for auto-registration \
         (MTRXAI_PEER_ID for pre-provisioned peers)"
    );
}

async fn complete_docker_bootstrap_after_registration(
    http_client: &reqwest::Client,
    config: &mut ClientConfig,
    reg: &RegisterPeerResponse,
    cluster_name: &str,
) -> anyhow::Result<()> {
    apply_register_response(config, reg);
    let join =
        join_public_cluster_by_name(http_client, &config.lobby_host, cluster_name, None).await?;
    upsert_cluster(config, membership_from_response(&join, Some(String::new())));
    config.cluster = Some(cluster_name.to_string());
    if config.llm_backend.is_none() {
        config.llm_backend = Some("ollama".to_string());
    }
    config.setup_complete = config.peer_id.is_some()
        && config.service_id.is_some()
        && !config.clusters.is_empty()
        && has_attached_llm_servers(config);
    save_client_config(config)?;

    println!(
        "Docker bootstrap: peer {} joined public cluster {} ({})",
        reg.peer_id, cluster_name, join.cluster_id
    );
    if !config.setup_complete {
        println!("⏳ Attach an LLM server in the web UI to finish setup");
    }
    Ok(())
}

async fn try_bootstrap_registration(
    http_client: &reqwest::Client,
    config: &ClientConfig,
    lobby_cfg: Option<&LobbyConfigResponse>,
    tx_store: &crate::tx_db::TxStore,
) -> anyhow::Result<Option<RegisterPeerResponse>> {
    let (service_name, service_password) = service_credentials_for_registration();
    let has_service_credentials = service_name.is_some() && service_password.is_some();
    let requires_creds = lobby_cfg
        .map(requires_credentials_registration)
        .unwrap_or(false);

    if requires_creds {
        if config.peer_id.is_some() {
            return register_with_lobby(
                http_client,
                &config.lobby_host,
                config.peer_id.as_deref(),
                service_name.as_deref(),
                service_password.as_deref(),
                false,
                None,
                tx_store,
            )
            .await
            .map(Some);
        }

        if has_service_credentials {
            return register_with_lobby(
                http_client,
                &config.lobby_host,
                None,
                service_name.as_deref(),
                service_password.as_deref(),
                false,
                None,
                tx_store,
            )
            .await
            .map(Some);
        }

        return Ok(None);
    }

    register_with_lobby(
        http_client,
        &config.lobby_host,
        config.peer_id.as_deref(),
        service_name.as_deref(),
        service_password.as_deref(),
        false,
        None,
        tx_store,
    )
    .await
    .map(Some)
}

pub async fn bootstrap_docker_peer(
    http_client: &reqwest::Client,
    config: &mut ClientConfig,
    _group: &str,
    tx_store: &crate::tx_db::TxStore,
) -> anyhow::Result<()> {
    let force = std::env::var("MTRXAI_FORCE_DOCKER_ROOM").ok().as_deref() == Some("1")
        || std::env::var("MTRXAI_FORCE_DOCKER_CLUSTER").ok().as_deref() == Some("1");
    if !force && !config.clusters.is_empty() && config.setup_complete {
        let names: Vec<_> = config
            .clusters
            .iter()
            .filter_map(|c| c.name.as_deref())
            .collect();
        println!(
            "Skipping Docker cluster bootstrap — using configured cluster(s): {}",
            names.join(", ")
        );
        return Ok(());
    }

    let cluster_name = default_public_cluster_name(config);

    let lobby_url = crate::lobby_url::lobby_api_url(&config.lobby_host, "/api/public/peers");
    loop {
        if http_client
            .get(&lobby_url)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    apply_env_identity(config);

    let lobby_cfg = fetch_lobby_config(http_client, &config.lobby_host)
        .await
        .ok();

    match try_bootstrap_registration(http_client, config, lobby_cfg.as_ref(), tx_store).await {
        Ok(Some(reg)) => {
            complete_docker_bootstrap_after_registration(http_client, config, &reg, &cluster_name)
                .await?;
        }
        Ok(None) => docker_setup_deferred_message(),
        Err(e) => {
            eprintln!("⚠️ Docker bootstrap registration skipped: {e}");
            docker_setup_deferred_message();
        }
    }

    Ok(())
}

fn upsert_inference_cell_server(config: &mut ClientConfig, url: &str) {
    if let Some(existing) = config
        .llm_servers
        .iter_mut()
        .find(|s| is_inference_cell_kind(&s.kind))
    {
        existing.url = url.to_string();
        existing.attached = true;
        existing.source = "cli".to_string();
        return;
    }

    config.llm_servers.push(LlmServerEntry {
        id: Uuid::new_v4().to_string(),
        kind: "inference-cell".to_string(),
        url: url.to_string(),
        label: None,
        attached: true,
        order: 0,
        source: "cli".to_string(),
        api_type: None,
        models: Vec::new(),
        advertise_to_cluster: true,
    });
}

pub fn apply_cli_overrides(config: &mut ClientConfig, lobby_host: &str, llm_host: Option<&str>) {
    config.lobby_host = crate::lobby_url::normalize_lobby_host(lobby_host);
    if config.p2p_mode == crate::shared::NetworkMode::Cluster {
        config.p2p_mode = network_mode_from_env();
    }
    if let Ok(url) = std::env::var("MTRXAI_INFERENCE_CELL_URL") {
        let url = url.trim().trim_end_matches('/').to_string();
        if !url.is_empty() {
            upsert_inference_cell_server(config, &url);
            config.llm_url = Some(url);
            if config.llm_backend.is_none() {
                config.llm_backend = Some("inference-cell".to_string());
            }
            if config.peer_id.is_some()
                && config.service_id.is_some()
                && !config.clusters.is_empty()
                && has_attached_llm_servers(config)
            {
                config.setup_complete = true;
            }
            return;
        }
    }
    if let Some(host) = llm_host {
        let url = format!("http://{}", host);
        if config.llm_servers.is_empty() {
            config.llm_servers.push(LlmServerEntry {
                id: Uuid::new_v4().to_string(),
                kind: "ollama".to_string(),
                url: url.clone(),
                label: None,
                attached: true,
                order: 0,
                source: "cli".to_string(),
                api_type: None,
                models: Vec::new(),
                advertise_to_cluster: true,
            });
        }
        config.llm_url = Some(url);
        if config.llm_backend.is_none() {
            config.llm_backend = Some("ollama".to_string());
        }
        if config.peer_id.is_some() && config.service_id.is_some() && !config.clusters.is_empty() {
            config.setup_complete = true;
        }
    }
}

pub fn upsert_cluster(config: &mut ClientConfig, cluster: ClusterMembership) {
    if let Some(existing) = config
        .clusters
        .iter_mut()
        .find(|c| c.cluster_id == cluster.cluster_id)
    {
        if cluster.name.is_some() {
            existing.name = cluster.name;
        }
        if cluster.visibility.is_some() {
            existing.visibility = cluster.visibility;
        }
        return;
    }
    config.clusters.push(cluster);
}

pub fn remove_cluster(config: &mut ClientConfig, cluster_id: &str) {
    config.clusters.retain(|c| c.cluster_id != cluster_id);
    if config.clusters.is_empty() {
        config.cluster = None;
    }
}

pub fn find_cluster<'a>(
    config: &'a ClientConfig,
    cluster_id: &str,
) -> Option<&'a ClusterMembership> {
    config.clusters.iter().find(|c| c.cluster_id == cluster_id)
}

pub fn find_cluster_mut<'a>(
    config: &'a mut ClientConfig,
    cluster_id: &str,
) -> Option<&'a mut ClusterMembership> {
    config
        .clusters
        .iter_mut()
        .find(|c| c.cluster_id == cluster_id)
}

pub fn membership_from_response(
    resp: &ClusterResponse,
    room_secret: Option<String>,
) -> ClusterMembership {
    let stored = room_secret.unwrap_or_default();
    ClusterMembership {
        cluster_id: resp.cluster_id.to_string(),
        name: resp.name.clone(),
        visibility: resp
            .visibility
            .clone()
            .or_else(|| Some("public".to_string())),
        accepting_jobs: Some(true),
        connected: Some(true),
        room_secret: Some(stored.clone()),
        cluster_password: Some(stored),
        require_e2ee: resp.require_e2ee,
        require_tee: None,
        required_attestation_flags: resp.required_attestation_flags,
        schedule_enabled: None,
        schedule_start: None,
        schedule_end: None,
    }
}

pub fn upsert_swarm(config: &mut ClientConfig, swarm: SwarmMembership) {
    if let Some(existing) = config
        .swarms
        .iter_mut()
        .find(|s| s.swarm_id == swarm.swarm_id)
    {
        if swarm.name.is_some() {
            existing.name = swarm.name;
        }
        if !swarm.p2p_token.is_empty() {
            existing.p2p_token = swarm.p2p_token;
        }
        if !swarm.bootnodes.is_empty() {
            existing.bootnodes = swarm.bootnodes;
        }
        return;
    }
    config.swarms.push(swarm);
}

pub fn remove_swarm(config: &mut ClientConfig, swarm_id: &str) {
    config.swarms.retain(|s| s.swarm_id != swarm_id);
}

pub fn find_swarm<'a>(config: &'a ClientConfig, swarm_id: &str) -> Option<&'a SwarmMembership> {
    config.swarms.iter().find(|s| s.swarm_id == swarm_id)
}

pub fn find_swarm_by_token(config: &ClientConfig, token: &str) -> Option<SwarmMembership> {
    config.swarms.iter().find(|s| s.p2p_token == token).cloned()
}

/// When the user creates or joins a swarm, enable libp2p transport if it was cluster-only.
pub fn ensure_p2p_mode_for_swarms(config: &mut ClientConfig) {
    if config.swarms.is_empty() {
        return;
    }
    if config.p2p_mode == crate::shared::NetworkMode::Cluster {
        config.p2p_mode = if config.clusters.is_empty() {
            crate::shared::NetworkMode::Swarm
        } else {
            crate::shared::NetworkMode::Both
        };
    }
}

pub fn find_swarm_mut<'a>(
    config: &'a mut ClientConfig,
    swarm_id: &str,
) -> Option<&'a mut SwarmMembership> {
    config.swarms.iter_mut().find(|s| s.swarm_id == swarm_id)
}

pub fn create_swarm_membership(name: Option<String>, p2p_token: Option<String>) -> SwarmMembership {
    SwarmMembership {
        swarm_id: Uuid::new_v4().to_string(),
        name,
        p2p_token: p2p_token.unwrap_or_else(|| Uuid::new_v4().to_string()),
        bootnodes: default_bootnodes_from_env(),
        accepting_jobs: Some(true),
        connected: Some(true),
        require_e2ee: None,
        require_tee: None,
        required_attestation_flags: None,
        schedule_enabled: None,
        schedule_start: None,
        schedule_end: None,
    }
}

#[cfg(test)]
mod service_credentials_tests {
    use super::service_credentials_for_registration;

    #[test]
    fn service_credentials_for_registration_requires_both_env_vars() {
        let prev_name = std::env::var("MTRXAI_SERVICE_NAME").ok();
        let prev_password = std::env::var("MTRXAI_SERVICE_PASSWORD").ok();
        std::env::remove_var("MTRXAI_SERVICE_NAME");
        std::env::remove_var("MTRXAI_SERVICE_PASSWORD");
        let (name, password) = service_credentials_for_registration();
        assert!(name.is_none());
        assert!(password.is_none());

        std::env::set_var("MTRXAI_SERVICE_NAME", "svc-only");
        let (name, password) = service_credentials_for_registration();
        assert!(name.is_none());
        assert!(password.is_none());

        std::env::set_var("MTRXAI_SERVICE_PASSWORD", "secret");
        let (name, password) = service_credentials_for_registration();
        assert_eq!(name.as_deref(), Some("svc-only"));
        assert_eq!(password.as_deref(), Some("secret"));

        match prev_name {
            Some(v) => std::env::set_var("MTRXAI_SERVICE_NAME", v),
            None => std::env::remove_var("MTRXAI_SERVICE_NAME"),
        }
        match prev_password {
            Some(v) => std::env::set_var("MTRXAI_SERVICE_PASSWORD", v),
            None => std::env::remove_var("MTRXAI_SERVICE_PASSWORD"),
        }
    }
}
