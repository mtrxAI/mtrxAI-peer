use crate::shared::{GpuHostStatus, PeerInfo};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROXY_PROTOCOL: &str = "/mtrxai/proxy/1.0.0";
pub const PROXY_PROTOCOL_V2: &str = "/mtrxai/proxy/2.0.0";
pub const MODEL_START_PROTOCOL: &str = "/mtrxai/model-start/1.0.0";
/// Stay under gossipsub default (64 KiB) with headroom for framing overhead.
pub const GOSSIP_CATALOG_MAX_BYTES: usize = 48 * 1024;

pub fn token_namespace(p2p_token: &str) -> String {
    hex::encode(Sha256::digest(p2p_token.as_bytes()))
}

pub fn catalog_topic(p2p_token: &str) -> String {
    format!("mtrxai/catalog/{}", token_namespace(p2p_token))
}

pub fn model_start_topic(p2p_token: &str) -> String {
    format!("mtrxai/modelstart/{}", token_namespace(p2p_token))
}

pub fn dht_peer_record_key(p2p_token: &str, peer_id: &str) -> String {
    format!(
        "mtrxai/swarm/{}/peer/{}",
        token_namespace(p2p_token),
        peer_id
    )
}

/// Messages sent over libp2p streams (same framing as WebRTC DataChannelMessage).
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StreamMessage {
    ProxyRequest {
        req_id: String,
        path: String,
        body: serde_json::Value,
    },
    /// Single-shot swarm proxy response (full body).
    ProxyResponse {
        req_id: String,
        body: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    ProxyRequestStart {
        req_id: String,
        path: String,
    },
    ProxyRequestChunk {
        req_id: String,
        seq: u32,
        chunk: String,
    },
    ProxyRequestEnd {
        req_id: String,
    },
    ProxyResponseChunk {
        req_id: String,
        chunk: String,
    },
    ProxyResponseDone {
        req_id: String,
    },
    ProxyResponseError {
        req_id: String,
        error: String,
    },
    Ping,
    Pong,
    /// Direct catalog exchange over request-response (reliable for small swarms).
    CatalogSync {
        peer_id: String,
        models: Vec<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peer_info: Option<PeerInfo>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_host: Option<GpuHostStatus>,
        accepting_jobs: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        listen_addrs: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tee_capable: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attestation_expiry: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trust_level: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_static_pk: Option<String>,
    },
    /// Application-layer E2EE proxy request (v2 protocol).
    EncryptedProxyRequest {
        req_id: String,
        path: String,
        room_id: String,
        consumer_ephemeral_pk: Vec<u8>,
        nonce: Vec<u8>,
        ciphertext: Vec<u8>,
        aad_version: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<crate::security::ProxyAuthProof>,
    },
    /// Start of a chunked E2EE proxy request (WebRTC wire-size limit).
    EncryptedProxyRequestStart {
        req_id: String,
        path: String,
        room_id: String,
        consumer_ephemeral_pk: Vec<u8>,
        aad_version: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<crate::security::ProxyAuthProof>,
    },
    /// Chunk of a chunked E2EE proxy request body.
    EncryptedProxyRequestChunk {
        req_id: String,
        seq: u32,
        nonce: Vec<u8>,
        ciphertext: Vec<u8>,
        aad_version: u8,
    },
    /// End of a chunked E2EE proxy request.
    EncryptedProxyRequestEnd {
        req_id: String,
    },
    EncryptedProxyResponse {
        req_id: String,
        nonce: Vec<u8>,
        ciphertext: Vec<u8>,
        aad_version: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// When true with empty ciphertext, body arrives via [`EncryptedProxyStreamChunk`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<bool>,
    },
    /// Encrypted streaming chunk (provider → consumer reverse request).
    EncryptedProxyStreamChunk {
        req_id: String,
        seq: u32,
        nonce: Vec<u8>,
        ciphertext: Vec<u8>,
        aad_version: u8,
        done: bool,
    },
    /// Proxy membership proof (must precede encrypted frames on v2).
    ProxyAuth {
        proof: crate::security::ProxyAuthProof,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipMessage {
    CatalogUpdate {
        peer_id: String,
        models: Vec<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peer_info: Option<PeerInfo>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_host: Option<GpuHostStatus>,
        accepting_jobs: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        listen_addrs: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tee_capable: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gpu_model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attestation_expiry: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trust_level: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_static_pk: Option<String>,
    },
    ModelStartRequest {
        req_id: String,
        model: String,
        requested_by: String,
    },
    ModelStartOffer {
        req_id: String,
        model: String,
        provider_peer: String,
        requested_by: String,
        estimated_vram_mb: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk_size_mb: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_on_requester: Option<bool>,
    },
    ModelStartProgress {
        req_id: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress_pct: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_peer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ModelStartRespond {
        req_id: String,
        accept: bool,
        provider_peer: String,
    },
}

#[derive(Clone, Debug)]
pub struct CachedPeerRecord {
    pub peer_id: String,
    pub models: Vec<serde_json::Value>,
    pub peer_info: Option<PeerInfo>,
    pub gpu_host: Option<GpuHostStatus>,
    pub accepting_jobs: bool,
    pub listen_addrs: Vec<String>,
    pub tee_capable: bool,
    pub trust_level: Option<String>,
    pub gpu_model: Option<String>,
    /// Hex-encoded X25519 static public key for E2EE session agreement (from catalog).
    pub provider_static_pk: Option<String>,
}

pub fn encode_gossip(msg: &GossipMessage) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(msg)?)
}

/// Strip heavy Ollama fields — full `local_models_full` entries can exceed gossip limits.
pub fn compact_models_for_gossip(models: &[serde_json::Value]) -> Vec<serde_json::Value> {
    models
        .iter()
        .filter_map(|m| {
            let name = m.get("name")?.as_str()?;
            let mut compact = serde_json::json!({ "name": name });
            if let Some(status) = m.get("_status") {
                compact["_status"] = serde_json::json!({
                    "loaded": status.get("loaded").unwrap_or(&serde_json::Value::Bool(false)),
                    "cpu_pct": status.get("cpu_pct"),
                    "gpu_pct": status.get("gpu_pct"),
                });
            }
            if let Some(details) = m.get("details").and_then(|d| d.as_object()) {
                let mut slim = serde_json::Map::new();
                for key in [
                    "parameter_size",
                    "quantization_level",
                    "family",
                    "families",
                    "format",
                    "parent_model",
                ] {
                    if let Some(v) = details.get(key) {
                        slim.insert(key.to_string(), v.clone());
                    }
                }
                if !slim.is_empty() {
                    compact["details"] = serde_json::Value::Object(slim);
                }
            }
            for key in ["size", "digest", "modified_at"] {
                if let Some(v) = m.get(key) {
                    compact[key] = v.clone();
                }
            }
            Some(compact)
        })
        .collect()
}

pub fn compact_gpu_for_gossip(gpu: &Option<GpuHostStatus>) -> Option<GpuHostStatus> {
    gpu.as_ref().map(|g| GpuHostStatus {
        available: g.available,
        utilization_pct: g.utilization_pct,
        memory_used_mb: g.memory_used_mb,
        memory_total_mb: g.memory_total_mb,
        memory_free_mb: g.memory_free_mb,
        name: g.name.clone(),
        producer: None,
        architecture: None,
        driver_version: None,
        cuda_version: None,
        device_count: None,
        temperature_c: None,
        memory_utilization_pct: None,
        power_draw_w: None,
        source: None,
        sampled_at_unix: None,
        devices: None,
    })
}

pub fn compact_peer_info_for_gossip(info: &Option<PeerInfo>) -> Option<PeerInfo> {
    info.as_ref().map(|p| PeerInfo {
        lat: p.lat,
        lon: p.lon,
        asn: p.asn.clone(),
        city: None,
        country: None,
    })
}

pub fn fit_catalog_gossip(msg: &mut GossipMessage) {
    loop {
        if encode_gossip(msg).map(|b| b.len()).unwrap_or(usize::MAX) <= GOSSIP_CATALOG_MAX_BYTES {
            return;
        }
        let GossipMessage::CatalogUpdate {
            models,
            peer_info,
            gpu_host,
            ..
        } = msg
        else {
            return;
        };
        if gpu_host.is_some() {
            *gpu_host = None;
            continue;
        }
        if peer_info.is_some() {
            *peer_info = None;
            continue;
        }
        if models.len() > 1 {
            models.truncate(models.len() / 2);
            continue;
        }
        models.clear();
        return;
    }
}

pub fn decode_gossip(bytes: &[u8]) -> anyhow::Result<GossipMessage> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn encode_stream_line(msg: &StreamMessage) -> anyhow::Result<String> {
    Ok(format!("{}\n", serde_json::to_string(msg)?))
}

pub async fn write_stream_message(
    stream: &mut libp2p::Stream,
    msg: &StreamMessage,
) -> anyhow::Result<()> {
    use futures_util::AsyncWriteExt;
    let line = encode_stream_line(msg)?;
    stream.write_all(line.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_stream_message(stream: &mut libp2p::Stream) -> anyhow::Result<StreamMessage> {
    use futures_util::AsyncReadExt;
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            anyhow::bail!("stream closed before message");
        }
        if byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
        if buf.len() > 4 * 1024 * 1024 {
            anyhow::bail!("stream message too large");
        }
    }
    Ok(serde_json::from_slice(&buf)?)
}
