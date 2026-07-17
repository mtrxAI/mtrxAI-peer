use crate::p2p_protocol::CachedPeerRecord;
use crate::shared::{GpuHostStatus, PeerInfo};
use serde_json::Value;
use std::collections::HashSet;

pub fn asn_key(asn: &str) -> String {
    asn.split_whitespace()
        .next()
        .unwrap_or(asn)
        .to_ascii_uppercase()
}

pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_KM * c
}

pub fn peer_load_score(model_entry: &Value, gpu_host: &Option<GpuHostStatus>) -> f64 {
    let mut score = 0.0;
    if let Some(status) = model_entry.get("_status") {
        let loaded = status
            .get("loaded")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if loaded {
            let cpu = status
                .get("cpu_pct")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as f64;
            let gpu = status
                .get("gpu_pct")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as f64;
            score += cpu.max(gpu);
        } else {
            score += 25.0;
        }
    }
    if let Some(gpu) = gpu_host {
        if gpu.available {
            score += f64::from(gpu.utilization_pct);
        }
    }
    score
}

pub fn rank_peers_for_model(
    requester: Option<&PeerInfo>,
    model: &str,
    peers: &[CachedPeerRecord],
    requester_peer_id: &str,
    blocked: &HashSet<String>,
) -> Vec<String> {
    let requester_asn = requester.map(|p| asn_key(&p.asn));

    let mut candidates: Vec<(String, bool, f64, f64)> = Vec::new();
    for peer in peers {
        if peer.peer_id == requester_peer_id {
            continue;
        }
        if blocked.contains(&peer.peer_id) {
            continue;
        }
        let Some(model_entry) = peer
            .models
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(model))
        else {
            continue;
        };

        let same_asn = match (&requester_asn, peer.peer_info.as_ref()) {
            (Some(req), Some(info)) => asn_key(&info.asn) == *req,
            _ => false,
        };

        let distance_km = match (requester, peer.peer_info.as_ref()) {
            (Some(req), Some(info)) => haversine_km(req.lat, req.lon, info.lat, info.lon),
            _ => f64::MAX,
        };

        let load_score = peer_load_score(model_entry, &peer.gpu_host);
        candidates.push((peer.peer_id.clone(), same_asn, distance_km, load_score));
    }

    candidates.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .reverse()
            .then(a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
            .then(a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal))
    });

    candidates.into_iter().map(|(id, _, _, _)| id).collect()
}

/// Swarm peer ranking: model availability + measured RTT + load (no ASN/geo).
pub fn rank_swarm_peers_for_model(
    model: &str,
    peers: &[CachedPeerRecord],
    requester_peer_id: &str,
    blocked: &HashSet<String>,
    latency_ms: &std::collections::HashMap<String, u64>,
) -> Vec<String> {
    rank_swarm_peers_for_model_with_tee(model, peers, requester_peer_id, blocked, latency_ms, false)
}

pub fn rank_swarm_peers_for_model_with_tee(
    model: &str,
    peers: &[CachedPeerRecord],
    requester_peer_id: &str,
    blocked: &HashSet<String>,
    latency_ms: &std::collections::HashMap<String, u64>,
    require_tee: bool,
) -> Vec<String> {
    let mut candidates: Vec<(String, bool, bool, u64, f64)> = Vec::new();
    for peer in peers {
        if peer.peer_id == requester_peer_id {
            continue;
        }
        if blocked.contains(&peer.peer_id) {
            continue;
        }
        if !peer.accepting_jobs {
            continue;
        }
        if require_tee && !peer.tee_capable {
            continue;
        }
        let Some(model_entry) = peer
            .models
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(model))
        else {
            continue;
        };
        let loaded = model_entry
            .get("_status")
            .and_then(|s| s.get("loaded"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let rtt = latency_ms.get(&peer.peer_id).copied().unwrap_or(u64::MAX / 2);
        let load_score = peer_load_score(model_entry, &peer.gpu_host);
        let tee_rank = peer.tee_capable
            || peer.trust_level.as_deref() == Some("tee_gpu");
        candidates.push((peer.peer_id.clone(), tee_rank, loaded, rtt, load_score));
    }

    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then(b.2.cmp(&a.2))
            .then(a.3.cmp(&b.3))
            .then(a.4.partial_cmp(&b.4).unwrap_or(std::cmp::Ordering::Equal))
    });

    candidates.into_iter().map(|(id, _, _, _, _)| id).collect()
}

pub fn peer_can_host(gpu_host: &Option<GpuHostStatus>, vram_needed_mb: u64, accepting_jobs: bool) -> bool {
    if !accepting_jobs {
        return false;
    }
    let Some(gpu) = gpu_host else {
        return false;
    };
    gpu.available && gpu.memory_free_mb >= vram_needed_mb
}

pub fn select_provider_peer(
    peers: &[CachedPeerRecord],
    vram_needed_mb: u64,
    requester_peer_id: &str,
) -> Option<String> {
    if let Some(requester) = peers.iter().find(|p| p.peer_id == requester_peer_id) {
        if peer_can_host(&requester.gpu_host, vram_needed_mb, requester.accepting_jobs) {
            return Some(requester_peer_id.to_string());
        }
    }

    let mut candidates: Vec<(String, f64)> = Vec::new();
    for peer in peers {
        if peer.peer_id == requester_peer_id {
            continue;
        }
        if !peer_can_host(&peer.gpu_host, vram_needed_mb, peer.accepting_jobs) {
            continue;
        }
        let model_entry = peer.models.first().cloned().unwrap_or(serde_json::json!({}));
        let load_score = peer_load_score(&model_entry, &peer.gpu_host);
        candidates.push((peer.peer_id.clone(), load_score));
    }

    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.first().map(|(id, _)| id.clone())
}

pub fn peers_with_loaded_model(peers: &[CachedPeerRecord], model: &str) -> Vec<String> {
    let mut out = Vec::new();
    for peer in peers {
        for m in &peer.models {
            if m.get("name").and_then(|n| n.as_str()) != Some(model) {
                continue;
            }
            let loaded = m
                .get("_status")
                .and_then(|s| s.get("loaded"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if loaded {
                out.push(peer.peer_id.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn peer(id: &str, asn: &str, lat: f64, lon: f64, model: &str) -> CachedPeerRecord {
        CachedPeerRecord {
            peer_id: id.to_string(),
            models: vec![json!({
                "name": model,
                "_status": { "loaded": true, "cpu_pct": 10, "gpu_pct": 0 }
            })],
            peer_info: Some(PeerInfo {
                lat,
                lon,
                asn: asn.to_string(),
                city: None,
                country: None,
            }),
            gpu_host: None,
            accepting_jobs: true,
            listen_addrs: Vec::new(),
            tee_capable: false,
            trust_level: None,
            gpu_model: None,
            provider_static_pk: None,
        }
    }

    #[test]
    fn rank_swarm_peers_prefers_low_latency() {
        use std::collections::HashMap;
        let peers = vec![
            CachedPeerRecord {
                peer_id: "near".into(),
                models: vec![json!({"name": "llama3", "_status": {"loaded": true}})],
                peer_info: None,
                gpu_host: None,
                accepting_jobs: true,
                listen_addrs: Vec::new(),
                tee_capable: false,
                trust_level: None,
                gpu_model: None,
                provider_static_pk: None,
            },
            CachedPeerRecord {
                peer_id: "far".into(),
                models: vec![json!({"name": "llama3", "_status": {"loaded": true}})],
                peer_info: None,
                gpu_host: None,
                accepting_jobs: true,
                listen_addrs: Vec::new(),
                tee_capable: false,
                trust_level: None,
                gpu_model: None,
                provider_static_pk: None,
            },
        ];
        let latency = HashMap::from([("near".into(), 5), ("far".into(), 200)]);
        let ranked = rank_swarm_peers_for_model("llama3", &peers, "me", &HashSet::new(), &latency);
        assert_eq!(ranked, vec!["near".to_string(), "far".to_string()]);
    }

    #[test]
    fn rank_prefers_same_asn() {
        let requester = PeerInfo {
            lat: 45.0,
            lon: 9.0,
            asn: "AS100 ISP".to_string(),
            city: None,
            country: None,
        };
        let peers = vec![
            peer("far", "AS200 Other", 45.01, 9.01, "llama3"),
            peer("near-asn", "AS100 ISP", 46.0, 10.0, "llama3"),
        ];
        let ranked = rank_peers_for_model(Some(&requester), "llama3", &peers, "me", &HashSet::new());
        assert_eq!(ranked[0], "near-asn");
    }
}
