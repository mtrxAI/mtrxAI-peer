use crate::shared::{ClusterStatus, SharedState, SwarmStatus};
use serde_json::{json, Value};
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

fn peers_from_model(model: &Value) -> Vec<Value> {
    if let Some(peers) = model.get("_peers").and_then(|p| p.as_array()) {
        return peers.clone();
    }
    if let Some(peer) = model.get("_peer") {
        if !peer.is_null() {
            return vec![peer.clone()];
        }
    }
    Vec::new()
}

fn peer_id_from(peer: &Value) -> Option<String> {
    peer.get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn model_metadata_score(model: &Value) -> usize {
    let mut score = 0usize;
    if model
        .get("details")
        .and_then(|d| d.get("parameter_size"))
        .and_then(|s| s.as_str())
        .is_some_and(|s| !s.is_empty())
    {
        score += 10;
    }
    if model
        .get("details")
        .and_then(|d| d.get("quantization_level"))
        .and_then(|s| s.as_str())
        .is_some_and(|s| !s.is_empty())
    {
        score += 4;
    }
    if model.get("size").and_then(|v| v.as_u64()).unwrap_or(0) > 0 {
        score += 5;
    }
    if model
        .get("digest")
        .and_then(|s| s.as_str())
        .is_some_and(|s| !s.is_empty())
    {
        score += 3;
    }
    score
}

pub fn prefer_richer_template(current: &Value, candidate: &Value) -> Value {
    if model_metadata_score(candidate) > model_metadata_score(current) {
        candidate.clone()
    } else {
        current.clone()
    }
}

fn merge_peer_entries(existing: &mut Value, incoming: &Value) {
    let loaded_existing = existing
        .get("loaded")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let loaded_incoming = incoming
        .get("loaded")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if loaded_incoming && !loaded_existing {
        *existing = incoming.clone();
    } else if loaded_incoming {
        if let Some(obj) = existing.as_object_mut() {
            obj.insert("loaded".to_string(), json!(true));
        }
    }
}

struct AggModel {
    template: Value,
    peers_by_id: StdHashMap<String, Value>,
    clusters: Vec<Value>,
    swarms: Vec<Value>,
}

pub async fn sync_unified_network_models(
    shared_state: &SharedState,
    cluster_network_models: &Arc<Mutex<StdHashMap<String, Vec<Value>>>>,
    swarm_network_models: &Arc<Mutex<StdHashMap<String, Vec<Value>>>>,
) {
    let cluster_lock = cluster_network_models.lock().await;
    let swarm_lock = swarm_network_models.lock().await;
    let mut by_name: StdHashMap<String, AggModel> = StdHashMap::new();

    for (cluster_id, models) in cluster_lock.iter() {
        for model in models {
            let Some(name) = model.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let cluster_peer_count = model
                .get("_peer_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(1);
            let cluster_loaded_count = model
                .get("_loaded_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let cluster_entry = json!({
                "cluster_id": cluster_id,
                "peer_count": cluster_peer_count,
                "loaded_count": cluster_loaded_count,
            });

            let agg = by_name.entry(name.to_string()).or_insert_with(|| AggModel {
                template: model.clone(),
                peers_by_id: StdHashMap::new(),
                clusters: Vec::new(),
                swarms: Vec::new(),
            });
            agg.template = prefer_richer_template(&agg.template, model);
            agg.clusters.push(cluster_entry);

            for peer in peers_from_model(model) {
                let Some(peer_id) = peer_id_from(&peer) else {
                    continue;
                };
                agg.peers_by_id
                    .entry(peer_id)
                    .and_modify(|existing| merge_peer_entries(existing, &peer))
                    .or_insert(peer);
            }
        }
    }

    for (swarm_id, models) in swarm_lock.iter() {
        for model in models {
            let Some(name) = model.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let swarm_peer_count = model
                .get("_peer_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(1);
            let swarm_loaded_count = model
                .get("_loaded_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let swarm_entry = json!({
                "swarm_id": swarm_id,
                "peer_count": swarm_peer_count,
                "loaded_count": swarm_loaded_count,
            });

            let agg = by_name.entry(name.to_string()).or_insert_with(|| AggModel {
                template: model.clone(),
                peers_by_id: StdHashMap::new(),
                clusters: Vec::new(),
                swarms: Vec::new(),
            });
            agg.template = prefer_richer_template(&agg.template, model);
            agg.swarms.push(swarm_entry);

            for peer in peers_from_model(model) {
                let Some(peer_id) = peer_id_from(&peer) else {
                    continue;
                };
                agg.peers_by_id
                    .entry(peer_id)
                    .and_modify(|existing| merge_peer_entries(existing, &peer))
                    .or_insert(peer);
            }
        }
    }

    let mut merged: Vec<Value> = by_name
        .into_iter()
        .map(|(name, agg)| {
            let peer_count = agg.peers_by_id.len() as u64;
            let loaded_count = agg
                .peers_by_id
                .values()
                .filter(|peer| peer.get("loaded").and_then(|v| v.as_bool()) == Some(true))
                .count() as u64;
            let representative = agg
                .peers_by_id
                .values()
                .find(|peer| peer.get("loaded").and_then(|v| v.as_bool()) == Some(true))
                .or_else(|| agg.peers_by_id.values().next())
                .cloned()
                .unwrap_or(json!(null));
            let peers: Vec<_> = agg.peers_by_id.values().cloned().collect();

            let primary_cluster = agg
                .clusters
                .iter()
                .max_by_key(|c| c.get("peer_count").and_then(|v| v.as_u64()).unwrap_or(0))
                .and_then(|c| c.get("cluster_id").cloned());
            let primary_swarm = agg
                .swarms
                .iter()
                .max_by_key(|s| s.get("peer_count").and_then(|v| v.as_u64()).unwrap_or(0))
                .and_then(|s| s.get("swarm_id").cloned());

            let mut obj = agg.template.as_object().cloned().unwrap_or_default();
            obj.insert("name".to_string(), json!(name));
            obj.insert("_peer".to_string(), representative);
            obj.insert("_peers".to_string(), json!(peers));
            obj.insert("_peer_count".to_string(), json!(peer_count));
            obj.insert("_loaded_count".to_string(), json!(loaded_count));
            if !agg.clusters.is_empty() {
                obj.insert("_clusters".to_string(), json!(agg.clusters));
            }
            if !agg.swarms.is_empty() {
                obj.insert("_swarms".to_string(), json!(agg.swarms));
            }
            if let Some(cid) = primary_cluster {
                obj.insert("_cluster_id".to_string(), cid);
            }
            if let Some(sid) = primary_swarm {
                obj.insert("_swarm_id".to_string(), sid);
            }
            if let Some(scope) = network_scope_label(&Value::Object(obj.clone())) {
                obj.insert("_network_scope".to_string(), json!(scope));
            }
            Value::Object(obj)
        })
        .collect();

    merged.sort_by(|a, b| {
        a.get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .cmp(b.get("name").and_then(|n| n.as_str()).unwrap_or(""))
    });

    let models_by_cluster = cluster_lock.clone();
    let models_by_swarm = swarm_lock.clone();
    drop(cluster_lock);
    drop(swarm_lock);

    let mut state = shared_state.lock().await;
    state.network_models = merged;
    for cluster in &mut state.clusters {
        cluster.network_models = models_by_cluster
            .get(&cluster.cluster_id)
            .cloned()
            .unwrap_or_default();
    }
    for swarm in &mut state.swarms {
        swarm.network_models = models_by_swarm
            .get(&swarm.swarm_id)
            .cloned()
            .unwrap_or_default();
    }
}

/// Legacy cluster-only sync — prefer `sync_unified_network_models` with both maps.
pub async fn sync_aggregated_network_models(
    shared_state: &SharedState,
    cluster_network_models: &Arc<Mutex<StdHashMap<String, Vec<Value>>>>,
    swarm_network_models: &Arc<Mutex<StdHashMap<String, Vec<Value>>>>,
) {
    sync_unified_network_models(shared_state, cluster_network_models, swarm_network_models).await;
}

fn infer_parameter_size_from_name(name: &str) -> String {
    let Some(tag) = name.rsplit(':').next() else {
        return String::new();
    };
    let lower = tag.to_ascii_lowercase();
    if lower.len() > 1
        && lower.ends_with('b')
        && lower
            .trim_end_matches('b')
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.')
    {
        return tag.to_ascii_uppercase();
    }
    String::new()
}

pub fn attach_scope_models(
    clusters: &mut [ClusterStatus],
    swarms: &mut [SwarmStatus],
    cluster_models: &StdHashMap<String, Vec<Value>>,
    swarm_models: &StdHashMap<String, Vec<Value>>,
) {
    for cluster in clusters {
        cluster.network_models = cluster_models
            .get(&cluster.cluster_id)
            .cloned()
            .unwrap_or_default();
    }
    for swarm in swarms {
        swarm.network_models = swarm_models
            .get(&swarm.swarm_id)
            .cloned()
            .unwrap_or_default();
    }
}

pub fn network_scope_label(model: &Value) -> Option<&'static str> {
    let has_cluster = model
        .get("_clusters")
        .and_then(|c| c.as_array())
        .is_some_and(|a| !a.is_empty())
        || model.get("_cluster_id").is_some();
    let has_swarm = model
        .get("_swarms")
        .and_then(|s| s.as_array())
        .is_some_and(|a| !a.is_empty())
        || model.get("_swarm_id").is_some();
    match (has_cluster, has_swarm) {
        (true, true) => Some("both"),
        (true, false) => Some("cluster"),
        (false, true) => Some("swarm"),
        _ => None,
    }
}

pub fn enrich_openai_remote_model(remote_model: &mut Value, network_model: &Value) {
    if let Some(name) = network_model.get("name").and_then(|n| n.as_str()) {
        remote_model["name"] = json!(name);
    }
    for key in [
        "_status",
        "_peer",
        "_peers",
        "_peer_count",
        "_loaded_count",
        "_clusters",
        "_swarms",
        "_cluster_id",
        "_swarm_id",
    ] {
        if let Some(v) = network_model.get(key) {
            remote_model[key] = v.clone();
        }
    }
    if let Some(scope) = network_scope_label(network_model) {
        remote_model["_network_scope"] = json!(scope);
    }
}

/// Build an Ollama-compatible `/api/tags` entry for a cluster/swarm network model.
pub fn ollama_tag_from_network_model(network_model: &Value) -> Value {
    let name = network_model
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("unknown");
    let family = name.split(':').next().unwrap_or(name);
    let parameter_size = network_model
        .get("details")
        .and_then(|d| d.get("parameter_size"))
        .and_then(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| infer_parameter_size_from_name(name));
    let quantization_level = network_model
        .get("details")
        .and_then(|d| d.get("quantization_level"))
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let modified_at = network_model
        .get("modified_at")
        .cloned()
        .unwrap_or_else(|| {
            json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        });

    let mut tag = json!({
        "name": name,
        "model": name,
        "modified_at": modified_at,
        "size": network_model.get("size").cloned().unwrap_or(json!(1)),
        "digest": network_model
            .get("digest")
            .and_then(|d| d.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
        "details": {
            "parent_model": "",
            "format": "remote",
            "family": family,
            "families": [family],
            "parameter_size": parameter_size,
            "quantization_level": quantization_level
        }
    });

    if let Some(details) = network_model.get("details").and_then(|d| d.as_object()) {
        if let Some(obj) = tag.get_mut("details").and_then(|d| d.as_object_mut()) {
            for (k, v) in details {
                obj.insert(k.clone(), v.clone());
            }
            obj.insert("format".to_string(), json!("remote"));
        }
    }

    enrich_openai_remote_model(&mut tag, network_model);
    if !network_model
        .get("_show_info")
        .map(|v| !v.is_null())
        .unwrap_or(false)
    {
        tag["_show_info"] = json!({
            "name": name,
            "model": name,
            "modified_at": tag.get("modified_at").cloned().unwrap_or(json!(null)),
            "size": tag.get("size").cloned().unwrap_or(json!(1)),
            "digest": tag.get("digest").cloned().unwrap_or(json!("")),
            "details": tag.get("details").cloned().unwrap_or(json!({})),
            "capabilities": ["completion"],
            "remote": true,
        });
    }
    tag
}

/// Ollama `/api/show`-compatible payload for remote cluster/swarm models (VS Code / Cursor).
pub fn network_model_show_info(network_model: &Value) -> Value {
    if let Some(existing) = network_model.get("_show_info") {
        if !existing.is_null() {
            return existing.clone();
        }
    }

    let tag = ollama_tag_from_network_model(network_model);
    let name = tag
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("unknown");
    let mut show = json!({
        "name": name,
        "model": name,
        "modified_at": tag.get("modified_at").cloned().unwrap_or(json!(null)),
        "size": tag.get("size").cloned().unwrap_or(json!(1)),
        "digest": tag.get("digest").cloned().unwrap_or(json!("")),
        "details": tag.get("details").cloned().unwrap_or(json!({
            "format": "remote",
            "family": name.split(':').next().unwrap_or(name),
            "families": [name.split(':').next().unwrap_or(name)],
            "parameter_size": "",
            "quantization_level": "",
            "parent_model": ""
        })),
        "capabilities": ["completion"],
        "remote": true,
    });
    if let Some(scope) = network_scope_label(network_model) {
        show["remote_scope"] = json!(scope);
    }
    show
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ollama_tag_for_swarm_model_includes_show_info_and_scope() {
        let model = json!({
            "name": "llama3:8b",
            "_swarm_id": "swarm-1",
            "_swarms": [{ "swarm_id": "swarm-1", "peer_count": 2, "loaded_count": 1 }],
        });
        let tag = ollama_tag_from_network_model(&model);
        assert_eq!(tag["name"], "llama3:8b");
        assert_eq!(tag["model"], "llama3:8b");
        assert_eq!(tag["_network_scope"], "swarm");
        assert!(tag.get("_show_info").is_some());
        assert_eq!(tag["_show_info"]["capabilities"], json!(["completion"]));
    }

    #[test]
    fn network_model_show_info_synthesizes_for_minimal_swarm_entry() {
        let model = json!({ "name": "qwen2.5:7b", "_swarm_id": "s1" });
        let show = network_model_show_info(&model);
        assert_eq!(show["name"], "qwen2.5:7b");
        assert_eq!(show["remote_scope"], "swarm");
        assert_eq!(show["capabilities"], json!(["completion"]));
    }
}
