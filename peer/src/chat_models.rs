//! Build the chat-tab model picker list with local vs remote classification.

use serde::Serialize;
use serde_json::Value;

use crate::client_config::is_custom_server_kind;
use crate::ollama_client::is_embed_only;

/// Hosted API kinds that should show as remote (cloud) even when attached locally.
pub fn is_hosted_remote_kind(kind: &str) -> bool {
    let k = kind.to_lowercase();
    k == "openai" || is_custom_server_kind(&k)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChatModelEntry {
    pub name: String,
    /// `"local"` (PC icon) or `"remote"` (cloud icon).
    pub location: String,
    pub source_kind: String,
    pub source_label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded: Option<bool>,
}

fn model_name(m: &Value) -> Option<&str> {
    m.get("name").and_then(|n| n.as_str()).filter(|s| !s.is_empty())
}

fn show_info_ref(m: &Value) -> Option<&Value> {
    m.get("_show_info").or_else(|| m.get("capabilities").map(|_| m))
}

fn is_chat_capable(m: &Value) -> bool {
    // Prefer enriched show info; fall back to treating unknown as chat-capable.
    let show = show_info_ref(m);
    if let Some(s) = show {
        // If capabilities are nested under _show_info, use is_embed_only on that.
        if s.get("capabilities").is_some() {
            return !is_embed_only(Some(s));
        }
    }
    // Some catalogs put capabilities on the model root.
    if m.get("capabilities").is_some() {
        return !is_embed_only(Some(m));
    }
    true
}

fn loaded_flag(m: &Value) -> Option<bool> {
    m.get("_status")
        .and_then(|s| s.get("loaded"))
        .and_then(|v| v.as_bool())
}

fn server_label_for(local: &Value, servers: &[(String, String, Option<String>)]) -> String {
    let server_id = local
        .get("_source_server")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = local
        .get("_source_kind")
        .and_then(|v| v.as_str())
        .unwrap_or("local");
    if let Some((_, _, label)) = servers.iter().find(|(id, _, _)| id == server_id) {
        if let Some(l) = label.as_ref().filter(|s| !s.is_empty()) {
            return l.clone();
        }
    }
    if let Some((_, k, _)) = servers.iter().find(|(id, _, _)| id == server_id) {
        if !k.is_empty() {
            return k.clone();
        }
    }
    kind.to_string()
}

fn network_source(m: &Value) -> (String, String) {
    if let Some(clusters) = m.get("_clusters").and_then(|c| c.as_array()) {
        if let Some(first) = clusters.first() {
            let id = first
                .get("cluster_id")
                .and_then(|v| v.as_str())
                .unwrap_or("cluster");
            let label = first
                .get("name")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(id);
            return ("cluster".to_string(), label.to_string());
        }
    }
    if let Some(cid) = m.get("_cluster_id").and_then(|v| v.as_str()) {
        return ("cluster".to_string(), cid.to_string());
    }
    if let Some(swarms) = m.get("_swarms").and_then(|s| s.as_array()) {
        if let Some(first) = swarms.first() {
            let id = first
                .get("swarm_id")
                .and_then(|v| v.as_str())
                .unwrap_or("swarm");
            let label = first
                .get("name")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(id);
            return ("swarm".to_string(), label.to_string());
        }
    }
    if let Some(sid) = m.get("_swarm_id").and_then(|v| v.as_str()) {
        return ("swarm".to_string(), sid.to_string());
    }
    ("network".to_string(), "Network".to_string())
}

/// Build a sorted, deduplicated list of chat-capable models for the debug chat picker.
///
/// - Local attached engines (ollama, vllm, …) → `location: "local"`
/// - Hosted attached kinds (openai, custom) → `location: "remote"`
/// - Network-only (cluster/swarm) → `location: "remote"`
/// - Name present both locally and on network → prefer local classification
/// - Embed-only models are omitted when detectable
///
/// `servers` is `(server_id, kind, label)` for source_label resolution.
pub fn build_chat_models(
    local_models: &[Value],
    network_models: &[Value],
    servers: &[(String, String, Option<String>)],
) -> Vec<ChatModelEntry> {
    use std::collections::HashMap;

    let mut by_name: HashMap<String, ChatModelEntry> = HashMap::new();

    for m in local_models {
        let Some(name) = model_name(m) else { continue };
        if !is_chat_capable(m) {
            continue;
        }
        let kind = m
            .get("_source_kind")
            .and_then(|v| v.as_str())
            .unwrap_or("local")
            .to_string();
        let location = if is_hosted_remote_kind(&kind) {
            "remote"
        } else {
            "local"
        };
        let source_label = server_label_for(m, servers);
        by_name.insert(
            name.to_string(),
            ChatModelEntry {
                name: name.to_string(),
                location: location.to_string(),
                source_kind: kind,
                source_label,
                loaded: loaded_flag(m),
            },
        );
    }

    for m in network_models {
        let Some(name) = model_name(m) else { continue };
        if !is_chat_capable(m) {
            continue;
        }
        // Prefer local entry when the same name is already routed locally.
        if by_name.contains_key(name) {
            continue;
        }
        let (source_kind, source_label) = network_source(m);
        by_name.insert(
            name.to_string(),
            ChatModelEntry {
                name: name.to_string(),
                location: "remote".to_string(),
                source_kind,
                source_label,
                loaded: loaded_flag(m),
            },
        );
    }

    let mut list: Vec<_> = by_name.into_values().collect();
    list.sort_by(|a, b| a.name.cmp(&b.name));
    list
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hosted_kinds_are_remote() {
        assert!(is_hosted_remote_kind("openai"));
        assert!(is_hosted_remote_kind("OpenAI"));
        assert!(is_hosted_remote_kind("custom"));
        assert!(is_hosted_remote_kind("customendpoint"));
        assert!(!is_hosted_remote_kind("ollama"));
        assert!(!is_hosted_remote_kind("vllm"));
        assert!(!is_hosted_remote_kind("inference-cell"));
        assert!(!is_hosted_remote_kind("lmstudio"));
    }

    #[test]
    fn local_ollama_is_local() {
        let local = vec![json!({
            "name": "llama3.2:latest",
            "_source_kind": "ollama",
            "_source_server": "srv1",
            "_status": { "loaded": true },
        })];
        let servers = vec![(
            "srv1".to_string(),
            "ollama".to_string(),
            Some("Ollama".to_string()),
        )];
        let list = build_chat_models(&local, &[], &servers);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].location, "local");
        assert_eq!(list[0].source_kind, "ollama");
        assert_eq!(list[0].source_label, "Ollama");
        assert_eq!(list[0].loaded, Some(true));
    }

    #[test]
    fn openai_attached_is_remote() {
        let local = vec![json!({
            "name": "gpt-4o",
            "_source_kind": "openai",
            "_source_server": "oa1",
        })];
        let servers = vec![("oa1".to_string(), "openai".to_string(), None)];
        let list = build_chat_models(&local, &[], &servers);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].location, "remote");
        assert_eq!(list[0].source_kind, "openai");
    }

    #[test]
    fn custom_attached_is_remote() {
        let local = vec![json!({
            "name": "my-api",
            "_source_kind": "custom",
            "_source_server": "c1",
        })];
        let list = build_chat_models(&local, &[], &[]);
        assert_eq!(list[0].location, "remote");
    }

    #[test]
    fn network_only_is_remote() {
        let network = vec![json!({
            "name": "qwen2.5:32b",
            "_clusters": [{ "cluster_id": "abc", "name": "europe" }],
        })];
        let list = build_chat_models(&[], &network, &[]);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].location, "remote");
        assert_eq!(list[0].source_kind, "cluster");
        assert_eq!(list[0].source_label, "europe");
    }

    #[test]
    fn local_wins_over_network_same_name() {
        let local = vec![json!({
            "name": "shared",
            "_source_kind": "ollama",
            "_source_server": "s1",
        })];
        let network = vec![json!({
            "name": "shared",
            "_swarm_id": "sw1",
        })];
        let list = build_chat_models(&local, &network, &[]);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].location, "local");
        assert_eq!(list[0].source_kind, "ollama");
    }

    #[test]
    fn embed_only_omitted() {
        let local = vec![
            json!({
                "name": "nomic-embed",
                "_source_kind": "ollama",
                "_show_info": { "capabilities": ["embedding"] },
            }),
            json!({
                "name": "chatty",
                "_source_kind": "ollama",
                "_show_info": { "capabilities": ["completion"] },
            }),
        ];
        let list = build_chat_models(&local, &[], &[]);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "chatty");
    }

    #[test]
    fn sorted_by_name() {
        let local = vec![
            json!({ "name": "zeta", "_source_kind": "ollama" }),
            json!({ "name": "alpha", "_source_kind": "vllm" }),
        ];
        let list = build_chat_models(&local, &[], &[]);
        assert_eq!(list[0].name, "alpha");
        assert_eq!(list[1].name, "zeta");
    }
}
