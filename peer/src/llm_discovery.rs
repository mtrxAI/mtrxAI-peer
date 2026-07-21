use reqwest::Client;
use serde::Serialize;
use std::collections::HashSet;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredServer {
    pub kind: String,
    pub url: String,
    pub model_count: usize,
    pub latency_ms: u64,
    pub healthy: bool,
}

#[derive(Debug, Clone)]
struct PortProbe {
    url: &'static str,
    paths: &'static [&'static str],
    candidate_kinds: &'static [&'static str],
}

const PORT_PROBES: &[PortProbe] = &[
    PortProbe {
        url: "http://127.0.0.1:11434",
        paths: &["/api/tags"],
        candidate_kinds: &["ollama"],
    },
    PortProbe {
        url: "http://127.0.0.1:8000",
        paths: &["/v1/models"],
        candidate_kinds: &["vllm", "tensorrt"],
    },
    PortProbe {
        url: "http://127.0.0.1:8080",
        paths: &["/v1/models"],
        candidate_kinds: &["llamacpp", "localai", "tgi"],
    },
    PortProbe {
        url: "http://127.0.0.1:80",
        paths: &["/v1/models"],
        candidate_kinds: &["tgi"],
    },
    PortProbe {
        url: "http://127.0.0.1:1234",
        paths: &["/v1/models"],
        candidate_kinds: &["lmstudio"],
    },
];

pub async fn scan_local_servers(client: &Client) -> Vec<DiscoveredServer> {
    let mut handles = Vec::new();
    for probe in PORT_PROBES {
        let client = client.clone();
        let url = probe.url.to_string();
        let paths: Vec<String> = probe.paths.iter().map(|p| p.to_string()).collect();
        let kinds: Vec<String> = probe
            .candidate_kinds
            .iter()
            .map(|k| k.to_string())
            .collect();
        handles.push(tokio::spawn(async move {
            probe_port(&client, &url, &paths, &kinds).await
        }));
    }

    let mut results = Vec::new();
    let mut seen_urls = HashSet::new();
    for handle in handles {
        if let Ok(Some(server)) = handle.await {
            if seen_urls.insert(server.url.clone()) {
                results.push(server);
            }
        }
    }
    results.sort_by_key(|s| s.latency_ms);
    results
}

async fn probe_port(
    client: &Client,
    base_url: &str,
    paths: &[String],
    candidate_kinds: &[String],
) -> Option<DiscoveredServer> {
    for path in paths {
        if let Some(server) = probe_endpoint(client, base_url, path, candidate_kinds).await {
            return Some(server);
        }
    }
    None
}

async fn probe_endpoint(
    client: &Client,
    base_url: &str,
    path: &str,
    candidate_kinds: &[String],
) -> Option<DiscoveredServer> {
    let started = Instant::now();
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let server_header = resp
        .headers()
        .get("server")
        .and_then(|v| v.to_str().ok())
        .map(str::to_lowercase);
    let json: serde_json::Value = resp.json().await.ok()?;

    if json.get("models").and_then(|m| m.as_array()).is_some() {
        let model_count = json
            .get("models")
            .and_then(|m| m.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        return Some(DiscoveredServer {
            kind: "ollama".to_string(),
            url: base_url.to_string(),
            model_count,
            latency_ms: started.elapsed().as_millis() as u64,
            healthy: true,
        });
    }

    let model_count = json
        .get("data")
        .and_then(|d| d.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    let kind = classify_openai_compat(&json, candidate_kinds, server_header.as_deref());

    Some(DiscoveredServer {
        kind,
        url: base_url.to_string(),
        model_count,
        latency_ms: started.elapsed().as_millis() as u64,
        healthy: true,
    })
}

fn classify_openai_compat(
    json: &serde_json::Value,
    candidate_kinds: &[String],
    server_header: Option<&str>,
) -> String {
    if let Some(header) = server_header {
        if header.contains("tgi") || header.contains("text-generation") {
            return "tgi".to_string();
        }
        if header.contains("uvicorn") && candidate_kinds.iter().any(|k| k == "vllm") {
            return "vllm".to_string();
        }
        if header.contains("localai") {
            return "localai".to_string();
        }
    }

    if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
        for entry in data {
            if let Some(id) = entry.get("id").and_then(|v| v.as_str()) {
                let id_lower = id.to_lowercase();
                if id_lower.contains("tensorrt") {
                    return "tensorrt".to_string();
                }
            }
            if let Some(owned_by) = entry.get("owned_by").and_then(|v| v.as_str()) {
                let owned_lower = owned_by.to_lowercase();
                if owned_lower.contains("vllm") {
                    return "vllm".to_string();
                }
                if owned_lower.contains("localai") {
                    return "localai".to_string();
                }
            }
        }
    }

    candidate_kinds
        .first()
        .cloned()
        .unwrap_or_else(|| "openai_compat".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_ollama_shape_not_used_for_openai() {
        let json = serde_json::json!({ "data": [{ "id": "llama3" }] });
        let kind = classify_openai_compat(&json, &["vllm".to_string()], None);
        assert_eq!(kind, "vllm");
    }

    #[test]
    fn classify_tgi_from_server_header() {
        let json = serde_json::json!({ "data": [] });
        let kind = classify_openai_compat(&json, &["llamacpp".to_string()], Some("tgi"));
        assert_eq!(kind, "tgi");
    }
}
