pub use crate::gpu::{
    gpu_probe_mode, parse_gpu_from_ollama_info, probe_gpu_host, probe_gpu_nvidia_smi,
    probe_gpu_via_ollama, GpuProbeMode,
};
use crate::shared::GpuHostStatus;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRuntimeStatus {
    pub loaded: bool,
    pub processor: String,
    pub cpu_pct: u8,
    pub gpu_pct: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_vram: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ModelCatalogSnapshot {
    pub models: Vec<Value>,
    pub model_names: Vec<String>,
    pub gpu_host: Option<GpuHostStatus>,
}

/// Derive CPU/GPU split from Ollama `/api/ps` fields (same logic as `ollama ps` CLI).
pub fn compute_processor_status(size: u64, size_vram: Option<u64>) -> (String, u8, u8) {
    let vram = size_vram.unwrap_or(0);
    if size == 0 {
        return ("cpu".to_string(), 100, 0);
    }
    if vram == 0 {
        return ("cpu".to_string(), 100, 0);
    }
    if vram >= size {
        return ("gpu".to_string(), 0, 100);
    }
    let gpu_pct = ((vram as f64 / size as f64) * 100.0).floor() as u8;
    let cpu_pct = 100u8.saturating_sub(gpu_pct);
    ("mixed".to_string(), cpu_pct, gpu_pct)
}

/// True when a model is loaded and fully resident on GPU (not CPU-only or mixed).
pub fn model_is_fully_gpu_loaded(model: &Value) -> bool {
    let Some(status) = model.get("_status") else {
        return false;
    };
    if !status
        .get("loaded")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return false;
    }
    if status.get("processor").and_then(|v| v.as_str()) == Some("gpu") {
        return true;
    }
    status.get("gpu_pct").and_then(|v| v.as_u64()) == Some(100)
}

/// True when a model may be advertised to cluster/swarm peers.
///
/// Any installed catalog entry is advertisable. GPU vs CPU residency is exposed in
/// `_status` for peer ranking — it must not omit active (loaded) models from the
/// network catalog, or CPU/mixed loads (common in containers) disappear while
/// "active" locally.
pub fn model_is_advertisable_for_network(model: &Value) -> bool {
    model
        .get("name")
        .and_then(|n| n.as_str())
        .map(|n| !n.trim().is_empty())
        .unwrap_or(false)
}

pub fn unloaded_status() -> ModelRuntimeStatus {
    ModelRuntimeStatus {
        loaded: false,
        processor: "cpu".to_string(),
        cpu_pct: 0,
        gpu_pct: 0,
        size: None,
        size_vram: None,
        expires_at: None,
    }
}

pub fn runtime_status_from_ps(ps: &Value) -> ModelRuntimeStatus {
    let size = ps.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
    let size_vram = ps.get("size_vram").and_then(|v| v.as_u64());
    let (processor, cpu_pct, gpu_pct) = compute_processor_status(size, size_vram);
    ModelRuntimeStatus {
        loaded: true,
        processor,
        cpu_pct,
        gpu_pct,
        size: Some(size),
        size_vram,
        expires_at: ps
            .get("expires_at")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    }
}

pub async fn fetch_installed_models(client: &Client, ollama_url: &str) -> anyhow::Result<Vec<Value>> {
    let url = format!("{}/api/tags", ollama_url.trim_end_matches('/'));
    let res = client.get(&url).send().await?;
    let json: Value = res.json().await?;
    Ok(json
        .get("models")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default())
}

pub async fn fetch_running_models(
    client: &Client,
    ollama_url: &str,
) -> anyhow::Result<HashMap<String, Value>> {
    let url = format!("{}/api/ps", ollama_url.trim_end_matches('/'));
    let res = client.get(&url).send().await?;
    let json: Value = res.json().await?;
    let mut running = HashMap::new();
    if let Some(arr) = json.get("models").and_then(|m| m.as_array()) {
        for m in arr {
            let name = m
                .get("name")
                .or_else(|| m.get("model"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            if !name.is_empty() {
                running.insert(name.to_string(), m.clone());
            }
        }
    }
    Ok(running)
}

pub async fn fetch_show_info(
    client: &Client,
    ollama_url: &str,
    name: &str,
) -> anyhow::Result<Value> {
    let url = format!("{}/api/show", ollama_url.trim_end_matches('/'));
    let res = client
        .post(&url)
        .json(&json!({ "name": name }))
        .send()
        .await?;
    Ok(res.json().await?)
}

pub fn model_has_capability(show: &Value, cap: &str) -> bool {
    show.get("capabilities")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .any(|s| s == cap)
        })
        .unwrap_or(false)
}

pub fn supports_generate(show: Option<&Value>) -> bool {
    match show {
        None => true,
        Some(s) => {
            let Some(arr) = s.get("capabilities").and_then(|c| c.as_array()) else {
                return true;
            };
            if arr.is_empty() {
                return true;
            }
            arr.iter()
                .filter_map(|v| v.as_str())
                .any(|c| c == "completion")
        }
    }
}

pub fn supports_embed(show: Option<&Value>) -> bool {
    show.map(|s| model_has_capability(s, "embedding"))
        .unwrap_or(false)
}

pub fn is_embed_only(show: Option<&Value>) -> bool {
    supports_embed(show) && !supports_generate(show)
}

async fn load_embed_model(
    client: &Client,
    ollama_url: &str,
    model: &str,
    keep_alive: impl Serialize,
) -> anyhow::Result<()> {
    let url = format!("{}/api/embed", ollama_url.trim_end_matches('/'));
    let res = client
        .post(&url)
        .json(&json!({
            "model": model,
            "input": ".",
            "keep_alive": keep_alive,
        }))
        .send()
        .await?;
    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("embed load failed: {text}");
    }
    Ok(())
}

pub fn merge_model_status(
    installed: &[Value],
    running: &HashMap<String, Value>,
    show_cache: &HashMap<String, Value>,
) -> Vec<Value> {
    installed
        .iter()
        .filter_map(|m| {
            let name = m.get("name").and_then(|n| n.as_str())?;
            let mut out = m.clone();
            let status = if let Some(ps) = running.get(name) {
                runtime_status_from_ps(ps)
            } else {
                unloaded_status()
            };
            if let Some(obj) = out.as_object_mut() {
                obj.insert(
                    "_status".to_string(),
                    serde_json::to_value(status).unwrap_or(Value::Null),
                );
                if let Some(show) = show_cache.get(name) {
                    obj.insert("_show_info".to_string(), show.clone());
                }
            }
            Some(out)
        })
        .collect()
}

pub fn catalog_hash(models: &[Value], gpu_host: &Option<GpuHostStatus>) -> String {
    let payload = json!({ "models": models, "gpu_host": gpu_host });
    let text = serde_json::to_string(&payload).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn build_model_catalog(
    client: &Client,
    ollama_url: &str,
    show_cache: &mut HashMap<String, Value>,
    gpu_probe: GpuProbeMode,
) -> anyhow::Result<ModelCatalogSnapshot> {
    let installed = fetch_installed_models(client, ollama_url).await.unwrap_or_default();
    let running = fetch_running_models(client, ollama_url)
        .await
        .unwrap_or_default();

    for m in &installed {
        if let Some(name) = m.get("name").and_then(|n| n.as_str()) {
            if !show_cache.contains_key(name) {
                if let Ok(show) = fetch_show_info(client, ollama_url, name).await {
                    show_cache.insert(name.to_string(), show);
                }
            }
        }
    }

    let models = merge_model_status(&installed, &running, show_cache);
    let model_names: Vec<String> = models
        .iter()
        .filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(str::to_string))
        .collect();

    let gpu_host = crate::gpu::probe_gpu_host(gpu_probe, Some(client), Some(ollama_url)).await;

    Ok(ModelCatalogSnapshot {
        models,
        model_names,
        gpu_host,
    })
}

pub fn model_poll_interval_secs() -> u64 {
    std::env::var("MTRXAI_MODEL_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

pub async fn pull_model_with_progress<F>(
    client: &Client,
    ollama_url: &str,
    model: &str,
    mut on_progress: F,
) -> anyhow::Result<()>
where
    F: FnMut(u8),
{
    use futures_util::StreamExt;
    let url = format!("{}/api/pull", ollama_url.trim_end_matches('/'));
    let res = client
        .post(&url)
        .json(&json!({ "name": model, "stream": true }))
        .send()
        .await?;
    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("pull failed: {text}");
    }
    let mut stream = res.bytes_stream();
    let mut last_pct = 0u8;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for line in String::from_utf8_lossy(&chunk).lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(val) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if val.get("error").is_some() {
                anyhow::bail!("{}", val["error"]);
            }
            let completed = val.get("completed").and_then(|v| v.as_u64()).unwrap_or(0);
            let total = val.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
            if total > 0 {
                let pct = ((completed as f64 / total as f64) * 100.0).floor() as u8;
                if pct > last_pct {
                    last_pct = pct;
                    on_progress(pct);
                }
            }
            if val.get("status").and_then(|v| v.as_str()) == Some("success") {
                on_progress(100);
                return Ok(());
            }
        }
    }
    Ok(())
}

pub async fn warmup_model(
    client: &Client,
    ollama_url: &str,
    model: &str,
    show: Option<&Value>,
) -> anyhow::Result<()> {
    load_model(client, ollama_url, model, show).await
}

pub async fn load_model(
    client: &Client,
    ollama_url: &str,
    model: &str,
    show: Option<&Value>,
) -> anyhow::Result<()> {
    if is_embed_only(show) {
        return load_embed_model(client, ollama_url, model, "30m").await;
    }
    if !supports_generate(show) {
        anyhow::bail!(
            "{model} does not support chat generation — it may be an embedding-only model"
        );
    }
    let url = format!("{}/api/generate", ollama_url.trim_end_matches('/'));
    let res = client
        .post(&url)
        .json(&json!({
            "model": model,
            "prompt": ".",
            "stream": false,
            "keep_alive": "30m"
        }))
        .send()
        .await?;
    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("load failed: {text}");
    }
    Ok(())
}

pub async fn unload_model(
    client: &Client,
    ollama_url: &str,
    model: &str,
    show: Option<&Value>,
) -> anyhow::Result<()> {
    if is_embed_only(show) || (supports_embed(show) && !supports_generate(show)) {
        return load_embed_model(client, ollama_url, model, 0).await;
    }
    if !supports_generate(show) {
        anyhow::bail!("{model} does not support unload via generate");
    }
    let url = format!("{}/api/generate", ollama_url.trim_end_matches('/'));
    let res = client
        .post(&url)
        .json(&json!({
            "model": model,
            "prompt": ".",
            "stream": false,
            "keep_alive": 0
        }))
        .send()
        .await?;
    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        anyhow::bail!("unload failed: {text}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_generate_legacy_without_capabilities() {
        assert!(supports_generate(None));
        assert!(supports_generate(Some(&json!({ "details": {} }))));
    }

    #[test]
    fn supports_embed_only_model() {
        let show = json!({ "capabilities": ["embedding"] });
        assert!(supports_embed(Some(&show)));
        assert!(!supports_generate(Some(&show)));
        assert!(is_embed_only(Some(&show)));
    }

    #[test]
    fn supports_completion_model() {
        let show = json!({ "capabilities": ["completion"] });
        assert!(supports_generate(Some(&show)));
        assert!(!is_embed_only(Some(&show)));
    }

    #[test]
    fn cpu_only_when_vram_missing() {
        let (proc, cpu, gpu) = compute_processor_status(1_000_000, None);
        assert_eq!(proc, "cpu");
        assert_eq!(cpu, 100);
        assert_eq!(gpu, 0);
    }

    #[test]
    fn cpu_only_when_vram_zero() {
        let (proc, cpu, gpu) = compute_processor_status(1_000_000, Some(0));
        assert_eq!(proc, "cpu");
        assert_eq!(cpu, 100);
        assert_eq!(gpu, 0);
    }

    #[test]
    fn gpu_only_when_fully_in_vram() {
        let (proc, cpu, gpu) = compute_processor_status(1_000_000, Some(1_000_000));
        assert_eq!(proc, "gpu");
        assert_eq!(cpu, 0);
        assert_eq!(gpu, 100);
    }

    #[test]
    fn mixed_split_matches_ollama_style() {
        let (proc, cpu, gpu) = compute_processor_status(100, Some(17));
        assert_eq!(proc, "mixed");
        assert_eq!(gpu, 17);
        assert_eq!(cpu, 83);
    }

    #[test]
    fn unloaded_status_not_loaded() {
        let s = unloaded_status();
        assert!(!s.loaded);
        assert_eq!(s.cpu_pct, 0);
    }

    #[test]
    fn fully_gpu_loaded_requires_loaded_flag() {
        let m = json!({ "name": "llama3", "_status": { "loaded": false, "processor": "gpu", "gpu_pct": 100 } });
        assert!(!model_is_fully_gpu_loaded(&m));
    }

    #[test]
    fn fully_gpu_loaded_accepts_processor_gpu() {
        let m = json!({ "name": "llama3", "_status": { "loaded": true, "processor": "gpu", "gpu_pct": 100 } });
        assert!(model_is_fully_gpu_loaded(&m));
    }

    #[test]
    fn fully_gpu_loaded_accepts_gpu_pct_only() {
        let m = json!({ "name": "llama3", "_status": { "loaded": true, "processor": "mixed", "gpu_pct": 100 } });
        assert!(model_is_fully_gpu_loaded(&m));
    }

    #[test]
    fn fully_gpu_loaded_rejects_mixed_and_cpu() {
        let mixed = json!({ "name": "a", "_status": { "loaded": true, "processor": "mixed", "gpu_pct": 50 } });
        let cpu = json!({ "name": "b", "_status": { "loaded": true, "processor": "cpu", "gpu_pct": 0 } });
        assert!(!model_is_fully_gpu_loaded(&mixed));
        assert!(!model_is_fully_gpu_loaded(&cpu));
    }

    #[test]
    fn fully_gpu_loaded_rejects_missing_status_and_remote() {
        assert!(!model_is_fully_gpu_loaded(&json!({ "name": "llama3" })));
        let remote = json!({ "name": "gpt", "_status": { "loaded": true, "processor": "remote" } });
        assert!(!model_is_fully_gpu_loaded(&remote));
    }

    #[test]
    fn advertisable_includes_unloaded_full_gpu_and_active_non_gpu() {
        let unloaded = json!({ "name": "a", "_status": { "loaded": false, "processor": "cpu", "gpu_pct": 0 } });
        let gpu = json!({ "name": "b", "_status": { "loaded": true, "processor": "gpu", "gpu_pct": 100 } });
        let mixed = json!({ "name": "c", "_status": { "loaded": true, "processor": "mixed", "gpu_pct": 40 } });
        let cpu = json!({ "name": "d", "_status": { "loaded": true, "processor": "cpu", "gpu_pct": 0 } });
        let remote = json!({ "name": "e", "_status": { "loaded": true, "processor": "remote" } });
        assert!(model_is_advertisable_for_network(&unloaded));
        assert!(model_is_advertisable_for_network(&gpu));
        assert!(model_is_advertisable_for_network(&mixed));
        assert!(model_is_advertisable_for_network(&cpu));
        assert!(model_is_advertisable_for_network(&remote));
        assert!(model_is_advertisable_for_network(&json!({ "name": "f" })));
        assert!(!model_is_advertisable_for_network(&json!({ "name": "" })));
        assert!(!model_is_advertisable_for_network(&json!({})));
    }
}
