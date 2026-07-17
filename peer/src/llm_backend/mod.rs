mod auth;
mod custom;
mod ollama;
mod openai_compat;

pub use custom::CustomEndpointBackend;
pub use ollama::OllamaBackend;
pub use openai_compat::OpenAiCompatBackend;

use crate::client_config::{is_custom_server_kind, is_inference_cell_kind, LlmServerEntry};
use crate::ollama_client::{build_model_catalog, gpu_probe_mode, GpuProbeMode, ModelCatalogSnapshot};
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LlmBackendKind {
    Ollama,
    OpenAiCompat,
    LocalAi,
    Custom,
}

#[derive(Debug, Clone)]
pub enum LlmBackend {
    Ollama(OllamaBackend),
    OpenAiCompat(OpenAiCompatBackend),
    Custom(CustomEndpointBackend),
}

impl LlmBackend {
    pub fn from_config(
        kind_str: &str,
        base_url: &str,
        client: Client,
    ) -> Result<Self> {
        Self::from_server_entry(
            &LlmServerEntry {
                id: String::new(),
                kind: kind_str.to_string(),
                url: base_url.to_string(),
                label: None,
                attached: false,
                order: 0,
                source: "legacy".to_string(),
                api_type: None,
                models: Vec::new(),
                advertise_to_cluster: true,
            },
            None,
            client,
        )
    }

    pub fn from_server_entry(
        entry: &LlmServerEntry,
        api_key: Option<String>,
        client: Client,
    ) -> Result<Self> {
        let kind = parse_kind(&entry.kind);
        let url = entry.url.trim_end_matches('/').to_string();
        match kind {
            LlmBackendKind::Ollama => Ok(LlmBackend::Ollama(OllamaBackend {
                base_url: url,
                client,
                api_key,
            })),
            LlmBackendKind::OpenAiCompat | LlmBackendKind::LocalAi => Ok(LlmBackend::OpenAiCompat(
                OpenAiCompatBackend {
                    base_url: url,
                    client,
                    label: entry.kind.clone(),
                    api_key,
                },
            )),
            LlmBackendKind::Custom => Ok(LlmBackend::Custom(CustomEndpointBackend {
                base_url: url,
                client,
                api_key,
                models: entry.models.clone(),
                api_type: entry
                    .api_type
                    .clone()
                    .unwrap_or_else(|| "chat-completions".to_string()),
            })),
        }
    }

    /// Like `from_server_entry`, but probes inference-cell `/mtrxai/v1/info` to pick Ollama vs OpenAI-compat.
    pub async fn from_server_entry_probed(
        entry: &LlmServerEntry,
        api_key: Option<String>,
        client: Client,
    ) -> Result<Self> {
        if is_inference_cell_kind(&entry.kind) {
            if let Ok(Some(engine)) = probe_inference_cell_engine(&client, &entry.url).await {
                if engine == "ollama" {
                    let url = entry.url.trim_end_matches('/').to_string();
                    return Ok(LlmBackend::Ollama(OllamaBackend {
                        base_url: url,
                        client,
                        api_key,
                    }));
                }
            }
        }
        Self::from_server_entry(entry, api_key, client)
    }

    pub fn kind(&self) -> LlmBackendKind {
        match self {
            LlmBackend::Ollama(_) => LlmBackendKind::Ollama,
            LlmBackend::OpenAiCompat(b) if b.label == "localai" => LlmBackendKind::LocalAi,
            LlmBackend::OpenAiCompat(_) => LlmBackendKind::OpenAiCompat,
            LlmBackend::Custom(_) => LlmBackendKind::Custom,
        }
    }

    pub fn kind_str(&self) -> &str {
        match self {
            LlmBackend::Ollama(_) => "ollama",
            LlmBackend::OpenAiCompat(b) => &b.label,
            LlmBackend::Custom(_) => "custom",
        }
    }

    pub fn base_url(&self) -> &str {
        match self {
            LlmBackend::Ollama(b) => &b.base_url,
            LlmBackend::OpenAiCompat(b) => &b.base_url,
            LlmBackend::Custom(b) => &b.base_url,
        }
    }

    pub fn supports_ollama_native(&self) -> bool {
        matches!(self, LlmBackend::Ollama(_))
    }

    pub fn chat_path(&self) -> &str {
        "/v1/chat/completions"
    }

    pub fn chat_path_for_model(&self, model: &str) -> String {
        match self {
            LlmBackend::Custom(b) => b.chat_path_for_model(model),
            _ => format!("{}{}", self.base_url(), self.chat_path()),
        }
    }

    pub fn is_full_url_path(&self, model: &str) -> bool {
        match self {
            LlmBackend::Custom(b) => b
                .models
                .iter()
                .find(|m| m.id == model)
                .and_then(|m| m.url.as_deref())
                .map(|u| u.starts_with("http://") || u.starts_with("https://"))
                .unwrap_or(false),
            _ => false,
        }
    }

    pub async fn health_check(&self) -> Result<()> {
        match self {
            LlmBackend::Ollama(b) => b.health_check().await,
            LlmBackend::OpenAiCompat(b) => b.health_check().await,
            LlmBackend::Custom(b) => b.health_check().await,
        }
    }

    pub async fn list_models(
        &self,
        show_cache: &mut HashMap<String, Value>,
        gpu_probe: GpuProbeMode,
    ) -> Result<ModelCatalogSnapshot> {
        match self {
            LlmBackend::Ollama(b) => {
                build_model_catalog(&b.client, &b.base_url, show_cache, gpu_probe).await
            }
            LlmBackend::OpenAiCompat(b) => b.list_models(gpu_probe).await,
            LlmBackend::Custom(b) => b.list_models(gpu_probe).await,
        }
    }

    pub async fn forward_post(&self, path: &str, body: &Value) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.client().post(&url).json(body);
        req = auth::apply_api_key(req, self.api_key());
        req.send().await
    }

    pub async fn forward_post_to_url(
        &self,
        url: &str,
        body: &Value,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self.client().post(url).json(body);
        req = auth::apply_api_key(req, self.api_key());
        req.send().await
    }

    pub async fn forward_get(&self, path: &str) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.client().get(&url);
        req = auth::apply_api_key(req, self.api_key());
        req.send().await
    }

    pub async fn forward_post_raw(
        &self,
        path: &str,
        body: String,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            format!("{}{}", self.base_url(), path)
        };
        let mut req = self
            .client()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body);
        req = auth::apply_api_key(req, self.api_key());
        req.send().await
    }

    /// Ollama `/api/delete` requires DELETE on current releases; older builds accept POST.
    pub async fn forward_delete_raw(
        &self,
        path: &str,
        model_name: &str,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            format!("{}{}", self.base_url(), path)
        };
        let body = serde_json::json!({ "model": model_name }).to_string();
        let mut req = self
            .client()
            .request(reqwest::Method::DELETE, &url)
            .header("Content-Type", "application/json")
            .body(body);
        req = auth::apply_api_key(req, self.api_key());
        let resp = req.send().await?;
        if resp.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            let fallback = serde_json::json!({ "model": model_name, "name": model_name }).to_string();
            let mut req = self
                .client()
                .post(&url)
                .header("Content-Type", "application/json")
                .body(fallback);
            req = auth::apply_api_key(req, self.api_key());
            return req.send().await;
        }
        Ok(resp)
    }

    pub async fn synthesize_tags(&self) -> Result<Value> {
        match self {
            LlmBackend::Ollama(b) => {
                let mut req = b.client.get(format!("{}/api/tags", b.base_url));
                req = auth::apply_api_key(req, b.api_key.as_deref());
                let resp = req.send().await.map_err(|e| anyhow!(e))?;
                Ok(resp.json().await.map_err(|e| anyhow!(e))?)
            }
            LlmBackend::OpenAiCompat(b) => b.synthesize_tags().await,
            LlmBackend::Custom(b) => b.synthesize_tags().await,
        }
    }

    pub async fn synthesize_v1_models(&self) -> Result<Value> {
        match self {
            LlmBackend::Ollama(b) => {
                let mut req = b.client.get(format!("{}/v1/models", b.base_url));
                req = auth::apply_api_key(req, b.api_key.as_deref());
                let resp = req.send().await.map_err(|e| anyhow!(e))?;
                Ok(resp.json().await.map_err(|e| anyhow!(e))?)
            }
            LlmBackend::OpenAiCompat(b) => {
                let mut req = b.client.get(format!("{}/v1/models", b.base_url));
                req = auth::apply_api_key(req, b.api_key.as_deref());
                let resp = req.send().await.map_err(|e| anyhow!(e))?;
                Ok(resp.json().await.map_err(|e| anyhow!(e))?)
            }
            LlmBackend::Custom(b) => {
                let snapshot = b.list_models(gpu_probe_mode()).await?;
                let data: Vec<Value> = snapshot
                    .models
                    .iter()
                    .filter_map(|m| {
                        let id = m.get("name").and_then(|n| n.as_str())?;
                        Some(serde_json::json!({
                            "id": id,
                            "object": "model",
                            "created": 0,
                            "owned_by": "custom",
                        }))
                    })
                    .collect();
                Ok(serde_json::json!({ "object": "list", "data": data }))
            }
        }
    }

    fn api_key(&self) -> Option<&str> {
        match self {
            LlmBackend::Ollama(b) => b.api_key.as_deref(),
            LlmBackend::OpenAiCompat(b) => b.api_key.as_deref(),
            LlmBackend::Custom(b) => b.api_key.as_deref(),
        }
    }

    fn client(&self) -> &Client {
        match self {
            LlmBackend::Ollama(b) => &b.client,
            LlmBackend::OpenAiCompat(b) => &b.client,
            LlmBackend::Custom(b) => &b.client,
        }
    }
}

pub fn parse_kind(s: &str) -> LlmBackendKind {
    if is_custom_server_kind(s) {
        return LlmBackendKind::Custom;
    }
    match s.to_lowercase().as_str() {
        "ollama" => LlmBackendKind::Ollama,
        "localai" => LlmBackendKind::LocalAi,
        "openai" | "vllm" | "llamacpp" | "llama.cpp" | "inference-cell" | "inference_cell"
        | "lmstudio" | "lm-studio" | "tgi" | "tensorrt" | "tensorrt-llm" => {
            LlmBackendKind::OpenAiCompat
        }
        _ => LlmBackendKind::Ollama,
    }
}

#[derive(Debug, Deserialize)]
struct InferenceCellInfo {
    engine: String,
}

/// Returns the icell engine (`llamacpp` or `ollama`) when the server responds to `/mtrxai/v1/info`,
/// or by probing inference routes when the admin info endpoint is unavailable.
pub async fn probe_inference_cell_engine(
    client: &Client,
    base_url: &str,
) -> Result<Option<String>> {
    let base = base_url.trim_end_matches('/');

    let info_url = format!("{base}/mtrxai/v1/info");
    if let Ok(resp) = client.get(&info_url).send().await {
        if resp.status().is_success() {
            if let Ok(info) = resp.json::<InferenceCellInfo>().await {
                return Ok(Some(info.engine));
            }
        }
    }

    // Ollama icell proxies /api/* and /v1/*; llama.cpp icell exposes /v1/models.
    let tags_url = format!("{base}/api/tags");
    if let Ok(resp) = client.get(&tags_url).send().await {
        if resp.status().is_success() {
            return Ok(Some("ollama".to_string()));
        }
    }

    let models_url = format!("{base}/v1/models");
    if let Ok(resp) = client.get(&models_url).send().await {
        if resp.status().is_success() {
            return Ok(Some("llamacpp".to_string()));
        }
    }

    Ok(None)
}

pub fn normalize_backend_label(s: &str) -> String {
    if is_custom_server_kind(s) {
        return "custom".to_string();
    }
    match s.to_lowercase().as_str() {
        "vllm" => "vllm".to_string(),
        "llamacpp" | "llama.cpp" => "llamacpp".to_string(),
        "inference-cell" | "inference_cell" => "inference-cell".to_string(),
        "lmstudio" | "lm-studio" => "lmstudio".to_string(),
        "localai" => "localai".to_string(),
        "tgi" => "tgi".to_string(),
        "tensorrt" | "tensorrt-llm" => "tensorrt".to_string(),
        "openai" => "openai".to_string(),
        _ => "ollama".to_string(),
    }
}

pub fn default_gpu_probe() -> GpuProbeMode {
    gpu_probe_mode()
}

pub fn backend_help(kind: LlmBackendKind) -> &'static str {
    match kind {
        LlmBackendKind::Ollama => "Make sure Ollama is running: ollama serve",
        LlmBackendKind::OpenAiCompat => {
            "Start your OpenAI-compatible server (vLLM, llama.cpp, LM Studio)"
        }
        LlmBackendKind::LocalAi => "Start LocalAI on the configured port",
        LlmBackendKind::Custom => "Configure a custom chat-completions endpoint with static models",
    }
}

pub fn ensure_backend(config: &crate::client_config::ClientConfig, client: Client) -> Result<LlmBackend> {
    let kind = config.llm_backend.as_deref().unwrap_or("ollama");
    let url = config
        .llm_url
        .as_deref()
        .ok_or_else(|| anyhow!("LLM URL not configured"))?;
    LlmBackend::from_config(kind, url, client)
}

pub fn ensure_backend_from_config(
    config: &crate::client_config::ClientConfig,
    client: Client,
) -> Result<LlmBackend> {
    ensure_backend(config, client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inference_cell_kind_is_openai_compat() {
        assert_eq!(parse_kind("inference-cell"), LlmBackendKind::OpenAiCompat);
        assert_eq!(normalize_backend_label("inference_cell"), "inference-cell");
    }

    #[test]
    fn parses_inference_cell_info_json() {
        let raw = r#"{"engine":"ollama","inference_api":"ollama","admin_api":"mtrxai/v1","version":"0.1.0"}"#;
        let info: InferenceCellInfo = serde_json::from_str(raw).unwrap();
        assert_eq!(info.engine, "ollama");
    }
}
