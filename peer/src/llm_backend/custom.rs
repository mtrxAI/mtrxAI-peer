use crate::client_config::CustomModelEntry;
use crate::ollama_client::{gpu_probe_mode, probe_gpu_host, GpuProbeMode, ModelCatalogSnapshot};
use anyhow::Result;
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct CustomEndpointBackend {
    pub base_url: String,
    pub client: Client,
    pub api_key: Option<String>,
    pub models: Vec<CustomModelEntry>,
    pub api_type: String,
}

impl CustomEndpointBackend {
    pub fn chat_path_for_model(&self, model: &str) -> String {
        self.models
            .iter()
            .find(|m| m.id == model)
            .and_then(|m| m.url.as_deref())
            .filter(|u| !u.is_empty())
            .map(|u| {
                if u.starts_with("http://") || u.starts_with("https://") {
                    u.to_string()
                } else if u.starts_with('/') {
                    format!("{}{}", self.base_url.trim_end_matches('/'), u)
                } else {
                    format!("{}/{}", self.base_url.trim_end_matches('/'), u)
                }
            })
            .unwrap_or_else(|| format!("{}{}", self.base_url.trim_end_matches('/'), "/v1/chat/completions"))
    }

    pub async fn health_check(&self) -> Result<()> {
        let health_url = format!("{}/health", self.base_url.trim_end_matches('/'));
        if let Ok(resp) = self.client.get(&health_url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        let models_url = format!("{}/v1/models", self.base_url.trim_end_matches('/'));
        if let Ok(resp) = self.client.get(&models_url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        if !self.models.is_empty() {
            return Ok(());
        }
        anyhow::bail!("custom endpoint health check failed")
    }

    pub async fn list_models(&self, gpu_probe: GpuProbeMode) -> Result<ModelCatalogSnapshot> {
        let mut models = Vec::new();
        let mut model_names = Vec::new();

        for entry in &self.models {
            let id = entry.id.trim();
            if id.is_empty() {
                continue;
            }
            model_names.push(id.to_string());
            let mut status = json!({
                "loaded": true,
                "processor": "remote",
            });
            if let Some(tool_calling) = entry.tool_calling {
                status["tool_calling"] = json!(tool_calling);
            }
            if let Some(vision) = entry.vision {
                status["vision"] = json!(vision);
            }
            if let Some(max_in) = entry.max_input_tokens {
                status["max_input_tokens"] = json!(max_in);
            }
            if let Some(max_out) = entry.max_output_tokens {
                status["max_output_tokens"] = json!(max_out);
            }
            models.push(json!({
                "name": id,
                "model": id,
                "details": {
                    "display_name": entry.display_name(),
                },
                "_status": status,
            }));
        }

        let gpu_host = probe_gpu_host(gpu_probe, None, None).await;

        Ok(ModelCatalogSnapshot {
            models,
            model_names,
            gpu_host,
        })
    }

    pub async fn synthesize_tags(&self) -> Result<Value> {
        let snapshot = self.list_models(gpu_probe_mode()).await?;
        Ok(json!({ "models": snapshot.models }))
    }
}
