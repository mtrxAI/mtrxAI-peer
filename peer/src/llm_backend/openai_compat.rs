use crate::ollama_client::{gpu_probe_mode, probe_gpu_host, GpuProbeMode, ModelCatalogSnapshot};
use anyhow::Result;
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct OpenAiCompatBackend {
    pub base_url: String,
    pub client: Client,
    pub label: String,
    pub api_key: Option<String>,
}

impl OpenAiCompatBackend {
    pub async fn health_check(&self) -> Result<()> {
        let mut req = self.client.get(format!("{}/v1/models", self.base_url));
        req = crate::llm_backend::auth::apply_api_key(req, self.api_key.as_deref());
        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(());
        }
        let mut req = self.client.get(format!("{}/health", self.base_url));
        req = crate::llm_backend::auth::apply_api_key(req, self.api_key.as_deref());
        let resp = req.send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("Server returned status {}", resp.status())
        }
    }

    pub async fn list_models(&self, gpu_probe: GpuProbeMode) -> Result<ModelCatalogSnapshot> {
        let mut req = self.client.get(format!("{}/v1/models", self.base_url));
        req = crate::llm_backend::auth::apply_api_key(req, self.api_key.as_deref());
        let resp = req.send().await?;
        let json: Value = resp.json().await.map_err(|e| anyhow::anyhow!(e))?;
        let data = json
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        let mut models = Vec::new();
        let mut model_names = Vec::new();

        for entry in data {
            let id = entry
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if id.is_empty() {
                continue;
            }
            model_names.push(id.clone());
            let status = entry.get("_status").cloned().unwrap_or_else(|| {
                json!({
                    // OpenAI-compatible backends typically don't expose "loaded/unloaded" state
                    // (unlike Ollama). Treat models as available on disk/servable, not preloaded.
                    "loaded": false,
                    "processor": "unknown",
                    "cpu_pct": 0,
                    "gpu_pct": 0,
                })
            });
            models.push(json!({
                "name": id,
                "model": id,
                "_status": status,
            }));
        }

        let gpu_host = probe_gpu_host(gpu_probe, Some(&self.client), None).await;

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
