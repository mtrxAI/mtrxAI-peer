use anyhow::Result;
use reqwest::Client;

#[derive(Debug, Clone)]
pub struct OllamaBackend {
    pub base_url: String,
    pub client: Client,
    pub api_key: Option<String>,
}

impl OllamaBackend {
    pub async fn health_check(&self) -> Result<()> {
        let mut req = self.client.get(format!("{}/api/tags", self.base_url));
        req = crate::llm_backend::auth::apply_api_key(req, self.api_key.as_deref());
        let resp = req.send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("Ollama returned status {}", resp.status())
        }
    }
}
