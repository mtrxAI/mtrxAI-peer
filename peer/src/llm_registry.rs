use crate::client_config::{
    is_inference_cell_kind, validate_custom_server_entry, ClientConfig, LlmServerEntry,
};
use crate::llm_backend::{normalize_backend_label, probe_inference_cell_engine, LlmBackend};
use crate::ollama_client::{gpu_probe_mode, GpuProbeMode, ModelCatalogSnapshot};
use crate::tx_db::TxStore;
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelCollision {
    pub name: String,
    pub skipped_server: String,
}

#[derive(Debug, Clone)]
pub struct MergedCatalog {
    pub models: Vec<Value>,
    pub model_names: Vec<String>,
    pub gpu_host: Option<crate::shared::GpuHostStatus>,
    pub collisions: Vec<ModelCollision>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmServerView {
    pub id: String,
    pub kind: String,
    pub url: String,
    pub label: Option<String>,
    pub attached: bool,
    pub order: u32,
    pub source: String,
    pub model_count: usize,
    pub healthy: bool,
    pub has_api_key: bool,
    pub has_admin_token: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_type: Option<String>,
    pub models: Vec<crate::client_config::CustomModelEntry>,
    pub advertise_to_cluster: bool,
    /// For inference-cell servers: `llamacpp` or `ollama` (from `/mtrxai/v1/info`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference_engine: Option<String>,
    /// Model catalog source for this server: `ollama` library or `huggingface` GGUF.
    pub catalog_source: String,
}

struct ServerRuntime {
    entry: LlmServerEntry,
    backend: Arc<LlmBackend>,
    show_cache: HashMap<String, Value>,
    inference_engine: Option<String>,
}

struct RegistryInner {
    servers: HashMap<String, ServerRuntime>,
    model_routes: HashMap<String, String>,
    merged: Option<MergedCatalog>,
}

#[derive(Clone)]
pub struct LlmServerRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    http_client: Arc<RwLock<Client>>,
    tx_store: Arc<TxStore>,
}

impl LlmServerRegistry {
    pub fn new(http_client: Arc<RwLock<Client>>, tx_store: Arc<TxStore>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                servers: HashMap::new(),
                model_routes: HashMap::new(),
                merged: None,
            })),
            http_client,
            tx_store,
        }
    }

    async fn load_api_key(
        &self,
        server_id: &str,
        config: &ClientConfig,
    ) -> Result<Option<String>> {
        self.tx_store
            .get_server_api_key(
                server_id,
                config.peer_id.as_deref(),
                config.service_id.as_deref(),
            )
            .await
    }

    async fn build_backend(
        &self,
        entry: &LlmServerEntry,
        config: &ClientConfig,
    ) -> Result<(LlmBackend, Option<String>)> {
        validate_custom_server_entry(entry).map_err(|e| anyhow!(e))?;
        let api_key = self.load_api_key(&entry.id, config).await?;
        let client = self.http_client.read().await.clone();
        let inference_engine = if is_inference_cell_kind(&entry.kind) {
            probe_inference_cell_engine(&client, &entry.url)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let backend =
            LlmBackend::from_server_entry_probed(entry, api_key, client).await?;
        Ok((backend, inference_engine))
    }

    pub async fn sync_from_config(&self, config: &ClientConfig) -> Result<()> {
        let mut inner = self.inner.write().await;
        let attached_ids: HashSet<_> = config
            .llm_servers
            .iter()
            .filter(|s| s.attached)
            .map(|s| s.id.clone())
            .collect();

        inner.servers.retain(|id, _| attached_ids.contains(id));

        for entry in &config.llm_servers {
            if !entry.attached {
                continue;
            }
            if inner.servers.contains_key(&entry.id) {
                if let Some(runtime) = inner.servers.get_mut(&entry.id) {
                    runtime.entry = entry.clone();
                    let (backend, inference_engine) = self.build_backend(entry, config).await?;
                    runtime.backend = Arc::new(backend);
                    runtime.inference_engine = inference_engine;
                }
                continue;
            }
            let (backend, inference_engine) = self.build_backend(entry, config).await?;
            inner.servers.insert(
                entry.id.clone(),
                ServerRuntime {
                    entry: entry.clone(),
                    backend: Arc::new(backend),
                    show_cache: HashMap::new(),
                    inference_engine,
                },
            );
        }

        Ok(())
    }

    pub async fn find_or_add_entry(
        &self,
        config: &mut ClientConfig,
        entry: LlmServerEntry,
    ) -> String {
        let kind = normalize_backend_label(&entry.kind);
        let url = entry.url.trim_end_matches('/').to_string();
        if let Some(existing) = config
            .llm_servers
            .iter()
            .find(|s| s.kind == kind && s.url == url)
        {
            return existing.id.clone();
        }
        let id = Uuid::new_v4().to_string();
        let order = config
            .llm_servers
            .iter()
            .filter(|s| s.attached)
            .map(|s| s.order)
            .max()
            .map(|o| o + 1)
            .unwrap_or(0);
        config.llm_servers.push(LlmServerEntry {
            id: id.clone(),
            kind,
            url,
            label: entry.label,
            attached: false,
            order,
            source: entry.source,
            api_type: entry.api_type,
            models: entry.models,
            advertise_to_cluster: true,
        });
        id
    }

    pub async fn attach(
        &self,
        config: &mut ClientConfig,
        id: &str,
    ) -> Result<()> {
        let idx = config
            .llm_servers
            .iter()
            .position(|s| s.id == id)
            .ok_or_else(|| anyhow!("server not found"))?;
        let entry = config.llm_servers[idx].clone();
        validate_custom_server_entry(&entry).map_err(|e| anyhow!(e))?;
        let (backend, inference_engine) = self.build_backend(&entry, config).await?;
        backend.health_check().await?;

        if !config.llm_servers[idx].attached {
            let next_order = config
                .llm_servers
                .iter()
                .filter(|s| s.attached)
                .map(|s| s.order)
                .max()
                .map(|o| o + 1)
                .unwrap_or(0);
            config.llm_servers[idx].attached = true;
            config.llm_servers[idx].order = next_order;
        }

        let attached_entry = config.llm_servers[idx].clone();
        {
            let mut inner = self.inner.write().await;
            inner.servers.insert(
                id.to_string(),
                ServerRuntime {
                    entry: attached_entry,
                    backend: Arc::new(backend),
                    show_cache: HashMap::new(),
                    inference_engine,
                },
            );
        }

        Ok(())
    }

    pub async fn detach(&self, config: &mut ClientConfig, id: &str) -> Result<()> {
        let entry = config
            .llm_servers
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| anyhow!("server not found"))?;
        entry.attached = false;

        let mut inner = self.inner.write().await;
        inner.servers.remove(id);
        inner.merged = None;
        inner.model_routes.clear();
        Ok(())
    }

    pub async fn remove_server(&self, config: &mut ClientConfig, id: &str) -> Result<()> {
        config.llm_servers.retain(|s| s.id != id);
        let _ = self.tx_store.delete_server_api_key(id).await;
        let _ = self.tx_store.delete_server_admin_token(id).await;
        let mut inner = self.inner.write().await;
        inner.servers.remove(id);
        inner.merged = None;
        inner.model_routes.clear();
        Ok(())
    }

    pub async fn rebuild_catalog(&self, gpu_probe: GpuProbeMode) -> Result<MergedCatalog> {
        let server_ids: Vec<(String, u32)> = {
            let inner = self.inner.read().await;
            let mut ordered: Vec<_> = inner
                .servers
                .values()
                .map(|s| (s.entry.id.clone(), s.entry.order))
                .collect();
            ordered.sort_by_key(|(_, order)| *order);
            ordered
        };

        let mut pairs = Vec::new();
        for (server_id, _) in server_ids {
            let (backend, entry, mut show_cache) = {
                let inner = self.inner.read().await;
                let runtime = inner
                    .servers
                    .get(&server_id)
                    .ok_or_else(|| anyhow!("server runtime missing"))?;
                (
                    runtime.backend.clone(),
                    runtime.entry.clone(),
                    runtime.show_cache.clone(),
                )
            };

            let snapshot = backend.list_models(&mut show_cache, gpu_probe).await?;

            {
                let mut inner = self.inner.write().await;
                if let Some(runtime) = inner.servers.get_mut(&server_id) {
                    runtime.show_cache = show_cache;
                }
            }

            pairs.push((entry, snapshot));
        }

        let merged = merge_catalog_snapshots(&pairs);
        let mut inner = self.inner.write().await;
        inner.model_routes = merged
            .models
            .iter()
            .filter_map(|m| {
                let name = m.get("name")?.as_str()?;
                let server = m.get("_source_server")?.as_str()?;
                Some((name.to_string(), server.to_string()))
            })
            .collect();
        inner.merged = Some(merged.clone());
        Ok(merged)
    }

    pub async fn merged_catalog(&self) -> Option<MergedCatalog> {
        self.inner.read().await.merged.clone()
    }

    pub async fn resolve_backend(&self, model: &str) -> Option<Arc<LlmBackend>> {
        let inner = self.inner.read().await;
        let server_id = inner.model_routes.get(model)?;
        inner.servers.get(server_id).map(|s| s.backend.clone())
    }

    pub async fn backend_for_server(&self, server_id: &str) -> Option<Arc<LlmBackend>> {
        self.inner
            .read()
            .await
            .servers
            .get(server_id)
            .map(|s| s.backend.clone())
    }

    pub async fn inference_engine_for_server(&self, server_id: &str) -> Option<String> {
        self.inner
            .read()
            .await
            .servers
            .get(server_id)
            .and_then(|s| s.inference_engine.clone())
    }

    pub fn catalog_source_for(kind: &str, inference_engine: Option<&str>, ollama_backend: bool) -> &'static str {
        if !is_inference_cell_kind(kind) {
            return "ollama";
        }
        match inference_engine {
            Some("ollama") => "ollama",
            Some(_) => "huggingface",
            None if ollama_backend => "ollama",
            None => "huggingface",
        }
    }

    pub fn inference_cell_uses_hf_catalog(kind: &str, inference_engine: Option<&str>) -> bool {
        Self::catalog_source_for(kind, inference_engine, false) == "huggingface"
    }

    async fn refresh_stale_inference_engines(&self, config: &ClientConfig) -> Result<()> {
        let stale: Vec<LlmServerEntry> = {
            let inner = self.inner.read().await;
            config
                .llm_servers
                .iter()
                .filter(|entry| {
                    entry.attached
                        && is_inference_cell_kind(&entry.kind)
                        && inner
                            .servers
                            .get(&entry.id)
                            .is_some_and(|runtime| runtime.inference_engine.is_none())
                })
                .cloned()
                .collect()
        };
        if stale.is_empty() {
            return Ok(());
        }

        for entry in stale {
            let (backend, inference_engine) = self.build_backend(&entry, config).await?;
            if inference_engine.is_none() {
                continue;
            }
            let mut inner = self.inner.write().await;
            if let Some(runtime) = inner.servers.get_mut(&entry.id) {
                runtime.backend = Arc::new(backend);
                runtime.inference_engine = inference_engine;
            }
        }
        Ok(())
    }

    pub async fn resolve_ollama_backend_for_model(&self, model: &str) -> Option<Arc<LlmBackend>> {
        if let Some(backend) = self.resolve_backend(model).await {
            if backend.supports_ollama_native() {
                return Some(backend);
            }
        }
        self.first_ollama_backend().await
    }

    pub async fn first_attached_backend(&self) -> Option<Arc<LlmBackend>> {
        let inner = self.inner.read().await;
        let mut ordered: Vec<_> = inner.servers.values().collect();
        ordered.sort_by_key(|s| s.entry.order);
        ordered.first().map(|s| s.backend.clone())
    }

    pub async fn first_ollama_backend(&self) -> Option<Arc<LlmBackend>> {
        let inner = self.inner.read().await;
        let mut ordered: Vec<_> = inner.servers.values().collect();
        ordered.sort_by_key(|s| s.entry.order);
        ordered
            .iter()
            .find(|s| s.backend.supports_ollama_native())
            .map(|s| s.backend.clone())
    }

    pub async fn attached_count(&self) -> usize {
        self.inner.read().await.servers.len()
    }

    pub async fn server_views(&self, config: &ClientConfig) -> Vec<LlmServerView> {
        let _ = self.refresh_stale_inference_engines(config).await;
        let inner = self.inner.read().await;
        let mut views = Vec::new();
        for entry in &config.llm_servers {
            let static_model_count = entry.models.len();
            let model_count = if entry.attached {
                inner
                    .merged
                    .as_ref()
                    .map(|m| {
                        m.models
                            .iter()
                            .filter(|model| {
                                model
                                    .get("_source_server")
                                    .and_then(|v| v.as_str())
                                    == Some(entry.id.as_str())
                            })
                            .count()
                    })
                    .unwrap_or(0)
            } else if static_model_count > 0 {
                static_model_count
            } else {
                0
            };
            let healthy = if entry.attached {
                inner.servers.contains_key(&entry.id)
            } else {
                false
            };
            let has_api_key = self
                .tx_store
                .has_server_api_key(&entry.id)
                .await
                .unwrap_or(false);
            let has_admin_token = self
                .tx_store
                .has_server_admin_token(&entry.id)
                .await
                .unwrap_or(false);
            let runtime = inner.servers.get(&entry.id);
            let inference_engine = runtime.and_then(|r| r.inference_engine.clone());
            let ollama_backend = runtime
                .map(|r| r.backend.supports_ollama_native())
                .unwrap_or(false);
            let catalog_source = Self::catalog_source_for(
                &entry.kind,
                inference_engine.as_deref(),
                ollama_backend,
            )
            .to_string();
            views.push(LlmServerView {
                id: entry.id.clone(),
                kind: entry.kind.clone(),
                url: entry.url.clone(),
                label: entry.label.clone(),
                attached: entry.attached,
                order: entry.order,
                source: entry.source.clone(),
                model_count,
                healthy,
                has_api_key,
                has_admin_token,
                api_type: entry.api_type.clone(),
                models: entry.models.clone(),
                advertise_to_cluster: entry.advertise_to_cluster,
                inference_engine,
                catalog_source,
            });
        }
        views.sort_by_key(|v| v.order);
        views
    }

    pub async fn health_check_attached(&self) -> Result<()> {
        let inner = self.inner.read().await;
        for runtime in inner.servers.values() {
            runtime.backend.health_check().await?;
        }
        Ok(())
    }

    pub async fn synthesize_merged_tags(&self) -> Result<Value> {
        let merged = self
            .merged_catalog()
            .await
            .ok_or_else(|| anyhow!("no catalog"))?;
        Ok(json!({ "models": merged.models }))
    }

    pub async fn synthesize_merged_v1_models(&self) -> Result<Value> {
        let merged = self
            .merged_catalog()
            .await
            .ok_or_else(|| anyhow!("no catalog"))?;
        let data: Vec<Value> = merged
            .models
            .iter()
            .filter_map(|m| {
                let id = m.get("name").and_then(|n| n.as_str())?;
                let mut entry = json!({
                    "id": id,
                    "object": "model",
                    "created": 0,
                    "owned_by": m.get("_source_kind").cloned().unwrap_or(json!("local")),
                });
                if let Some(status) = m.get("_status") {
                    entry["_status"] = status.clone();
                }
                if let Some(kind) = m.get("_source_kind") {
                    entry["_source_kind"] = kind.clone();
                }
                if let Some(url) = m.get("_source_url") {
                    entry["_source_url"] = url.clone();
                }
                Some(entry)
            })
            .collect();
        Ok(json!({ "object": "list", "data": data }))
    }
}

pub fn merge_catalog_snapshots(
    ordered: &[(LlmServerEntry, ModelCatalogSnapshot)],
) -> MergedCatalog {
    let mut models = Vec::new();
    let mut model_names = Vec::new();
    let mut seen_names = HashSet::new();
    let mut collisions = Vec::new();
    let mut gpu_host = None;

    for (entry, snapshot) in ordered {
        if gpu_host.is_none() {
            gpu_host = snapshot.gpu_host.clone();
        }
        for mut model in snapshot.models.clone() {
            let name = model
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            if seen_names.contains(&name) {
                let skipped = entry
                    .label
                    .clone()
                    .unwrap_or_else(|| format!("{} @ {}", entry.kind, entry.url));
                collisions.push(ModelCollision {
                    name: name.clone(),
                    skipped_server: skipped,
                });
                continue;
            }
            seen_names.insert(name.clone());
            if let Some(obj) = model.as_object_mut() {
                obj.insert("_source_server".to_string(), json!(entry.id));
                obj.insert("_source_kind".to_string(), json!(entry.kind));
                obj.insert("_source_url".to_string(), json!(entry.url));
            }
            model_names.push(name);
            models.push(model);
        }
    }

    MergedCatalog {
        models,
        model_names,
        gpu_host,
        collisions,
    }
}

/// Returns models suitable for cluster/swarm advertisement (excludes local-only servers
/// and models that are partially loaded on CPU; includes unloaded and 100% GPU models).
pub fn filter_models_for_cluster_advertisement(
    snapshot: &MergedCatalog,
    config: &ClientConfig,
) -> (Vec<Value>, Vec<String>) {
    use std::collections::HashSet;

    let hidden: HashSet<&str> = config
        .llm_servers
        .iter()
        .filter(|s| !s.advertise_to_cluster)
        .map(|s| s.id.as_str())
        .collect();
    let models: Vec<Value> = snapshot
        .models
        .iter()
        .filter(|m| {
            let server_ok = m
                .get("_source_server")
                .and_then(|v| v.as_str())
                .map(|id| !hidden.contains(id))
                .unwrap_or(true);
            server_ok && crate::ollama_client::model_is_advertisable_for_network(m)
        })
        .cloned()
        .collect();
    let model_names: Vec<String> = models
        .iter()
        .filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    (models, model_names)
}

#[derive(Clone)]
pub struct RegistryHandle {
    pub inner: Arc<LlmServerRegistry>,
}

impl RegistryHandle {
    pub fn new(http_client: Arc<RwLock<Client>>, tx_store: Arc<TxStore>) -> Self {
        Self {
            inner: Arc::new(LlmServerRegistry::new(http_client, tx_store)),
        }
    }

    pub async fn replace_http_client(&self, client: Client) {
        *self.inner.http_client.write().await = client;
    }
}

pub fn migrate_config(config: &mut ClientConfig) {
    if !config.llm_servers.is_empty() {
        return;
    }
    if let Some(url) = config.llm_url.clone() {
        let kind = config
            .llm_backend
            .as_deref()
            .map(normalize_backend_label)
            .unwrap_or_else(|| "ollama".to_string());
        config.llm_servers.push(LlmServerEntry {
            id: Uuid::new_v4().to_string(),
            kind,
            url,
            label: None,
            attached: true,
            order: 0,
            source: "migration".to_string(),
            api_type: None,
            models: Vec::new(),
            advertise_to_cluster: true,
        });
    }
}

pub async fn init_registry_from_config(
    registry: &LlmServerRegistry,
    config: &ClientConfig,
    shared_state: &crate::shared::SharedState,
) {
    if let Err(e) = registry.sync_from_config(config).await {
        eprintln!("⚠️ Failed to sync LLM registry: {}", e);
        return;
    }
    if registry.attached_count().await == 0 {
        return;
    }
    let gpu_probe = gpu_probe_mode();
    match registry.rebuild_catalog(gpu_probe).await {
        Ok(catalog) => {
            let mut st = shared_state.lock().await;
            st.local_models = catalog.model_names.clone();
            st.local_models_full = catalog.models.clone();
            st.last_gpu_host = catalog.gpu_host.clone();
            st.local_model_collisions = catalog.collisions.clone();
        }
        Err(e) => {
            eprintln!("⚠️ Failed to fetch local models: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::LlmServerEntry;

    fn sample_entry(id: &str, kind: &str, order: u32) -> LlmServerEntry {
        LlmServerEntry {
            id: id.to_string(),
            kind: kind.to_string(),
            url: format!("http://127.0.0.1:{}", 11434 + order),
            label: None,
            attached: true,
            order,
            source: "test".to_string(),
            api_type: None,
            models: Vec::new(),
            advertise_to_cluster: true,
        }
    }

    fn snapshot_with_models(names: &[&str]) -> ModelCatalogSnapshot {
        ModelCatalogSnapshot {
            models: names
                .iter()
                .map(|n| json!({ "name": n, "model": n }))
                .collect(),
            model_names: names.iter().map(|s| s.to_string()).collect(),
            gpu_host: None,
        }
    }

    fn gpu_loaded_model(name: &str) -> Value {
        json!({
            "name": name,
            "model": name,
            "_status": { "loaded": true, "processor": "gpu", "gpu_pct": 100 },
        })
    }

    fn merged_with_status_models(models: Vec<Value>) -> MergedCatalog {
        let model_names: Vec<String> = models
            .iter()
            .filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect();
        MergedCatalog {
            models,
            model_names,
            gpu_host: None,
            collisions: Vec::new(),
        }
    }

    #[test]
    fn catalog_source_for_inference_cell_engines() {
        assert_eq!(
            LlmServerRegistry::catalog_source_for("inference-cell", Some("ollama"), false),
            "ollama"
        );
        assert_eq!(
            LlmServerRegistry::catalog_source_for("inference-cell", Some("llamacpp"), false),
            "huggingface"
        );
        assert_eq!(
            LlmServerRegistry::catalog_source_for("inference-cell", None, true),
            "ollama"
        );
        assert_eq!(
            LlmServerRegistry::catalog_source_for("inference-cell", None, false),
            "huggingface"
        );
        assert_eq!(LlmServerRegistry::catalog_source_for("ollama", None, false), "ollama");
    }

    #[test]
    fn inference_cell_uses_hf_catalog() {
        assert!(!LlmServerRegistry::inference_cell_uses_hf_catalog(
            "inference-cell",
            Some("ollama")
        ));
        assert!(LlmServerRegistry::inference_cell_uses_hf_catalog(
            "inference-cell",
            Some("llamacpp")
        ));
        assert!(LlmServerRegistry::inference_cell_uses_hf_catalog("inference-cell", None));
        assert!(!LlmServerRegistry::inference_cell_uses_hf_catalog("ollama", None));
    }

    #[test]
    fn merge_first_wins_on_duplicate_names() {
        let ordered = vec![
            (sample_entry("a", "ollama", 0), snapshot_with_models(&["llama3", "phi3"])),
            (
                sample_entry("b", "vllm", 1),
                snapshot_with_models(&["llama3", "mistral"]),
            ),
        ];
        let merged = merge_catalog_snapshots(&ordered);
        assert_eq!(merged.model_names, vec!["llama3", "phi3", "mistral"]);
        assert_eq!(merged.collisions.len(), 1);
        assert_eq!(merged.collisions[0].name, "llama3");
        assert!(merged.models[0].get("_source_kind").and_then(|v| v.as_str()) == Some("ollama"));
    }

    #[test]
    fn migrate_legacy_single_backend() {
        let mut cfg = ClientConfig {
            llm_backend: Some("vllm".to_string()),
            llm_url: Some("http://127.0.0.1:8000".to_string()),
            ..ClientConfig::default()
        };
        migrate_config(&mut cfg);
        assert_eq!(cfg.llm_servers.len(), 1);
        assert_eq!(cfg.llm_servers[0].kind, "vllm");
        assert!(cfg.llm_servers[0].attached);
    }

    #[test]
    fn filter_excludes_local_only_server_models() {
        let mut local_only = sample_entry("hidden", "ollama", 0);
        local_only.advertise_to_cluster = false;
        let shared = sample_entry("shared", "ollama", 1);
        let ordered = vec![
            (
                local_only,
                ModelCatalogSnapshot {
                    models: vec![gpu_loaded_model("bge-large:335m")],
                    model_names: vec!["bge-large:335m".to_string()],
                    gpu_host: None,
                },
            ),
            (
                shared,
                ModelCatalogSnapshot {
                    models: vec![gpu_loaded_model("llama3")],
                    model_names: vec!["llama3".to_string()],
                    gpu_host: None,
                },
            ),
        ];
        let merged = merge_catalog_snapshots(&ordered);
        let config = ClientConfig {
            llm_servers: ordered.iter().map(|(e, _)| e.clone()).collect(),
            ..ClientConfig::default()
        };
        let (advertised, names) = filter_models_for_cluster_advertisement(&merged, &config);
        assert_eq!(names, vec!["llama3"]);
        assert_eq!(advertised.len(), 1);
        assert_eq!(
            advertised[0].get("name").and_then(|n| n.as_str()),
            Some("llama3")
        );
    }

    #[test]
    fn filter_excludes_mixed_cpu_gpu_keeps_unloaded_and_full_gpu() {
        let merged = merged_with_status_models(vec![
            json!({
                "name": "unloaded",
                "_status": { "loaded": false, "processor": "cpu", "gpu_pct": 0 },
            }),
            json!({
                "name": "mixed",
                "_status": { "loaded": true, "processor": "mixed", "gpu_pct": 40 },
            }),
            json!({
                "name": "cpu-loaded",
                "_status": { "loaded": true, "processor": "cpu", "gpu_pct": 0 },
            }),
            gpu_loaded_model("gpu-ready"),
            json!({
                "name": "remote",
                "_status": { "loaded": true, "processor": "remote" },
            }),
        ]);
        let config = ClientConfig::default();
        let (advertised, names) = filter_models_for_cluster_advertisement(&merged, &config);
        assert_eq!(names, vec!["unloaded", "gpu-ready"]);
        assert_eq!(advertised.len(), 2);
    }
}
