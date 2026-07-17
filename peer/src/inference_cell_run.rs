use crate::client_config::{is_inference_cell_kind, ClientConfig};
use crate::gpu_history::unix_now;
use crate::ollama_client::gpu_probe_mode;
use crate::shared::{LocalModelRunState, SharedState};
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct PullAccepted {
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct PullJobStatus {
    status: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    model_id: Option<String>,
}

async fn upsert_local_run(
    shared_state: &SharedState,
    job_id: &str,
    model: &str,
    status: &str,
    progress_pct: Option<u8>,
    message: Option<String>,
) {
    let now = unix_now();
    let mut state = shared_state.lock().await;
    if let Some(run) = state
        .local_model_runs
        .iter_mut()
        .find(|r| r.job_id == job_id)
    {
        run.status = status.to_string();
        run.progress_pct = progress_pct;
        run.message = message;
        run.updated_at_unix = now;
    } else {
        state.local_model_runs.push(LocalModelRunState {
            job_id: job_id.to_string(),
            model: model.to_string(),
            status: status.to_string(),
            progress_pct,
            message,
            updated_at_unix: now,
        });
    }
}

pub fn spawn_inference_cell_run(
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    job_id: String,
    server_id: String,
    hf_repo: String,
    quant: String,
    display_name: String,
) {
    tokio::spawn(async move {
        upsert_local_run(
            &shared_state,
            &job_id,
            &display_name,
            "queued",
            Some(0),
            Some("Starting Hugging Face download".to_string()),
        )
        .await;

        if let Err(e) = run_inference_cell_task(
            &shared_state,
            &proxy_state,
            &job_id,
            &server_id,
            &hf_repo,
            &quant,
            &display_name,
        )
        .await
        {
            upsert_local_run(
                &shared_state,
                &job_id,
                &display_name,
                "failed",
                None,
                Some(e.to_string()),
            )
            .await;
        }
    });
}

async fn run_inference_cell_task(
    shared_state: &SharedState,
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    job_id: &str,
    server_id: &str,
    hf_repo: &str,
    quant: &str,
    display_name: &str,
) -> Result<()> {
    let (base_url, admin_token) = resolve_inference_cell_server(proxy_state, server_id).await?;
    let client = proxy_state.ollama_http_client.read().await.clone();

    upsert_local_run(
        shared_state,
        job_id,
        display_name,
        "downloading",
        Some(10),
        Some(format!("Pulling {hf_repo} ({quant})")),
    )
    .await;

    let cell_job_id = start_pull(&client, &base_url, admin_token.as_deref(), hf_repo, quant).await?;

    poll_pull_job(
        shared_state,
        &client,
        &base_url,
        admin_token.as_deref(),
        job_id,
        display_name,
        &cell_job_id,
    )
    .await?;

    upsert_local_run(
        shared_state,
        job_id,
        display_name,
        "loading",
        Some(95),
        Some("Loading model into inference cell".to_string()),
    )
    .await;

    let gpu_probe = gpu_probe_mode();
    if let Ok(catalog) = proxy_state
        .llm_registry
        .inner
        .rebuild_catalog(gpu_probe)
        .await
    {
        let mut state = shared_state.lock().await;
        state.local_models = catalog.model_names.clone();
        state.local_models_full = catalog.models.clone();
        state.last_gpu_host = catalog.gpu_host.clone();
        state.local_model_collisions = catalog.collisions.clone();
    }
    proxy_state.bump_cluster_state();

    upsert_local_run(
        shared_state,
        job_id,
        display_name,
        "ready",
        Some(100),
        Some("Model is ready on inference cell".to_string()),
    )
    .await;

    Ok(())
}

async fn resolve_inference_cell_server(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    server_id: &str,
) -> Result<(String, Option<String>)> {
    let cfg = proxy_state.client_config.read().await;
    let entry = cfg
        .llm_servers
        .iter()
        .find(|s| s.id == server_id)
        .ok_or_else(|| anyhow!("server not found"))?;

    if !entry.attached {
        return Err(anyhow!("server is not attached"));
    }
    if !is_inference_cell_kind(&entry.kind) {
        return Err(anyhow!("server is not an inference-cell backend"));
    }

    let admin_token = proxy_state
        .tx_store
        .get_server_admin_token(
            server_id,
            cfg.peer_id.as_deref(),
            cfg.service_id.as_deref(),
        )
        .await?;

    Ok((entry.url.trim_end_matches('/').to_string(), admin_token))
}

async fn start_pull(
    client: &Client,
    base_url: &str,
    admin_token: Option<&str>,
    hf_repo: &str,
    quant: &str,
) -> Result<String> {
    let url = format!("{base_url}/mtrxai/v1/models/pull");
    let mut req = client.post(&url).json(&serde_json::json!({
        "repo": hf_repo,
        "quant": quant,
    }));
    if let Some(token) = admin_token.filter(|t| !t.is_empty()) {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let resp = req.send().await.context("inference-cell pull request")?;
    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(anyhow!(
            "inference-cell rejected pull: admin token required (set Admin token in server settings)"
        ));
    }
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("inference-cell pull failed: {text}"));
    }

    let accepted: PullAccepted = resp.json().await.context("parse pull response")?;
    Ok(accepted.job_id)
}

async fn poll_pull_job(
    shared_state: &SharedState,
    client: &Client,
    base_url: &str,
    admin_token: Option<&str>,
    local_job_id: &str,
    display_name: &str,
    cell_job_id: &str,
) -> Result<()> {
    let url = format!("{base_url}/mtrxai/v1/models/pull/{cell_job_id}");
    loop {
        let mut req = client.get(&url);
        if let Some(token) = admin_token.filter(|t| !t.is_empty()) {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.context("poll inference-cell pull job")?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(anyhow!("inference-cell rejected pull status: admin token required"));
        }
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("inference-cell pull status failed: {text}"));
        }

        let status: PullJobStatus = resp.json().await.context("parse pull job status")?;
        match status.status.as_str() {
            "queued" => {
                upsert_local_run(
                    shared_state,
                    local_job_id,
                    display_name,
                    "downloading",
                    Some(20),
                    Some("Queued on inference cell".to_string()),
                )
                .await;
            }
            "running" => {
                upsert_local_run(
                    shared_state,
                    local_job_id,
                    display_name,
                    "downloading",
                    Some(60),
                    Some("Downloading from Hugging Face".to_string()),
                )
                .await;
            }
            "importing" => {
                upsert_local_run(
                    shared_state,
                    local_job_id,
                    display_name,
                    "importing",
                    Some(80),
                    Some("Importing model into Ollama".to_string()),
                )
                .await;
            }
            "ok" => return Ok(()),
            "failed" => {
                return Err(anyhow!(
                    status
                        .error
                        .unwrap_or_else(|| "inference-cell pull failed".to_string())
                ));
            }
            other => {
                return Err(anyhow!("unexpected inference-cell pull status: {other}"));
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub fn find_inference_cell_server(config: &ClientConfig, server_id: &str) -> Result<()> {
    let entry = config
        .llm_servers
        .iter()
        .find(|s| s.id == server_id)
        .ok_or_else(|| anyhow!("server not found"))?;
    if !entry.attached {
        return Err(anyhow!("server is not attached"));
    }
    if !is_inference_cell_kind(&entry.kind) {
        return Err(anyhow!("server is not an inference-cell backend"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::LlmServerEntry;

    #[test]
    fn find_inference_cell_server_requires_kind_and_attach() {
        let cfg = ClientConfig {
            llm_servers: vec![LlmServerEntry {
                id: "s1".to_string(),
                kind: "inference-cell".to_string(),
                url: "https://127.0.0.1:8443".to_string(),
                label: None,
                attached: false,
                order: 0,
                source: "manual".to_string(),
                api_type: None,
                models: vec![],
                advertise_to_cluster: true,
            }],
            ..Default::default()
        };
        assert!(find_inference_cell_server(&cfg, "s1").is_err());
        assert!(find_inference_cell_server(&cfg, "missing").is_err());
    }
}
