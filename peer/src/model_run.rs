use crate::gpu_history::unix_now;
use crate::ollama_client::{gpu_probe_mode, pull_model_with_progress, warmup_model, fetch_show_info, is_embed_only};
use crate::shared::{LocalModelRunState, SharedState};
use std::sync::Arc;

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

pub fn spawn_local_model_run(
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    job_id: String,
    model: String,
) {
    spawn_local_model_run_on_server(shared_state, proxy_state, job_id, model, None);
}

pub fn spawn_local_model_run_on_server(
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    job_id: String,
    model: String,
    server_id: Option<String>,
) {
    tokio::spawn(async move {
        upsert_local_run(
            &shared_state,
            &job_id,
            &model,
            "downloading",
            Some(0),
            Some("Starting local download".to_string()),
        )
        .await;

        if let Err(e) =
            run_local_model_task(&shared_state, &proxy_state, &job_id, &model, server_id.as_deref())
                .await
        {
            upsert_local_run(
                &shared_state,
                &job_id,
                &model,
                "failed",
                None,
                Some(e.to_string()),
            )
            .await;
        }
    });
}

async fn run_local_model_task(
    shared_state: &SharedState,
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    job_id: &str,
    model: &str,
    server_id: Option<&str>,
) -> anyhow::Result<()> {
    let backend = if let Some(server_id) = server_id {
        proxy_state
            .llm_registry
            .inner
            .backend_for_server(server_id)
            .await
            .filter(|b| b.supports_ollama_native())
            .ok_or_else(|| anyhow::anyhow!("No Ollama backend for server {server_id}"))?
    } else {
        proxy_state
            .llm_registry
            .inner
            .resolve_ollama_backend_for_model(model)
            .await
            .or(proxy_state.llm_registry.inner.first_ollama_backend().await)
            .ok_or_else(|| anyhow::anyhow!("No Ollama server attached"))?
    };
    let ollama_url = backend.base_url().to_string();
    let client = proxy_state.ollama_http_client.read().await.clone();
    let gpu_probe = gpu_probe_mode();

    let shared_progress = shared_state.clone();
    let job_id_progress = job_id.to_string();
    let model_progress = model.to_string();
    pull_model_with_progress(&client, &ollama_url, model, move |pct| {
        let shared = shared_progress.clone();
        let jid = job_id_progress.clone();
        let m = model_progress.clone();
        tokio::spawn(async move {
            upsert_local_run(&shared, &jid, &m, "downloading", Some(pct), None).await;
        });
    })
    .await?;

    upsert_local_run(
        shared_state,
        job_id,
        model,
        "warming",
        Some(100),
        Some("Download complete; warming model".to_string()),
    )
    .await;

    let show = fetch_show_info(&client, &ollama_url, model).await.ok();
    if is_embed_only(show.as_ref()) {
        warmup_model(&client, &ollama_url, model, show.as_ref()).await?;
        upsert_local_run(
            shared_state,
            job_id,
            model,
            "ready",
            Some(100),
            Some("Embedding model ready (embed warmup)".to_string()),
        )
        .await;
    } else {
        warmup_model(&client, &ollama_url, model, show.as_ref()).await?;
        upsert_local_run(
            shared_state,
            job_id,
            model,
            "ready",
            Some(100),
            Some("Model is ready locally".to_string()),
        )
        .await;
    }

    let mut show_cache = shared_state.lock().await.show_info_cache.clone();
    if let Ok(snapshot) = backend.list_models(&mut show_cache, gpu_probe).await {
        let mut state = shared_state.lock().await;
        state.local_models = snapshot.model_names.clone();
        state.local_models_full = snapshot.models.clone();
        state.show_info_cache = show_cache;
        state.last_gpu_host = snapshot.gpu_host.clone();
    }
    proxy_state.bump_cluster_state();

    Ok(())
}
