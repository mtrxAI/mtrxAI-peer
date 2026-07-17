use crate::client_config::ClientConfig;
use crate::llm_proxy::ProxyState;
use crate::ollama_client::gpu_probe_mode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

pub fn spawn_llm_monitor(proxy_state: ProxyState, _client_config: Arc<RwLock<ClientConfig>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;

        loop {
            interval.tick().await;

            if proxy_state.llm_registry.inner.attached_count().await == 0 {
                proxy_state.llm_ready.store(false, Ordering::Relaxed);
                continue;
            }

            match proxy_state
                .llm_registry
                .inner
                .health_check_attached()
                .await
            {
                Ok(()) => {
                    let was_ready = proxy_state.llm_ready.load(Ordering::Relaxed);
                    proxy_state.llm_ready.store(true, Ordering::Relaxed);
                    if !was_ready {
                        match proxy_state
                            .llm_registry
                            .inner
                            .rebuild_catalog(gpu_probe_mode())
                            .await
                        {
                            Ok(catalog) => {
                                let mut st = proxy_state.shared_state.lock().await;
                                st.local_models = catalog.model_names.clone();
                                st.local_models_full = catalog.models.clone();
                                st.last_gpu_host = catalog.gpu_host.clone();
                                st.local_model_collisions = catalog.collisions.clone();
                                drop(st);
                                let _ = proxy_state.cluster_notify_tx.send(()).await;
                                let _ = proxy_state.swarm_notify_tx.send(()).await;
                                println!("✅ LLM server(s) reconnected");
                            }
                            Err(e) => {
                                eprintln!("⚠️ Failed to rebuild LLM catalog after reconnect: {e}");
                            }
                        }
                    }
                }
                Err(_) => {
                    proxy_state.llm_ready.store(false, Ordering::Relaxed);
                }
            }
        }
    });
}
