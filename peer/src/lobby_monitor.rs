use crate::client_config::{probe_lobby_reachable, ClientConfig};
use crate::shared::SharedState;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn spawn_lobby_monitor(
    shared_state: SharedState,
    http_client: reqwest::Client,
    client_config: Arc<RwLock<ClientConfig>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;

        loop {
            interval.tick().await;
            let lobby_host = client_config.read().await.lobby_host.clone();
            let reachable = probe_lobby_reachable(&http_client, &lobby_host).await;
            shared_state.lock().await.lobby_connected = reachable;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{AppState, ModelStartAction, ProxyRequestCommand};
    use std::sync::Arc;
    use tokio::sync::{mpsc, Mutex};

    #[tokio::test]
    async fn cluster_sync_does_not_clear_lobby_connected() {
        let (proxy_tx, _proxy_rx) = mpsc::channel::<ProxyRequestCommand>(1);
        let (model_tx, _model_rx) = mpsc::channel::<ModelStartAction>(1);
        let shared_state: SharedState = Arc::new(Mutex::new(AppState {
            my_id: String::new(),
            peer_id: String::new(),
            service_id: String::new(),
            clusters: Vec::new(),
            swarms: Vec::new(),
            credit_balance: 0,
            swarm_proxy_request_tx: proxy_tx.clone(),
            swarm_model_start_tx: model_tx.clone(),
            local_models: Vec::new(),
            local_models_full: Vec::new(),
            network_models: Vec::new(),
            proxy_request_tx: proxy_tx,
            model_start_tx: model_tx,
            connection_tx: mpsc::channel(1).0,
            outgoing_model_requests: Vec::new(),
            incoming_model_offers: Vec::new(),
            incoming_connection_offers: Vec::new(),
            local_model_runs: Vec::new(),
            local_model_collisions: Vec::new(),
            peer_info: None,
            last_advertised_hash: None,
            show_info_cache: Default::default(),
            last_gpu_host: None,
            gpu_history: Default::default(),
            lobby_connected: true,
            peer_connections: Vec::new(),
            gpu_thermal_guard: Default::default(),
        }));

        {
            let mut state = shared_state.lock().await;
            state.clusters = Vec::new();
        }

        assert!(shared_state.lock().await.lobby_connected);
    }
}
