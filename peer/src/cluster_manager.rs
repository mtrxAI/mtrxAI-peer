use crate::client_config::{cluster_accepts_jobs, cluster_is_connected, ClusterMembership};
use crate::network_catalog::sync_unified_network_models;
use crate::network_scheduler::cluster_schedule_fields;
use crate::shared::{
    ClusterStatus, ConnectionAction, ModelStartAction, PeerConnectionView, PeerDirection,
    PeerModerationAction, PeerRegistry, ProxyRequestCommand, SharedState,
};
use crate::webrtc_manager::WebRTCManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

pub struct ClusterManager {
    peer_id: String,
    lobby_host: String,
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    peer_registry: PeerRegistry,
    proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
    model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
    connection_cmd_rx: mpsc::Receiver<ConnectionAction>,
    cluster_notify_rx: mpsc::Receiver<()>,
    peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
    clusters: Vec<ClusterMembership>,
    cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    cluster_cmd_txs: HashMap<String, mpsc::Sender<ProxyRequestCommand>>,
    cluster_model_start_txs: HashMap<String, mpsc::Sender<ModelStartAction>>,
    cluster_connection_txs: HashMap<String, mpsc::Sender<ConnectionAction>>,
    cluster_moderation_txs: HashMap<String, mpsc::Sender<PeerModerationAction>>,
    cluster_handles: HashMap<String, JoinHandle<()>>,
    cluster_connected: Arc<Mutex<HashMap<String, bool>>>,
}

impl ClusterManager {
    pub fn new(
        peer_id: String,
        lobby_host: String,
        clusters: Vec<ClusterMembership>,
        shared_state: SharedState,
        proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
        model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
        connection_cmd_rx: mpsc::Receiver<ConnectionAction>,
        cluster_notify_rx: mpsc::Receiver<()>,
        peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
        proxy_state: Arc<crate::llm_proxy::ProxyState>,
        peer_registry: PeerRegistry,
        cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
        swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    ) -> Self {
        Self {
            peer_id,
            lobby_host,
            shared_state,
            proxy_state,
            peer_registry,
            proxy_cmd_rx,
            model_start_cmd_rx,
            connection_cmd_rx,
            cluster_notify_rx,
            peer_moderation_rx,
            clusters,
            cluster_network_models,
            swarm_network_models,
            cluster_cmd_txs: HashMap::new(),
            cluster_model_start_txs: HashMap::new(),
            cluster_connection_txs: HashMap::new(),
            cluster_moderation_txs: HashMap::new(),
            cluster_handles: HashMap::new(),
            cluster_connected: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.clusters = self.proxy_state.client_config.read().await.clusters.clone();
        self.sync_cluster_status_list().await;
        for cluster in self.clusters.clone() {
            if !cluster_is_connected(&cluster) {
                continue;
            }
            if let Err(e) = self.spawn_cluster(&cluster).await {
                eprintln!("Failed to connect cluster {}: {}", cluster.cluster_id, e);
                self.record_cluster_error(&cluster.cluster_id, &e.to_string())
                    .await;
            }
        }

        let mut reconnect_interval = tokio::time::interval(std::time::Duration::from_secs(15));
        reconnect_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        reconnect_interval.tick().await;

        loop {
            tokio::select! {
                Some(cmd) = self.proxy_cmd_rx.recv() => {
                    self.route_proxy_cmd(cmd).await;
                }
                Some(action) = self.model_start_cmd_rx.recv() => {
                    self.route_model_start_action(action).await;
                }
                Some(action) = self.connection_cmd_rx.recv() => {
                    self.route_connection_action(action).await;
                }
                Some(_) = self.cluster_notify_rx.recv() => {
                    let clusters = self.proxy_state.client_config.read().await.clusters.clone();
                    self.reconcile_clusters(clusters).await;
                }
                Some(action) = self.peer_moderation_rx.recv() => {
                    self.handle_peer_moderation(action).await;
                }
                _ = reconnect_interval.tick() => {
                    self.reap_finished_cluster_tasks().await;
                    let clusters = self.proxy_state.client_config.read().await.clusters.clone();
                    for cluster in clusters {
                        if cluster_is_connected(&cluster)
                            && !self.cluster_handles.contains_key(&cluster.cluster_id)
                        {
                            if let Err(e) = self.spawn_cluster(&cluster).await {
                                eprintln!(
                                    "Failed to reconnect cluster {}: {}",
                                    cluster.cluster_id, e
                                );
                                self.record_cluster_error(&cluster.cluster_id, &e.to_string())
                                    .await;
                            }
                        }
                    }
                    self.sync_cluster_status_list().await;
                }
                else => break,
            }
        }
        Ok(())
    }

    async fn reconcile_clusters(&mut self, desired: Vec<ClusterMembership>) {
        self.reap_finished_cluster_tasks().await;
        let active_ids: Vec<_> = self.cluster_handles.keys().cloned().collect();
        for id in active_ids {
            let should_stop = desired
                .iter()
                .find(|c| c.cluster_id == id)
                .map(|c| !cluster_is_connected(c))
                .unwrap_or(true);
            if should_stop {
                self.stop_cluster(&id).await;
            }
        }
        for cluster in desired {
            if cluster_is_connected(&cluster)
                && !self.cluster_handles.contains_key(&cluster.cluster_id)
            {
                if let Err(e) = self.spawn_cluster(&cluster).await {
                    eprintln!("Failed to spawn cluster {}: {}", cluster.cluster_id, e);
                    self.record_cluster_error(&cluster.cluster_id, &e.to_string())
                        .await;
                }
            }
        }
        self.clusters = self.proxy_state.client_config.read().await.clusters.clone();
        self.sync_cluster_status_list().await;
    }

    async fn reap_finished_cluster_tasks(&mut self) {
        let finished: Vec<String> = self
            .cluster_handles
            .iter()
            .filter(|(_, handle)| handle.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        for id in finished {
            self.cluster_handles.remove(&id);
            eprintln!("⚠️ Lobby WebSocket task ended for cluster {id}; will reconnect");
        }
    }

    async fn stop_cluster(&mut self, cluster_id: &str) {
        if let Some(handle) = self.cluster_handles.remove(cluster_id) {
            handle.abort();
        }
        self.cluster_cmd_txs.remove(cluster_id);
        self.cluster_model_start_txs.remove(cluster_id);
        self.cluster_connection_txs.remove(cluster_id);
        self.cluster_moderation_txs.remove(cluster_id);
        self.cluster_network_models.lock().await.remove(cluster_id);
        self.cluster_connected.lock().await.remove(cluster_id);
        self.peer_registry
            .lock()
            .await
            .retain(|_, p| p.cluster_id.as_deref() != Some(cluster_id));
        sync_unified_network_models(
            &self.shared_state,
            &self.cluster_network_models,
            &self.swarm_network_models,
        )
        .await;
        sync_peer_connections_with_store(
            &self.shared_state,
            &self.peer_registry,
            &self.proxy_state.tx_store,
            &self.proxy_state.peer_stats,
        )
        .await;
        self.sync_cluster_status_list().await;
    }

    async fn spawn_cluster(&mut self, cluster: &ClusterMembership) -> anyhow::Result<()> {
        if self.cluster_handles.contains_key(&cluster.cluster_id) {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel(32);
        let (model_tx, model_rx) = mpsc::channel(32);
        let (conn_tx, conn_rx) = mpsc::channel(32);
        let (mod_tx, mod_rx) = mpsc::channel(32);
        self.cluster_cmd_txs.insert(cluster.cluster_id.clone(), tx);
        self.cluster_model_start_txs
            .insert(cluster.cluster_id.clone(), model_tx);
        self.cluster_connection_txs
            .insert(cluster.cluster_id.clone(), conn_tx);
        self.cluster_moderation_txs
            .insert(cluster.cluster_id.clone(), mod_tx);

        let manager = match WebRTCManager::new(
            self.peer_id.clone(),
            cluster.cluster_id.clone(),
            cluster.name.clone(),
            self.lobby_host.clone(),
            self.shared_state.clone(),
            rx,
            model_rx,
            conn_rx,
            mod_rx,
            self.proxy_state.clone(),
            self.peer_registry.clone(),
            self.cluster_network_models.clone(),
            self.swarm_network_models.clone(),
            self.cluster_connected.clone(),
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                self.cluster_cmd_txs.remove(&cluster.cluster_id);
                self.cluster_model_start_txs.remove(&cluster.cluster_id);
                self.cluster_connection_txs.remove(&cluster.cluster_id);
                self.cluster_moderation_txs.remove(&cluster.cluster_id);
                return Err(e);
            }
        };

        let cluster_id = cluster.cluster_id.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = manager.run().await {
                eprintln!("WebRTCManager error (cluster {cluster_id}): {e}");
            }
        });
        self.cluster_handles
            .insert(cluster.cluster_id.clone(), handle);
        Ok(())
    }

    async fn route_proxy_cmd(&self, cmd: ProxyRequestCommand) {
        if let Some(ref target) = cmd.target_peer {
            if self
                .proxy_state
                .tx_store
                .is_peer_blocked(target)
                .await
                .unwrap_or(false)
            {
                let _ = cmd
                    .response_tx
                    .send(Err(format!("Peer {target} is blocked")))
                    .await;
                return;
            }
        }

        let cluster_id = if let Some(cluster_id) = cmd.cluster_id.clone() {
            cluster_id
        } else {
            match find_cluster_for_model(&cmd.model, &self.cluster_network_models).await {
                Some(id) => id,
                None => {
                    let _ = cmd
                        .response_tx
                        .send(Err(format!("No cluster advertises model {}", cmd.model)))
                        .await;
                    return;
                }
            }
        };

        let Some(tx) = self.cluster_cmd_txs.get(&cluster_id) else {
            let _ = cmd
                .response_tx
                .send(Err(format!("Not connected to cluster {cluster_id}")))
                .await;
            return;
        };

        if tx.send(cmd).await.is_err() {
            eprintln!("Failed to forward proxy command to cluster {cluster_id}");
        }
    }

    async fn handle_peer_moderation(&self, action: PeerModerationAction) {
        match action {
            PeerModerationAction::CloseConnections { peer_id } => {
                for tx in self.cluster_moderation_txs.values() {
                    let _ = tx
                        .send(PeerModerationAction::CloseConnections {
                            peer_id: peer_id.clone(),
                        })
                        .await;
                }
            }
            PeerModerationAction::Report { peer_id, reason } => {
                if let Some(tx) = self.cluster_moderation_txs.values().next() {
                    let _ = tx
                        .send(PeerModerationAction::Report { peer_id, reason })
                        .await;
                }
            }
        }
    }

    async fn route_model_start_action(&self, action: ModelStartAction) {
        let cluster_id = match &action {
            ModelStartAction::Request { cluster_id, .. } => cluster_id.clone(),
            ModelStartAction::Respond { cluster_id, .. } => cluster_id.clone(),
        };
        let Some(cluster_id) = cluster_id else {
            return;
        };

        let Some(tx) = self.cluster_model_start_txs.get(&cluster_id) else {
            eprintln!("Not connected to cluster {cluster_id} for model start");
            return;
        };

        if tx.send(action).await.is_err() {
            eprintln!("Failed to forward model start action to cluster {cluster_id}");
        }
    }

    async fn route_connection_action(&self, action: ConnectionAction) {
        let cluster_id = match &action {
            ConnectionAction::Respond { cluster_id, .. } => cluster_id.clone(),
        };
        let Some(cluster_id) = cluster_id else {
            return;
        };

        let Some(tx) = self.cluster_connection_txs.get(&cluster_id) else {
            eprintln!("Not connected to cluster {cluster_id} for connection respond");
            return;
        };

        if tx.send(action).await.is_err() {
            eprintln!("Failed to forward connection action to cluster {cluster_id}");
        }
    }

    async fn sync_cluster_status_list(&self) {
        let connected = self.cluster_connected.lock().await.clone();
        let registry = self.peer_registry.lock().await;
        let cfg = self.proxy_state.client_config.read().await;
        let (thermal_active, prev_errors) = {
            let state = self.shared_state.lock().await;
            let errors: HashMap<String, Option<String>> = state
                .clusters
                .iter()
                .map(|c| (c.cluster_id.clone(), c.last_error.clone()))
                .collect();
            (state.gpu_thermal_guard.active, errors)
        };
        let clusters_cfg = cfg.clusters.clone();
        let statuses: Vec<ClusterStatus> = clusters_cfg
            .into_iter()
            .map(|c| {
                let outbound = registry
                    .values()
                    .filter(|p| {
                        p.cluster_id.as_deref() == Some(c.cluster_id.as_str())
                            && p.direction == PeerDirection::Outbound
                    })
                    .count();
                let inbound = registry
                    .values()
                    .filter(|p| {
                        p.cluster_id.as_deref() == Some(c.cluster_id.as_str())
                            && p.direction == PeerDirection::Inbound
                    })
                    .count();
                let schedule = cluster_schedule_fields(&cfg, &c);
                let lobby_ok = connected.get(&c.cluster_id).copied().unwrap_or(false);
                ClusterStatus {
                    cluster_id: c.cluster_id.clone(),
                    name: c.name.clone(),
                    visibility: c.visibility.clone(),
                    lobby_connected: lobby_ok,
                    outbound_peers: outbound,
                    inbound_peers: inbound,
                    accepting_jobs: cluster_accepts_jobs(&c),
                    connected: cluster_is_connected(&c),
                    network_models: Vec::new(),
                    schedule_enabled: schedule.schedule_enabled,
                    schedule_start: schedule.schedule_start,
                    schedule_end: schedule.schedule_end,
                    schedule_next_transition_at: schedule.schedule_next_transition_at,
                    schedule_inside_window: schedule.schedule_inside_window,
                    thermal_paused: thermal_active && !cluster_accepts_jobs(&c),
                    last_error: if lobby_ok {
                        None
                    } else {
                        prev_errors.get(&c.cluster_id).cloned().flatten()
                    },
                }
            })
            .collect();
        let mut state = self.shared_state.lock().await;
        state.clusters = statuses;
        drop(state);
        self.attach_cluster_models().await;
    }

    async fn record_cluster_error(&self, cluster_id: &str, error: &str) {
        let mut state = self.shared_state.lock().await;
        if let Some(cluster) = state
            .clusters
            .iter_mut()
            .find(|c| c.cluster_id == cluster_id)
        {
            cluster.last_error = Some(error.to_string());
        }
    }

    async fn attach_cluster_models(&self) {
        let models_by_cluster = self.cluster_network_models.lock().await.clone();
        let mut state = self.shared_state.lock().await;
        for cluster in &mut state.clusters {
            cluster.network_models = models_by_cluster
                .get(&cluster.cluster_id)
                .cloned()
                .unwrap_or_default();
        }
    }
}

async fn find_cluster_for_model(
    model: &str,
    cluster_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
) -> Option<String> {
    let lock = cluster_network_models.lock().await;
    let mut best: Option<(String, u64)> = None;
    for (cluster_id, models) in lock.iter() {
        for m in models {
            if m.get("name").and_then(|n| n.as_str()) != Some(model) {
                continue;
            }
            let count = m.get("_peer_count").and_then(|v| v.as_u64()).unwrap_or(1);
            match &best {
                Some((_, best_count)) if *best_count >= count => {}
                _ => best = Some((cluster_id.clone(), count)),
            }
        }
    }
    best.map(|(id, _)| id)
}

pub async fn sync_peer_connections_with_store(
    shared_state: &SharedState,
    peer_registry: &PeerRegistry,
    tx_store: &crate::tx_db::TxStore,
    peer_stats: &crate::peer_stats::PeerStatsTrackerHandle,
) {
    let blocked: std::collections::HashSet<String> = tx_store
        .list_blocked_peers()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|b| b.peer_id)
        .collect();
    let lifetime_totals = tx_store
        .token_totals_by_counterparty()
        .await
        .unwrap_or_default();
    sync_peer_connections(
        shared_state,
        peer_registry,
        &blocked,
        peer_stats,
        &lifetime_totals,
    )
    .await;
}

pub async fn sync_peer_connections(
    shared_state: &SharedState,
    peer_registry: &PeerRegistry,
    blocked_peers: &std::collections::HashSet<String>,
    peer_stats: &crate::peer_stats::PeerStatsTrackerHandle,
    lifetime_totals: &std::collections::HashMap<String, u64>,
) {
    let peer_rows: Vec<(
        String,
        Option<String>,
        Option<String>,
        String,
        u64,
        bool,
        u64,
    )> = peer_registry
        .lock()
        .await
        .values()
        .map(|p| {
            (
                p.peer_id.clone(),
                p.cluster_id.clone(),
                p.swarm_id.clone(),
                match p.direction {
                    PeerDirection::Inbound => "inbound".to_string(),
                    PeerDirection::Outbound => "outbound".to_string(),
                },
                p.connected_at.elapsed().as_secs(),
                p.data_channel_open,
                p.attestation_flags,
            )
        })
        .collect();

    let views: Vec<PeerConnectionView> = {
        let stats_guard = peer_stats.lock().ok();
        peer_rows
            .into_iter()
            .map(
                |(
                    peer_id,
                    cluster_id,
                    swarm_id,
                    direction,
                    since_secs,
                    data_channel_open,
                    attestation_flags,
                )| {
                    let lifetime = lifetime_totals.get(&peer_id).copied().unwrap_or(0);
                    let stats = stats_guard.as_ref().map(|g| g.snapshot(&peer_id, lifetime));
                    let blocked = blocked_peers.contains(&peer_id);
                    PeerConnectionView {
                        peer_id,
                        cluster_id,
                        swarm_id,
                        direction,
                        since_secs,
                        data_channel_open,
                        blocked,
                        attestation_flags,
                        stats,
                    }
                },
            )
            .collect()
    };
    shared_state.lock().await.peer_connections = views;
}

pub fn cluster_membership_from_response(
    resp: &crate::client_config::ClusterResponse,
    room_secret: Option<String>,
) -> ClusterMembership {
    crate::client_config::membership_from_response(resp, room_secret)
}
