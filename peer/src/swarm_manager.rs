use crate::client_config::{
    resolve_swarm_bootnodes, swarm_accepts_jobs, swarm_is_connected, ClientConfig, SwarmMembership,
};
use crate::llm_proxy::ProxyState;
use crate::network_catalog::sync_unified_network_models;
use crate::network_scheduler::swarm_schedule_fields;
use crate::p2p_manager::P2pManager;
use crate::shared::{
    ModelStartAction, PeerConnectionView, PeerDirection, PeerModerationAction, PeerRegistry,
    ProxyRequestCommand, SharedState, SwarmStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

pub struct SwarmManager {
    peer_id: String,
    lobby_host: String,
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    peer_registry: PeerRegistry,
    proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
    model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
    swarm_notify_rx: mpsc::Receiver<()>,
    peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
    swarms: Vec<SwarmMembership>,
    swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_cmd_txs: HashMap<String, mpsc::Sender<ProxyRequestCommand>>,
    swarm_model_start_txs: HashMap<String, mpsc::Sender<ModelStartAction>>,
    swarm_moderation_txs: HashMap<String, mpsc::Sender<PeerModerationAction>>,
    swarm_handles: HashMap<String, JoinHandle<()>>,
    swarm_connected: Arc<Mutex<HashMap<String, bool>>>,
}

impl SwarmManager {
    pub fn new(
        peer_id: String,
        lobby_host: String,
        swarms: Vec<SwarmMembership>,
        shared_state: SharedState,
        proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
        model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
        swarm_notify_rx: mpsc::Receiver<()>,
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
            swarm_notify_rx,
            peer_moderation_rx,
            swarms,
            swarm_network_models,
            cluster_network_models,
            swarm_cmd_txs: HashMap::new(),
            swarm_model_start_txs: HashMap::new(),
            swarm_moderation_txs: HashMap::new(),
            swarm_handles: HashMap::new(),
            swarm_connected: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.swarms = self.proxy_state.client_config.read().await.swarms.clone();
        self.sync_swarm_status_list().await;
        for swarm in self.swarms.clone() {
            if !swarm_is_connected(&swarm) {
                continue;
            }
            if let Err(e) = self.spawn_swarm(&swarm).await {
                eprintln!("Failed to connect swarm {}: {}", swarm.swarm_id, e);
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
                Some(_) = self.swarm_notify_rx.recv() => {
                    let swarms = self.proxy_state.client_config.read().await.swarms.clone();
                    self.reconcile_swarms(swarms).await;
                }
                Some(action) = self.peer_moderation_rx.recv() => {
                    self.handle_peer_moderation(action).await;
                }
                _ = reconnect_interval.tick() => {
                    self.reap_finished_swarm_tasks().await;
                    let swarms = self.proxy_state.client_config.read().await.swarms.clone();
                    for swarm in swarms {
                        if swarm_is_connected(&swarm)
                            && !self.swarm_handles.contains_key(&swarm.swarm_id)
                        {
                            if let Err(e) = self.spawn_swarm(&swarm).await {
                                eprintln!(
                                    "Failed to reconnect swarm {}: {}",
                                    swarm.swarm_id, e
                                );
                            }
                        }
                    }
                    self.sync_swarm_status_list().await;
                }
                else => break,
            }
        }
        Ok(())
    }

    async fn reconcile_swarms(&mut self, desired: Vec<SwarmMembership>) {
        self.reap_finished_swarm_tasks().await;
        let active_ids: Vec<_> = self.swarm_handles.keys().cloned().collect();
        for id in active_ids {
            let should_stop = desired
                .iter()
                .find(|s| s.swarm_id == id)
                .map(|s| !swarm_is_connected(s))
                .unwrap_or(true);
            if should_stop {
                self.stop_swarm(&id).await;
            }
        }
        for swarm in desired {
            if swarm_is_connected(&swarm) && !self.swarm_handles.contains_key(&swarm.swarm_id) {
                if let Err(e) = self.spawn_swarm(&swarm).await {
                    eprintln!("Failed to spawn swarm {}: {}", swarm.swarm_id, e);
                }
            }
        }
        self.swarms = self.proxy_state.client_config.read().await.swarms.clone();
        self.sync_swarm_status_list().await;
    }

    async fn reap_finished_swarm_tasks(&mut self) {
        let finished: Vec<String> = self
            .swarm_handles
            .iter()
            .filter(|(_, handle)| handle.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        let had_finished = !finished.is_empty();
        for id in finished {
            self.swarm_handles.remove(&id);
            self.swarm_connected.lock().await.remove(&id);
            eprintln!("⚠️ P2P task ended for swarm {id}; will reconnect");
        }
        if had_finished {
            self.sync_swarm_status_list().await;
        }
    }

    async fn stop_swarm(&mut self, swarm_id: &str) {
        if let Some(handle) = self.swarm_handles.remove(swarm_id) {
            handle.abort();
        }
        self.swarm_cmd_txs.remove(swarm_id);
        self.swarm_model_start_txs.remove(swarm_id);
        self.swarm_moderation_txs.remove(swarm_id);
        self.swarm_network_models.lock().await.remove(swarm_id);
        self.swarm_connected.lock().await.remove(swarm_id);
        self.peer_registry
            .lock()
            .await
            .retain(|_, p| p.swarm_id.as_deref() != Some(swarm_id));
        sync_unified_network_models(
            &self.shared_state,
            &self.cluster_network_models,
            &self.swarm_network_models,
        )
        .await;
        sync_swarm_peer_connections(
            &self.shared_state,
            &self.peer_registry,
            &self.proxy_state.tx_store,
            &self.proxy_state.peer_stats,
        )
        .await;
        self.sync_swarm_status_list().await;
    }

    async fn spawn_swarm(&mut self, swarm: &SwarmMembership) -> anyhow::Result<()> {
        if self.swarm_handles.contains_key(&swarm.swarm_id) {
            return Ok(());
        }

        let bootnodes =
            resolve_swarm_bootnodes(&self.proxy_state.http_client, &self.lobby_host, swarm).await;

        let (tx, rx) = mpsc::channel(32);
        let (model_tx, model_rx) = mpsc::channel(32);
        let (mod_tx, mod_rx) = mpsc::channel(32);
        self.swarm_cmd_txs.insert(swarm.swarm_id.clone(), tx);
        self.swarm_model_start_txs
            .insert(swarm.swarm_id.clone(), model_tx);
        self.swarm_moderation_txs
            .insert(swarm.swarm_id.clone(), mod_tx);

        let manager = P2pManager::new(
            self.peer_id.clone(),
            swarm.clone(),
            bootnodes,
            self.shared_state.clone(),
            rx,
            model_rx,
            mod_rx,
            self.proxy_state.clone(),
            self.peer_registry.clone(),
            self.swarm_network_models.clone(),
            self.cluster_network_models.clone(),
            self.swarm_connected.clone(),
        );

        let swarm_id = swarm.swarm_id.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = manager.run().await {
                eprintln!("P2pManager error (swarm {swarm_id}): {e}");
            }
        });
        self.swarm_handles.insert(swarm.swarm_id.clone(), handle);
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

        let swarm_id = if let Some(swarm_id) = cmd.swarm_id.clone() {
            swarm_id
        } else {
            match find_swarm_for_model(&cmd.model, &self.swarm_network_models).await {
                Some(id) => id,
                None => {
                    let _ = cmd
                        .response_tx
                        .send(Err(format!("No swarm advertises model {}", cmd.model)))
                        .await;
                    return;
                }
            }
        };

        let Some(tx) = self.swarm_cmd_txs.get(&swarm_id) else {
            let _ = cmd
                .response_tx
                .send(Err(format!("Not connected to swarm {swarm_id}")))
                .await;
            return;
        };

        if tx.send(cmd).await.is_err() {
            eprintln!("Failed to forward proxy command to swarm {swarm_id}");
        }
    }

    async fn handle_peer_moderation(&self, action: PeerModerationAction) {
        match action {
            PeerModerationAction::CloseConnections { peer_id } => {
                for tx in self.swarm_moderation_txs.values() {
                    let _ = tx
                        .send(PeerModerationAction::CloseConnections {
                            peer_id: peer_id.clone(),
                        })
                        .await;
                }
            }
            PeerModerationAction::Report { peer_id, reason } => {
                if let Some(tx) = self.swarm_moderation_txs.values().next() {
                    let _ = tx
                        .send(PeerModerationAction::Report { peer_id, reason })
                        .await;
                }
            }
        }
    }

    async fn route_model_start_action(&self, action: ModelStartAction) {
        let swarm_id = match &action {
            ModelStartAction::Request { swarm_id, .. } => swarm_id.clone(),
            ModelStartAction::Respond { swarm_id, .. } => swarm_id.clone(),
        };
        let Some(swarm_id) = swarm_id else {
            eprintln!("Model start action missing swarm_id");
            return;
        };

        let Some(tx) = self.swarm_model_start_txs.get(&swarm_id) else {
            eprintln!("Not connected to swarm {swarm_id} for model start");
            return;
        };

        if tx.send(action).await.is_err() {
            eprintln!("Failed to forward model start action to swarm {swarm_id}");
        }
    }

    async fn sync_swarm_status_list(&self) {
        let connected = self.swarm_connected.lock().await.clone();
        let registry = self.peer_registry.lock().await;
        let cfg = self.proxy_state.client_config.read().await;
        let thermal_active = self.shared_state.lock().await.gpu_thermal_guard.active;
        let swarms_cfg = cfg.swarms.clone();
        let statuses: Vec<SwarmStatus> = swarms_cfg
            .into_iter()
            .map(|s| {
                let outbound = registry
                    .values()
                    .filter(|p| {
                        p.swarm_id.as_deref() == Some(s.swarm_id.as_str())
                            && p.direction == PeerDirection::Outbound
                    })
                    .count();
                let inbound = registry
                    .values()
                    .filter(|p| {
                        p.swarm_id.as_deref() == Some(s.swarm_id.as_str())
                            && p.direction == PeerDirection::Inbound
                    })
                    .count();
                let schedule = swarm_schedule_fields(&cfg, &s);
                SwarmStatus {
                    swarm_id: s.swarm_id.clone(),
                    name: s.name.clone(),
                    p2p_connected: connected.get(&s.swarm_id).copied().unwrap_or(false),
                    outbound_peers: outbound,
                    inbound_peers: inbound,
                    accepting_jobs: swarm_accepts_jobs(&s),
                    connected: swarm_is_connected(&s),
                    network_models: Vec::new(),
                    schedule_enabled: schedule.schedule_enabled,
                    schedule_start: schedule.schedule_start,
                    schedule_end: schedule.schedule_end,
                    schedule_next_transition_at: schedule.schedule_next_transition_at,
                    schedule_inside_window: schedule.schedule_inside_window,
                    thermal_paused: thermal_active && !swarm_accepts_jobs(&s),
                }
            })
            .collect();
        let mut state = self.shared_state.lock().await;
        state.swarms = statuses;
        drop(state);
        self.attach_swarm_models().await;
    }

    async fn attach_swarm_models(&self) {
        let models_by_swarm = self.swarm_network_models.lock().await.clone();
        let mut state = self.shared_state.lock().await;
        for swarm in &mut state.swarms {
            swarm.network_models = models_by_swarm
                .get(&swarm.swarm_id)
                .cloned()
                .unwrap_or_default();
        }
    }
}

pub fn build_swarm_statuses(
    cfg: &ClientConfig,
    swarms_cfg: &[SwarmMembership],
    live: &[SwarmStatus],
) -> Vec<SwarmStatus> {
    swarms_cfg
        .iter()
        .map(|s| {
            live.iter()
                .find(|l| l.swarm_id == s.swarm_id)
                .cloned()
                .unwrap_or_else(|| {
                    let schedule = swarm_schedule_fields(cfg, s);
                    SwarmStatus {
                        swarm_id: s.swarm_id.clone(),
                        name: s.name.clone(),
                        p2p_connected: false,
                        outbound_peers: 0,
                        inbound_peers: 0,
                        accepting_jobs: swarm_accepts_jobs(s),
                        connected: swarm_is_connected(s),
                        network_models: Vec::new(),
                        schedule_enabled: schedule.schedule_enabled,
                        schedule_start: schedule.schedule_start,
                        schedule_end: schedule.schedule_end,
                        schedule_next_transition_at: schedule.schedule_next_transition_at,
                        schedule_inside_window: schedule.schedule_inside_window,
                        thermal_paused: false,
                    }
                })
        })
        .collect()
}

pub async fn sync_swarm_status_for_proxy(proxy_state: &ProxyState) {
    let cfg = proxy_state.client_config.read().await;
    let swarms_cfg = cfg.swarms.clone();
    drop(cfg);
    let live = proxy_state.shared_state.lock().await.swarms.clone();
    let cfg = proxy_state.client_config.read().await;
    proxy_state.shared_state.lock().await.swarms = build_swarm_statuses(&cfg, &swarms_cfg, &live);
}

async fn find_swarm_for_model(
    model: &str,
    swarm_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
) -> Option<String> {
    let lock = swarm_network_models.lock().await;
    let mut best: Option<(String, u64)> = None;
    for (swarm_id, models) in lock.iter() {
        for m in models {
            if m.get("name").and_then(|n| n.as_str()) != Some(model) {
                continue;
            }
            let count = m.get("_peer_count").and_then(|v| v.as_u64()).unwrap_or(1);
            match &best {
                Some((_, best_count)) if *best_count >= count => {}
                _ => best = Some((swarm_id.clone(), count)),
            }
        }
    }
    best.map(|(id, _)| id)
}

pub async fn sync_swarm_peer_connections(
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
                    let blocked = blocked.contains(&peer_id);
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
