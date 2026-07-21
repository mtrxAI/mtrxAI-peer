pub mod agent_compat;
pub mod api;
pub mod attestation;
pub mod client_config;
pub mod cluster_dc_e2ee;
pub mod cluster_manager;
pub mod connect_allowance;
pub mod crypto;
pub mod gpu;
pub mod gpu_history;
pub mod gpu_thermal_guard;
pub mod hf_catalog;
pub mod inference_cell_run;
pub mod inference_ipc;
pub mod inference_sidecar;
pub mod llm_backend;
pub mod llm_discovery;
pub mod llm_monitor;
pub mod llm_proxy;
pub mod llm_registry;
pub mod lobby_monitor;
pub mod lobby_url;
pub mod model_run;
pub mod network_actions;
pub mod network_catalog;
pub mod network_scheduler;
pub mod ollama_client;
pub mod p2p_manager;
pub mod p2p_protocol;
pub mod p2p_proxy;
pub mod peer_discovery;
pub mod peer_ranking;
pub mod peer_stats;
pub mod proxy_e2ee;
pub mod security;
pub mod shared;
pub mod swarm_manager;
pub mod token_usage;
pub mod tx_db;
pub mod webrtc_manager;

use client_config::{
    apply_cli_overrides, bootstrap_docker_peer, has_attached_llm_servers,
    invalidate_local_peer_identity, load_client_config, save_client_config,
    sync_peer_device_key_with_lobby,
};
use cluster_manager::ClusterManager;
use gpu_history::spawn_gpu_monitor;
use llm_monitor::spawn_llm_monitor;
use llm_registry::init_registry_from_config;
use lobby_monitor::spawn_lobby_monitor;
use network_scheduler::spawn_network_scheduler;
use ollama_client::{gpu_probe_mode, model_poll_interval_secs};
use peer_discovery::discover_peer_location;
use shared::{AppState, NetworkMode, RuntimeEvent, SharedState};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use swarm_manager::SwarmManager;
use tokio::sync::{mpsc, Mutex, RwLock};
use tx_db::{spawn_transaction_sync, TxStore};

async fn spawn_swarm_manager(
    slot: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    proxy_cmd_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::ProxyRequestCommand>>>>,
    model_start_cmd_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::ModelStartAction>>>>,
    swarm_notify_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    peer_moderation_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::PeerModerationAction>>>>,
    proxy_state: &Arc<llm_proxy::ProxyState>,
    shared_state: &SharedState,
    peer_registry: &shared::PeerRegistry,
    cluster_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
) {
    if slot.lock().await.is_some() {
        return;
    }
    let cfg = proxy_state.client_config.read().await.clone();
    let peer_id = match cfg.peer_id {
        Some(id) if !id.is_empty() => id,
        _ => return,
    };
    if !cfg.setup_complete {
        return;
    }
    if !matches!(cfg.p2p_mode, NetworkMode::Swarm | NetworkMode::Both) {
        return;
    }
    let Some(proxy_rx) = proxy_cmd_rx_holder.lock().await.take() else {
        return;
    };
    let Some(model_start_rx) = model_start_cmd_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        return;
    };
    let Some(notify_rx) = swarm_notify_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        model_start_cmd_rx_holder
            .lock()
            .await
            .replace(model_start_rx);
        return;
    };
    let Some(moderation_rx) = peer_moderation_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        model_start_cmd_rx_holder
            .lock()
            .await
            .replace(model_start_rx);
        swarm_notify_rx_holder.lock().await.replace(notify_rx);
        return;
    };

    let manager = SwarmManager::new(
        peer_id,
        cfg.lobby_host.clone(),
        cfg.swarms.clone(),
        shared_state.clone(),
        proxy_rx,
        model_start_rx,
        notify_rx,
        moderation_rx,
        proxy_state.clone(),
        peer_registry.clone(),
        cluster_network_models.clone(),
        swarm_network_models.clone(),
    );
    *slot.lock().await = Some(tokio::spawn(async move {
        if let Err(e) = manager.run().await {
            eprintln!("SwarmManager error: {}", e);
        }
    }));
}

async fn spawn_cluster_manager(
    slot: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    proxy_cmd_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::ProxyRequestCommand>>>>,
    model_start_cmd_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::ModelStartAction>>>>,
    connection_cmd_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::ConnectionAction>>>>,
    cluster_notify_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    peer_moderation_rx_holder: &Arc<Mutex<Option<mpsc::Receiver<shared::PeerModerationAction>>>>,
    proxy_state: &Arc<llm_proxy::ProxyState>,
    shared_state: &SharedState,
    peer_registry: &shared::PeerRegistry,
    cluster_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_network_models: &Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
) {
    if slot.lock().await.is_some() {
        return;
    }
    let cfg = proxy_state.client_config.read().await.clone();
    let peer_id = match cfg.peer_id {
        Some(id) if !id.is_empty() => id,
        _ => return,
    };
    if !cfg.setup_complete {
        return;
    }
    if !matches!(cfg.p2p_mode, NetworkMode::Cluster | NetworkMode::Both) {
        return;
    }
    let Some(proxy_rx) = proxy_cmd_rx_holder.lock().await.take() else {
        return;
    };
    let Some(model_start_rx) = model_start_cmd_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        return;
    };
    let Some(connection_rx) = connection_cmd_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        model_start_cmd_rx_holder
            .lock()
            .await
            .replace(model_start_rx);
        return;
    };
    let Some(notify_rx) = cluster_notify_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        model_start_cmd_rx_holder
            .lock()
            .await
            .replace(model_start_rx);
        connection_cmd_rx_holder.lock().await.replace(connection_rx);
        return;
    };
    let Some(moderation_rx) = peer_moderation_rx_holder.lock().await.take() else {
        proxy_cmd_rx_holder.lock().await.replace(proxy_rx);
        model_start_cmd_rx_holder
            .lock()
            .await
            .replace(model_start_rx);
        connection_cmd_rx_holder.lock().await.replace(connection_rx);
        cluster_notify_rx_holder.lock().await.replace(notify_rx);
        return;
    };

    let manager = ClusterManager::new(
        peer_id,
        cfg.lobby_host.clone(),
        cfg.clusters.clone(),
        shared_state.clone(),
        proxy_rx,
        model_start_rx,
        connection_rx,
        notify_rx,
        moderation_rx,
        proxy_state.clone(),
        peer_registry.clone(),
        cluster_network_models.clone(),
        swarm_network_models.clone(),
    );
    *slot.lock().await = Some(tokio::spawn(async move {
        if let Err(e) = manager.run().await {
            eprintln!("ClusterManager error: {}", e);
        }
    }));
}

pub async fn run() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════╗");
    println!("║       mtrxAI Client - Startup          ║");
    println!("╚══════════════════════════════════════╝\n");

    println!(" Model poll interval: {}s", model_poll_interval_secs());
    println!(" GPU probe mode: {:?}", gpu_probe_mode());

    let args: Vec<String> = std::env::args().collect();
    let proxy_port = std::env::var("MTRXAI_PROXY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| args.get(1).and_then(|s| s.parse().ok()))
        .unwrap_or(11345);
    let lobby_server_host = std::env::var("MTRXAI_LOBBY_HOST")
        .ok()
        .or_else(|| args.get(2).cloned())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let llm_server_host = std::env::var("MTRXAI_OLLAMA_HOST")
        .ok()
        .or_else(|| args.get(3).cloned());

    println!(" mtrxAI client docker-v2");
    println!(
        " Proxy port: {} | lobby: {} | ollama: {}",
        proxy_port,
        lobby_server_host,
        llm_server_host.as_deref().unwrap_or("(none)")
    );
    if let Ok(group) = std::env::var("MTRXAI_ROOM_GROUP") {
        println!(" Docker room group: {}", group);
    }

    let mut client_config = load_client_config();
    apply_cli_overrides(
        &mut client_config,
        &lobby_server_host,
        llm_server_host.as_deref(),
    );

    crate::lobby_url::log_lobby_target(&client_config.lobby_host);
    crate::attestation::log_startup_diagnostics();

    let http_client = crate::lobby_url::build_lobby_http_client()?;

    match crate::client_config::fetch_lobby_config(&http_client, &client_config.lobby_host).await {
        Ok(cfg) => println!(
            " ✅ Lobby reachable — peer_registration={}",
            cfg.peer_registration
        ),
        Err(e) => eprintln!(" ⚠️ Lobby probe failed: {e}"),
    }

    let docker_group = std::env::var("MTRXAI_CLUSTER_NAME")
        .ok()
        .or_else(|| std::env::var("MTRXAI_ROOM_NAME").ok())
        .filter(|s| !s.trim().is_empty())
        .map(|_| "custom".to_string())
        .or_else(|| std::env::var("MTRXAI_ROOM_GROUP").ok());
    if docker_group.is_some() {
        let tx_store = TxStore::open_default()?;
        if let Err(e) =
            bootstrap_docker_peer(&http_client, &mut client_config, "custom", &tx_store).await
        {
            eprintln!("⚠️ Docker bootstrap: {e}");
        }
    }

    let tx_store = Arc::new(TxStore::open_default()?);
    println!(" Transaction DB: {}", tx_db::tx_db_path());

    if client_config.setup_complete && client_config.peer_id.is_some() {
        match sync_peer_device_key_with_lobby(&http_client, &mut client_config, tx_store.as_ref())
            .await
        {
            Ok(()) => {}
            Err(e) => {
                eprintln!("⚠️ Lobby peer identity rejected: {e}");
                invalidate_local_peer_identity(&mut client_config);
                save_client_config(&client_config)?;
            }
        }
    }

    let setup_complete = client_config.setup_complete;

    let mut credit_balance = 0i64;
    let peer_id = client_config.peer_id.clone().unwrap_or_default();
    let service_id = client_config.service_id.clone().unwrap_or_default();

    let peer_info = if setup_complete {
        discover_peer_location().await
    } else {
        None
    };

    let (proxy_cmd_tx, proxy_cmd_rx) = mpsc::channel(100);
    let proxy_cmd_rx_holder: Arc<Mutex<Option<mpsc::Receiver<shared::ProxyRequestCommand>>>> =
        Arc::new(Mutex::new(Some(proxy_cmd_rx)));
    let (swarm_proxy_cmd_tx, swarm_proxy_cmd_rx) = mpsc::channel(100);
    let swarm_proxy_cmd_rx_holder: Arc<Mutex<Option<mpsc::Receiver<shared::ProxyRequestCommand>>>> =
        Arc::new(Mutex::new(Some(swarm_proxy_cmd_rx)));
    let (model_start_tx, model_start_rx) = mpsc::channel(100);
    let model_start_cmd_rx_holder: Arc<Mutex<Option<mpsc::Receiver<shared::ModelStartAction>>>> =
        Arc::new(Mutex::new(Some(model_start_rx)));
    let (connection_tx, connection_rx) = mpsc::channel(100);
    let connection_cmd_rx_holder: Arc<Mutex<Option<mpsc::Receiver<shared::ConnectionAction>>>> =
        Arc::new(Mutex::new(Some(connection_rx)));
    let (swarm_model_start_tx, swarm_model_start_rx) = mpsc::channel(100);
    let swarm_model_start_cmd_rx_holder: Arc<
        Mutex<Option<mpsc::Receiver<shared::ModelStartAction>>>,
    > = Arc::new(Mutex::new(Some(swarm_model_start_rx)));
    let (runtime_event_tx, mut runtime_event_rx) = mpsc::channel(8);
    let (cluster_notify_tx, cluster_notify_rx) = mpsc::channel(8);
    let cluster_notify_rx_holder: Arc<Mutex<Option<mpsc::Receiver<()>>>> =
        Arc::new(Mutex::new(Some(cluster_notify_rx)));
    let (swarm_notify_tx, swarm_notify_rx) = mpsc::channel(8);
    let swarm_notify_rx_holder: Arc<Mutex<Option<mpsc::Receiver<()>>>> =
        Arc::new(Mutex::new(Some(swarm_notify_rx)));
    let (peer_moderation_tx, peer_moderation_rx) = mpsc::channel(32);
    let peer_moderation_rx_holder: Arc<
        Mutex<Option<mpsc::Receiver<shared::PeerModerationAction>>>,
    > = Arc::new(Mutex::new(Some(peer_moderation_rx)));
    let (swarm_moderation_tx, swarm_moderation_rx) = mpsc::channel(32);
    let swarm_moderation_rx_holder: Arc<
        Mutex<Option<mpsc::Receiver<shared::PeerModerationAction>>>,
    > = Arc::new(Mutex::new(Some(swarm_moderation_rx)));

    let cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let peer_registry: shared::PeerRegistry = Arc::new(Mutex::new(HashMap::new()));

    let shared_state: SharedState = Arc::new(Mutex::new(AppState {
        my_id: peer_id.clone(),
        peer_id: peer_id.clone(),
        service_id: service_id.clone(),
        clusters: Vec::new(),
        swarms: Vec::new(),
        credit_balance,
        local_models: Vec::new(),
        local_models_full: Vec::new(),
        network_models: Vec::new(),
        proxy_request_tx: proxy_cmd_tx,
        model_start_tx,
        connection_tx,
        swarm_proxy_request_tx: swarm_proxy_cmd_tx,
        swarm_model_start_tx,
        outgoing_model_requests: Vec::new(),
        incoming_model_offers: Vec::new(),
        incoming_connection_offers: Vec::new(),
        local_model_runs: Vec::new(),
        local_model_collisions: Vec::new(),
        peer_info,
        last_advertised_hash: None,
        show_info_cache: HashMap::new(),
        last_gpu_host: None,
        gpu_history: Default::default(),
        lobby_connected: false,
        peer_connections: Vec::new(),
        gpu_thermal_guard: Default::default(),
    }));

    let config_arc = Arc::new(RwLock::new(client_config.clone()));

    let peer_stats = crate::peer_stats::PeerStatsTrackerHandle::new(std::sync::Mutex::new(
        crate::peer_stats::PeerStatsTracker::new(),
    ));
    let proxy_bind = crate::security::proxy_bind_host();
    let proxy_state = Arc::new(llm_proxy::ProxyState::new(
        shared_state.clone(),
        config_arc.clone(),
        client_config.lobby_host.clone(),
        proxy_port,
        proxy_bind.clone(),
        runtime_event_tx.clone(),
        cluster_notify_tx.clone(),
        swarm_notify_tx.clone(),
        setup_complete,
        tx_store.clone(),
        peer_stats.clone(),
        peer_registry.clone(),
        peer_moderation_tx.clone(),
        &client_config,
    ));

    spawn_lobby_monitor(
        shared_state.clone(),
        http_client.clone(),
        config_arc.clone(),
    );

    spawn_llm_monitor(proxy_state.as_ref().clone(), config_arc.clone());

    spawn_network_scheduler(
        config_arc.clone(),
        cluster_notify_tx.clone(),
        swarm_notify_tx.clone(),
    );

    if setup_complete {
        spawn_transaction_sync(proxy_state.clone(), tx_store.clone());
    }

    if setup_complete && has_attached_llm_servers(&client_config) {
        init_registry_from_config(
            &proxy_state.llm_registry.inner,
            &client_config,
            &shared_state,
        )
        .await;
        let count = proxy_state.llm_registry.inner.attached_count().await;
        println!("✅ LLM registry: {} attached server(s)", count);
        spawn_gpu_monitor(
            shared_state.clone(),
            proxy_state.llm_registry.clone(),
            config_arc.clone(),
            cluster_notify_tx.clone(),
            swarm_notify_tx.clone(),
        );
    }

    let cluster_mgr_slot: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> =
        Arc::new(Mutex::new(None));
    let swarm_mgr_slot: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> =
        Arc::new(Mutex::new(None));

    spawn_cluster_manager(
        &cluster_mgr_slot,
        &proxy_cmd_rx_holder,
        &model_start_cmd_rx_holder,
        &connection_cmd_rx_holder,
        &cluster_notify_rx_holder,
        &peer_moderation_rx_holder,
        &proxy_state,
        &shared_state,
        &peer_registry,
        &cluster_network_models,
        &swarm_network_models,
    )
    .await;

    spawn_swarm_manager(
        &swarm_mgr_slot,
        &swarm_proxy_cmd_rx_holder,
        &swarm_model_start_cmd_rx_holder,
        &swarm_notify_rx_holder,
        &swarm_moderation_rx_holder,
        &proxy_state,
        &shared_state,
        &peer_registry,
        &cluster_network_models,
        &swarm_network_models,
    )
    .await;

    let cluster_mgr_slot_bg = cluster_mgr_slot.clone();
    let swarm_mgr_slot_bg = swarm_mgr_slot.clone();
    let proxy_cmd_rx_holder_bg = proxy_cmd_rx_holder.clone();
    let swarm_proxy_cmd_rx_holder_bg = swarm_proxy_cmd_rx_holder.clone();
    let model_start_cmd_rx_holder_bg = model_start_cmd_rx_holder.clone();
    let connection_cmd_rx_holder_bg = connection_cmd_rx_holder.clone();
    let swarm_model_start_cmd_rx_holder_bg = swarm_model_start_cmd_rx_holder.clone();
    let cluster_notify_rx_holder_bg = cluster_notify_rx_holder.clone();
    let swarm_notify_rx_holder_bg = swarm_notify_rx_holder.clone();
    let peer_moderation_rx_holder_bg = peer_moderation_rx_holder.clone();
    let swarm_moderation_rx_holder_bg = swarm_moderation_rx_holder.clone();
    let cluster_network_models_bg = cluster_network_models.clone();
    let swarm_network_models_bg = swarm_network_models.clone();
    let shared_bg = shared_state.clone();
    let proxy_bg = proxy_state.clone();
    let registry_bg = peer_registry.clone();
    let config_bg = config_arc.clone();
    let cluster_notify_tx_bg = cluster_notify_tx.clone();
    let swarm_notify_tx_bg = swarm_notify_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = runtime_event_rx.recv().await {
            match event {
                RuntimeEvent::SetupInvalidated => {
                    proxy_bg.setup_complete.store(false, Ordering::Relaxed);
                }
                RuntimeEvent::SetupComplete => {
                    proxy_bg.setup_complete.store(true, Ordering::Relaxed);
                    let cfg = config_bg.read().await.clone();
                    if has_attached_llm_servers(&cfg) {
                        init_registry_from_config(&proxy_bg.llm_registry.inner, &cfg, &shared_bg)
                            .await;
                        spawn_gpu_monitor(
                            shared_bg.clone(),
                            proxy_bg.llm_registry.clone(),
                            config_bg.clone(),
                            cluster_notify_tx_bg.clone(),
                            swarm_notify_tx_bg.clone(),
                        );
                    }
                    spawn_cluster_manager(
                        &cluster_mgr_slot_bg,
                        &proxy_cmd_rx_holder_bg,
                        &model_start_cmd_rx_holder_bg,
                        &connection_cmd_rx_holder_bg,
                        &cluster_notify_rx_holder_bg,
                        &peer_moderation_rx_holder_bg,
                        &proxy_bg,
                        &shared_bg,
                        &registry_bg,
                        &cluster_network_models_bg,
                        &swarm_network_models_bg,
                    )
                    .await;
                    spawn_swarm_manager(
                        &swarm_mgr_slot_bg,
                        &swarm_proxy_cmd_rx_holder_bg,
                        &swarm_model_start_cmd_rx_holder_bg,
                        &swarm_notify_rx_holder_bg,
                        &swarm_moderation_rx_holder_bg,
                        &proxy_bg,
                        &shared_bg,
                        &registry_bg,
                        &cluster_network_models_bg,
                        &swarm_network_models_bg,
                    )
                    .await;
                    spawn_transaction_sync(proxy_bg.clone(), proxy_bg.tx_store.clone());
                }
                RuntimeEvent::SwarmsChanged => {
                    spawn_swarm_manager(
                        &swarm_mgr_slot_bg,
                        &swarm_proxy_cmd_rx_holder_bg,
                        &swarm_model_start_cmd_rx_holder_bg,
                        &swarm_notify_rx_holder_bg,
                        &swarm_moderation_rx_holder_bg,
                        &proxy_bg,
                        &shared_bg,
                        &registry_bg,
                        &cluster_network_models_bg,
                        &swarm_network_models_bg,
                    )
                    .await;
                }
            }
        }
    });

    let proxy_state_for_server = (*proxy_state).clone();
    let sidecar_state = proxy_state.clone();
    if crate::inference_sidecar::should_use_sidecar() {
        tokio::spawn(async move {
            if let Err(e) = crate::inference_sidecar::run_inference_sidecar(sidecar_state).await {
                eprintln!("Inference sidecar error: {e}");
            }
        });
    }
    let proxy_task = tokio::spawn(async move {
        println!("🚀 Starting client server on port {}", proxy_port);
        if let Err(e) =
            llm_proxy::run_proxy_server(&proxy_bind, proxy_port, proxy_state_for_server).await
        {
            eprintln!("❌ Client server error: {}", e);
        }
    });

    tokio::select! {
        _ = proxy_task => {
            println!("Client server finished");
        }
        _ = async {
            loop {
                let mut guard = cluster_mgr_slot.lock().await;
                if let Some(handle) = guard.take() {
                    drop(guard);
                    let _ = handle.await;
                    break;
                }
                drop(guard);
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        } => {
            println!("Cluster manager finished");
        }
    }

    Ok(())
}
