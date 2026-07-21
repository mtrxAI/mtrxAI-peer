use crate::client_config::{
    cluster_accepts_jobs, cluster_is_connected, save_client_config, swarm_accepts_jobs,
    swarm_is_connected, ClientConfig,
};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

pub fn network_all_disconnected(cfg: &ClientConfig) -> bool {
    let clusters = cfg.clusters.iter().all(|c| !cluster_is_connected(c));
    let swarms = cfg.swarms.iter().all(|s| !swarm_is_connected(s));
    (cfg.clusters.is_empty() || clusters) && (cfg.swarms.is_empty() || swarms)
}

pub fn network_all_paused(cfg: &ClientConfig) -> bool {
    let clusters = cfg
        .clusters
        .iter()
        .filter(|c| cluster_is_connected(c))
        .all(|c| !cluster_accepts_jobs(c));
    let swarms = cfg
        .swarms
        .iter()
        .filter(|s| swarm_is_connected(s))
        .all(|s| !swarm_accepts_jobs(s));
    let has_connected =
        cfg.clusters.iter().any(cluster_is_connected) || cfg.swarms.iter().any(swarm_is_connected);
    has_connected && clusters && swarms
}

pub async fn disconnect_all(
    client_config: &Arc<RwLock<ClientConfig>>,
    cluster_notify_tx: &mpsc::Sender<()>,
    swarm_notify_tx: &mpsc::Sender<()>,
) -> Result<(), String> {
    {
        let mut cfg = client_config.write().await;
        for cluster in &mut cfg.clusters {
            cluster.connected = Some(false);
        }
        for swarm in &mut cfg.swarms {
            swarm.connected = Some(false);
        }
        save_client_config(&cfg).map_err(|e| e.to_string())?;
    }
    let _ = cluster_notify_tx.send(()).await;
    let _ = swarm_notify_tx.send(()).await;
    Ok(())
}

pub async fn connect_all(
    client_config: &Arc<RwLock<ClientConfig>>,
    cluster_notify_tx: &mpsc::Sender<()>,
    swarm_notify_tx: &mpsc::Sender<()>,
) -> Result<(), String> {
    {
        let mut cfg = client_config.write().await;
        for cluster in &mut cfg.clusters {
            cluster.connected = Some(true);
        }
        for swarm in &mut cfg.swarms {
            swarm.connected = Some(true);
        }
        save_client_config(&cfg).map_err(|e| e.to_string())?;
    }
    let _ = cluster_notify_tx.send(()).await;
    let _ = swarm_notify_tx.send(()).await;
    Ok(())
}

pub async fn set_pause_all(
    client_config: &Arc<RwLock<ClientConfig>>,
    cluster_notify_tx: &mpsc::Sender<()>,
    swarm_notify_tx: &mpsc::Sender<()>,
    paused: bool,
) -> Result<(), String> {
    {
        let mut cfg = client_config.write().await;
        for cluster in &mut cfg.clusters {
            cluster.accepting_jobs = Some(!paused);
        }
        for swarm in &mut cfg.swarms {
            swarm.accepting_jobs = Some(!paused);
        }
        save_client_config(&cfg).map_err(|e| e.to_string())?;
    }
    let _ = cluster_notify_tx.send(()).await;
    let _ = swarm_notify_tx.send(()).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::{ClientConfig, ClusterMembership, SwarmMembership};

    fn sample_cfg(connected: bool, paused: bool) -> ClientConfig {
        ClientConfig {
            clusters: vec![ClusterMembership {
                cluster_id: "c1".into(),
                name: None,
                visibility: None,
                accepting_jobs: Some(!paused),
                connected: Some(connected),
                room_secret: None,
                cluster_password: None,
                require_e2ee: None,
                require_tee: None,
                required_attestation_flags: None,
                schedule_enabled: None,
                schedule_start: None,
                schedule_end: None,
            }],
            swarms: vec![SwarmMembership {
                swarm_id: "s1".into(),
                name: None,
                p2p_token: "tok".into(),
                bootnodes: vec![],
                accepting_jobs: Some(!paused),
                connected: Some(connected),
                require_e2ee: None,
                require_tee: None,
                required_attestation_flags: None,
                schedule_enabled: None,
                schedule_start: None,
                schedule_end: None,
            }],
            ..ClientConfig::default()
        }
    }

    #[test]
    fn network_all_disconnected_when_all_off() {
        assert!(network_all_disconnected(&sample_cfg(false, false)));
    }

    #[test]
    fn network_all_paused_when_connected_and_not_accepting() {
        assert!(network_all_paused(&sample_cfg(true, true)));
    }
}
