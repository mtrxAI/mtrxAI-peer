use peer::network_catalog::sync_unified_network_models;
use peer::shared::AppState;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

mod common;

fn empty_app_state() -> AppState {
    let (tx, _rx) = mpsc::channel(1);
    common::test_app_state(tx)
}

fn model_for_peer(peer_id: &str, loaded: bool) -> serde_json::Value {
    let peer = json!({ "id": peer_id, "loaded": loaded });
    json!({
        "name": "llama3",
        "_peer": peer,
        "_peers": [peer],
        "_peer_count": 1,
        "_loaded_count": u64::from(loaded),
    })
}

#[tokio::test]
async fn aggregated_network_models_deduplicate_peer_across_clusters() {
    let shared_state = Arc::new(Mutex::new(empty_app_state()));
    let cluster_network_models = Arc::new(Mutex::new(HashMap::from([
        (
            "cluster-a".to_string(),
            vec![model_for_peer("peer-me", true)],
        ),
        (
            "cluster-b".to_string(),
            vec![model_for_peer("peer-me", true)],
        ),
    ])));

    let swarm_network_models = Arc::new(Mutex::new(HashMap::new()));

    sync_unified_network_models(
        &shared_state,
        &cluster_network_models,
        &swarm_network_models,
    )
    .await;

    let state = shared_state.lock().await;
    assert_eq!(state.network_models.len(), 1);
    assert_eq!(state.network_models[0]["name"], "llama3");
    assert_eq!(state.network_models[0]["_peer_count"], 1);
    assert_eq!(state.network_models[0]["_loaded_count"], 1);
    assert_eq!(
        state.network_models[0]["_clusters"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn aggregated_network_models_keeps_distinct_peers() {
    let shared_state = Arc::new(Mutex::new(empty_app_state()));
    let cluster_network_models = Arc::new(Mutex::new(HashMap::from([
        (
            "cluster-a".to_string(),
            vec![model_for_peer("peer-a", false)],
        ),
        (
            "cluster-b".to_string(),
            vec![model_for_peer("peer-b", true)],
        ),
    ])));

    let swarm_network_models = Arc::new(Mutex::new(HashMap::new()));

    sync_unified_network_models(
        &shared_state,
        &cluster_network_models,
        &swarm_network_models,
    )
    .await;

    let state = shared_state.lock().await;
    assert_eq!(state.network_models.len(), 1);
    assert_eq!(state.network_models[0]["_peer_count"], 2);
    assert_eq!(state.network_models[0]["_loaded_count"], 1);
}
