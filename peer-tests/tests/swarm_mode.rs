use peer::network_catalog::sync_unified_network_models;
use peer::p2p_protocol::{decode_gossip, encode_gossip, GossipMessage, StreamMessage};
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

#[tokio::test]
async fn unified_network_models_merge_cluster_and_swarm() {
    let shared_state = Arc::new(Mutex::new(empty_app_state()));
    let cluster_models = Arc::new(Mutex::new(HashMap::from([(
        "cluster-a".to_string(),
        vec![json!({
            "name": "llama3",
            "_peer": { "id": "peer-a", "loaded": true },
            "_peers": [{ "id": "peer-a", "loaded": true }],
            "_peer_count": 1,
            "_loaded_count": 1,
        })],
    )])));
    let swarm_models = Arc::new(Mutex::new(HashMap::from([(
        "swarm-x".to_string(),
        vec![json!({
            "name": "llama3",
            "_peer": { "id": "peer-b", "loaded": false },
            "_peers": [{ "id": "peer-b", "loaded": false }],
            "_peer_count": 1,
            "_loaded_count": 0,
        })],
    )])));

    sync_unified_network_models(&shared_state, &cluster_models, &swarm_models).await;

    let state = shared_state.lock().await;
    assert_eq!(state.network_models.len(), 1);
    assert_eq!(state.network_models[0]["_peer_count"], 2);
    assert!(state.network_models[0].get("_cluster_id").is_some());
    assert!(state.network_models[0].get("_swarm_id").is_some());
}

#[test]
fn p2p_protocol_gossip_roundtrip() {
    let msg = GossipMessage::CatalogUpdate {
        peer_id: "peer-1".to_string(),
        models: vec![json!({"name": "llama3"})],
        peer_info: None,
        gpu_host: None,
        accepting_jobs: true,
        listen_addrs: Vec::new(),
        tee_capable: None,
        gpu_model: None,
        attestation_expiry: None,
        trust_level: None,
        provider_static_pk: None,
    };
    let bytes = encode_gossip(&msg).unwrap();
    let decoded = decode_gossip(&bytes).unwrap();
    match decoded {
        GossipMessage::CatalogUpdate { peer_id, .. } => assert_eq!(peer_id, "peer-1"),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn compact_models_for_gossip_strips_heavy_fields() {
    let models = vec![json!({
        "name": "llama3",
        "modelfile": "A".repeat(100_000),
        "_status": { "loaded": true, "cpu_pct": 10, "gpu_pct": 20, "extra": "x" },
    })];
    let compact = peer::p2p_protocol::compact_models_for_gossip(&models);
    assert_eq!(compact.len(), 1);
    assert_eq!(compact[0]["name"], "llama3");
    assert!(compact[0].get("modelfile").is_none());
    assert_eq!(compact[0]["_status"]["loaded"], true);
    let bytes =
        peer::p2p_protocol::encode_gossip(&peer::p2p_protocol::GossipMessage::CatalogUpdate {
            peer_id: "p1".into(),
            models: compact,
            peer_info: None,
            gpu_host: None,
            accepting_jobs: true,
            listen_addrs: vec![],
            tee_capable: None,
            gpu_model: None,
            attestation_expiry: None,
            trust_level: None,
            provider_static_pk: None,
        })
        .unwrap();
    assert!(bytes.len() < peer::p2p_protocol::GOSSIP_CATALOG_MAX_BYTES);
}

#[test]
fn stream_message_serializes_with_type_tag() {
    let msg = StreamMessage::ProxyRequestStart {
        req_id: "r1".to_string(),
        path: "/v1/chat/completions".to_string(),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"proxyrequeststart\""));
}
