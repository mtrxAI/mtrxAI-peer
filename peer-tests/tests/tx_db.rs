use peer::tx_db::{TransactionView, TxStore};
use std::sync::Arc;
use uuid::Uuid;

fn local_peer() -> String {
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string()
}

fn other_peer() -> String {
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".to_string()
}

fn unrelated_peer() -> String {
    "cccccccc-cccc-cccc-cccc-cccccccccccc".to_string()
}

#[tokio::test]
async fn record_local_report_creates_pending_transaction() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let remote = other_peer();

    store
        .record_local_report(
            "req-1".to_string(),
            "consumer",
            &local,
            remote.clone(),
            "llama3".to_string(),
            10,
            20,
            30,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].req_id, "req-1");
    assert_eq!(rows[0].role, "consumer");
    assert_eq!(rows[0].counterparty_peer_id, remote);
    assert_eq!(rows[0].model, "llama3");
    assert_eq!(rows[0].total_tokens, 30);
    assert_eq!(rows[0].status, "pending");
    assert_eq!(rows[0].local_credit_delta, 0);
}

#[tokio::test]
async fn upsert_from_lobby_updates_credit_deltas_and_stats() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let remote = other_peer();

    store
        .record_local_report(
            "req-2".to_string(),
            "consumer",
            &local,
            remote.clone(),
            "mistral".to_string(),
            5,
            5,
            10,
        )
        .await
        .unwrap();

    let consumer_id = Uuid::parse_str(&local).unwrap();
    let provider_id = Uuid::parse_str(&remote).unwrap();

    store
        .upsert_from_lobby(
            TransactionView {
                req_id: "req-2".to_string(),
                consumer_peer_id: Some(consumer_id),
                provider_peer_id: Some(provider_id),
                model: Some("mistral".to_string()),
                total_tokens: Some(10),
                same_service: Some(false),
                consumer_credit_delta: Some(-25),
                provider_credit_delta: Some(25),
                status: "settled".to_string(),
                settled_at: Some(chrono::Utc::now()),
            },
            &local,
            None,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "settled");
    assert_eq!(rows[0].local_credit_delta, -25);

    let stats = store.stats(&local, None).await.unwrap();
    assert_eq!(stats.by_model.len(), 1);
    assert_eq!(stats.by_model[0].model, "mistral");
    assert_eq!(stats.by_model[0].consumed, 25);
    assert_eq!(stats.by_model[0].earned, 0);
    assert_eq!(stats.by_peer.len(), 1);
    assert_eq!(stats.by_peer[0].peer_id, remote);
    assert_eq!(stats.by_peer[0].consumed, 25);
}

#[tokio::test]
async fn record_local_report_reuse_resets_terminal_row() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let remote = other_peer();

    store
        .record_local_report(
            "req-reuse".to_string(),
            "consumer",
            &local,
            remote.clone(),
            "llama3".to_string(),
            1,
            1,
            2,
        )
        .await
        .unwrap();

    store
        .upsert_from_lobby(
            TransactionView {
                req_id: "req-reuse".to_string(),
                consumer_peer_id: Some(Uuid::parse_str(&local).unwrap()),
                provider_peer_id: Some(Uuid::parse_str(&remote).unwrap()),
                model: Some("llama3".to_string()),
                total_tokens: Some(2),
                same_service: Some(false),
                consumer_credit_delta: Some(0),
                provider_credit_delta: Some(0),
                status: "mismatched".to_string(),
                settled_at: Some(chrono::Utc::now()),
            },
            &local,
            None,
        )
        .await
        .unwrap();

    store
        .record_local_report(
            "req-reuse".to_string(),
            "consumer",
            &local,
            remote,
            "llama3".to_string(),
            10,
            20,
            30,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "pending");
    assert_eq!(rows[0].total_tokens, 30);
    assert_eq!(rows[0].settled_at_unix, 0);
}

#[tokio::test]
async fn upsert_from_lobby_ignores_unrelated_transactions() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let peer_a = other_peer();
    let peer_b = unrelated_peer();

    let consumer_id = Uuid::parse_str(&peer_a).unwrap();
    let provider_id = Uuid::parse_str(&peer_b).unwrap();

    store
        .upsert_from_lobby(
            TransactionView {
                req_id: "req-other".to_string(),
                consumer_peer_id: Some(consumer_id),
                provider_peer_id: Some(provider_id),
                model: Some("qwen".to_string()),
                total_tokens: Some(100),
                same_service: Some(false),
                consumer_credit_delta: Some(-10),
                provider_credit_delta: Some(10),
                status: "settled".to_string(),
                settled_at: Some(chrono::Utc::now()),
            },
            &local,
            None,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn provider_role_earns_credits_in_stats() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let remote = other_peer();

    let consumer_id = Uuid::parse_str(&remote).unwrap();
    let provider_id = Uuid::parse_str(&local).unwrap();

    store
        .upsert_from_lobby(
            TransactionView {
                req_id: "req-3".to_string(),
                consumer_peer_id: Some(consumer_id),
                provider_peer_id: Some(provider_id),
                model: Some("phi3".to_string()),
                total_tokens: Some(50),
                same_service: Some(false),
                consumer_credit_delta: Some(-40),
                provider_credit_delta: Some(40),
                status: "settled".to_string(),
                settled_at: Some(chrono::Utc::now()),
            },
            &local,
            None,
        )
        .await
        .unwrap();

    let stats = store.stats(&local, None).await.unwrap();
    assert_eq!(stats.by_model[0].earned, 40);
    assert_eq!(stats.by_model[0].consumed, 0);
    assert_eq!(stats.by_peer[0].earned, 40);
}

#[tokio::test]
async fn record_local_inference_creates_local_status_row() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();

    store
        .record_local_inference(
            "local-req-1".to_string(),
            &local,
            "llama3".to_string(),
            10,
            20,
            30,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "local");
    assert_eq!(rows[0].role, "local");
    assert_eq!(rows[0].counterparty_peer_id, local);
    assert_eq!(rows[0].total_tokens, 30);

    let stats = store.stats(&local, None).await.unwrap();
    assert_eq!(stats.local_tokens, 30);
    assert_eq!(stats.swarm_tokens, 0);
}

#[tokio::test]
async fn record_swarm_report_creates_swarm_status_row() {
    let store = Arc::new(TxStore::open_in_memory().unwrap());
    let local = local_peer();
    let remote = other_peer();

    store
        .record_swarm_report(
            "swarm-req-1".to_string(),
            "provider",
            &local,
            remote.clone(),
            "mistral".to_string(),
            5,
            15,
            20,
        )
        .await
        .unwrap();

    let rows = store.list_transactions(10, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "swarm");
    assert_eq!(rows[0].role, "provider");
    assert_eq!(rows[0].counterparty_peer_id, remote);

    let stats = store.stats(&local, None).await.unwrap();
    assert_eq!(stats.swarm_tokens, 20);
    assert_eq!(stats.local_tokens, 0);
}

#[tokio::test]
async fn chat_session_create_list_get_delete() {
    use peer::tx_db::ChatMessage;

    let store = Arc::new(TxStore::open_in_memory().unwrap());

    let created = store
        .upsert_chat_session(
            "chat-1".to_string(),
            "Hello world".to_string(),
            "llama3".to_string(),
            vec![ChatMessage {
                role: "user".to_string(),
                content: "Hello world".to_string(),
            }],
            None,
        )
        .await
        .unwrap();
    assert_eq!(created.id, "chat-1");
    assert_eq!(created.model, "llama3");
    assert_eq!(created.messages.len(), 1);

    let listed = store.list_chat_sessions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "chat-1");
    assert_eq!(listed[0].message_count, 1);
    assert_eq!(listed[0].title, "Hello world");

    let loaded = store.get_chat_session("chat-1").await.unwrap().unwrap();
    assert_eq!(loaded.messages[0].content, "Hello world");

    store
        .upsert_chat_session(
            "chat-1".to_string(),
            "Hello world".to_string(),
            "llama3".to_string(),
            vec![
                ChatMessage {
                    role: "user".to_string(),
                    content: "Hello world".to_string(),
                },
                ChatMessage {
                    role: "assistant".to_string(),
                    content: "Hi!".to_string(),
                },
            ],
            Some(created.created_at_unix),
        )
        .await
        .unwrap();

    let updated = store.get_chat_session("chat-1").await.unwrap().unwrap();
    assert_eq!(updated.messages.len(), 2);
    assert_eq!(updated.created_at_unix, created.created_at_unix);

    assert!(store.delete_chat_session("chat-1").await.unwrap());
    assert!(store.get_chat_session("chat-1").await.unwrap().is_none());
    assert!(store.list_chat_sessions().await.unwrap().is_empty());
    assert!(!store.delete_chat_session("chat-1").await.unwrap());
}
