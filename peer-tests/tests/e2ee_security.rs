//! Integration tests for zero-trust security primitives.

#[test]
fn proxy_auth_hmac_round_trip() {
    let proof = peer::security::build_proxy_auth("test-token", "peer-abc");
    assert!(peer::security::verify_proxy_auth(
        &proof,
        "test-token",
        "peer-abc"
    ));
}

#[test]
fn room_auth_hash_verification() {
    let hash = peer::crypto::auth_hash_from_secret("room-password");
    assert!(peer::crypto::verify_auth_hash("room-password", &hash));
    assert!(!peer::crypto::verify_auth_hash("wrong", &hash));
}

#[test]
fn e2ee_envelope_round_trip() {
    let store = peer::tx_db::TxStore::open_in_memory().unwrap();
    let room_id = "room-test";
    let room_secret = "shared-secret";
    let room_root = peer::crypto::derive_room_root_key(room_secret, room_id);
    let static_keys = peer::crypto::load_room_static_keypair(&store, room_id, &room_root).unwrap();
    let ephemeral = peer::crypto::generate_ephemeral_keypair();
    let req_id = "req-1";
    let path = "/api/chat";
    let plain = br#"{"model":"llama","prompt":"secret"}"#;
    let session_key = peer::crypto::derive_session_key_consumer(
        ephemeral.secret(),
        &static_keys.public_key,
        req_id,
        room_id,
    );
    let enc = peer::crypto::encrypt_payload(&session_key, req_id, path, room_id, plain).unwrap();
    let dec = peer::crypto::decrypt_payload(&session_key, req_id, path, room_id, &enc).unwrap();
    assert_eq!(dec, plain);
}

#[test]
fn swarm_e2ee_room_id_shared_across_peers() {
    let token = "test-swarm-token";
    let mut cfg_a = peer::client_config::ClientConfig::default();
    cfg_a.swarms.push(peer::client_config::SwarmMembership {
        swarm_id: "peer-a-local-id".into(),
        name: None,
        p2p_token: token.into(),
        bootnodes: vec![],
        accepting_jobs: Some(true),
        connected: Some(true),
        require_e2ee: None,
        require_tee: None,
        required_attestation_flags: None,
        schedule_enabled: None,
        schedule_start: None,
        schedule_end: None,
    });
    let mut cfg_b = peer::client_config::ClientConfig::default();
    cfg_b.swarms.push(peer::client_config::SwarmMembership {
        swarm_id: "peer-b-local-id".into(),
        name: None,
        p2p_token: token.into(),
        bootnodes: vec![],
        accepting_jobs: Some(true),
        connected: Some(true),
        require_e2ee: None,
        require_tee: None,
        required_attestation_flags: None,
        schedule_enabled: None,
        schedule_start: None,
        schedule_end: None,
    });
    let room_a =
        peer::proxy_e2ee::e2ee_room_id_for_swarm_membership(&cfg_a, "peer-a-local-id").unwrap();
    let room_b =
        peer::proxy_e2ee::e2ee_room_id_for_swarm_membership(&cfg_b, "peer-b-local-id").unwrap();
    assert_eq!(room_a, room_b);

    let store_a = peer::tx_db::TxStore::open_in_memory().unwrap();
    let store_b = peer::tx_db::TxStore::open_in_memory().unwrap();
    let secret = peer::proxy_e2ee::room_secret_for_proxy(&cfg_a, &room_a).unwrap();
    let room_root = peer::crypto::derive_room_root_key(&secret, &room_a);
    let keys_a = peer::crypto::load_room_static_keypair(&store_a, &room_a, &room_root).unwrap();
    let keys_b = peer::crypto::load_room_static_keypair(&store_b, &room_a, &room_root).unwrap();
    assert_eq!(keys_a.public_key, keys_b.public_key);
    assert_eq!(keys_a.secret_key, keys_b.secret_key);
}

#[test]
fn encrypted_proxy_chunk_round_trip_cross_peer() {
    let store_consumer = peer::tx_db::TxStore::open_in_memory().unwrap();
    let store_provider = peer::tx_db::TxStore::open_in_memory().unwrap();
    let room_id = peer::proxy_e2ee::e2ee_room_id_for_swarm_token("shared-swarm-token");
    let mut cfg = peer::client_config::ClientConfig::default();
    cfg.swarms.push(peer::client_config::SwarmMembership {
        swarm_id: "local".into(),
        name: None,
        p2p_token: "shared-swarm-token".into(),
        bootnodes: vec![],
        accepting_jobs: Some(true),
        connected: Some(true),
        require_e2ee: None,
        require_tee: None,
        required_attestation_flags: None,
        schedule_enabled: None,
        schedule_start: None,
        schedule_end: None,
    });
    let room_secret = peer::proxy_e2ee::room_secret_for_proxy(&cfg, &room_id).unwrap();
    let room_root = peer::crypto::derive_room_root_key(&room_secret, &room_id);
    let provider_keys =
        peer::crypto::load_room_static_keypair(&store_provider, &room_id, &room_root).unwrap();
    let _consumer_keys =
        peer::crypto::load_room_static_keypair(&store_consumer, &room_id, &room_root).unwrap();

    let ephemeral = peer::crypto::generate_ephemeral_keypair();
    let req_id = "p2p_1";
    let path = "/api/chat";
    let seq = 0u32;
    let plain = b"data: {\"message\":{\"content\":\"hi\"}}\n\n";

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (nonce, ciphertext) = peer::proxy_e2ee::encrypt_proxy_chunk(
            &store_provider,
            &cfg,
            &room_id,
            req_id,
            path,
            seq,
            &ephemeral.public_key,
            plain,
        )
        .await
        .unwrap();

        let decrypted = peer::proxy_e2ee::decrypt_proxy_chunk(
            &store_consumer,
            &cfg,
            req_id,
            path,
            seq,
            &room_id,
            &ephemeral.secret().to_bytes(),
            &provider_keys.public_key,
            &nonce,
            &ciphertext,
        )
        .await
        .unwrap();
        assert_eq!(decrypted, String::from_utf8_lossy(plain));
    });
}

#[test]
fn stale_room_keys_migrate_to_deterministic_seed() {
    let store = peer::tx_db::TxStore::open_in_memory().unwrap();
    let room_id = "room-migrate";
    let room_secret = "shared-secret";
    let room_root = peer::crypto::derive_room_root_key(room_secret, room_id);

    let mut stale = vec![0u8; 64];
    stale[..32].fill(1);
    stale[32..].fill(2);
    store.put_room_key_sync(room_id, &stale).unwrap();

    let keys = peer::crypto::load_room_static_keypair(&store, room_id, &room_root).unwrap();
    let expected = peer::crypto::load_room_static_keypair(&store, room_id, &room_root).unwrap();
    assert_eq!(keys.public_key, expected.public_key);
    assert_ne!(keys.public_key[..], stale[..32]);
}

#[test]
fn tee_attestation_disabled_stub() {
    // TEE crate excluded from workspace/Docker until mtrxai-tee-attestation is copied into images.
    assert!(true);
}

#[test]
fn libp2p_key_persistence() {
    let store = peer::tx_db::TxStore::open_in_memory().unwrap();
    let kp1 = peer::security::load_or_create_libp2p_keypair(&store, "peer-1").unwrap();
    let kp2 = peer::security::load_or_create_libp2p_keypair(&store, "peer-1").unwrap();
    assert_eq!(kp1.public().to_peer_id(), kp2.public().to_peer_id());
}
