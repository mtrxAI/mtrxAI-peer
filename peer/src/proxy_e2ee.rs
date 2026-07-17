//! E2EE proxy helpers for consumer and provider paths.

use crate::client_config::ClientConfig;
use crate::crypto::{
    decrypt_payload, derive_room_root_key, derive_session_key, derive_session_key_consumer,
    encrypt_payload, generate_ephemeral_keypair, load_room_static_keypair, EncryptedPayload,
};
use crate::p2p_protocol::{token_namespace, StreamMessage};
use crate::security::{build_proxy_auth, ProxyAuthProof};
use serde_json::Value;

/// Stable E2EE room id for a swarm — shared by every peer with the same `p2p_token`.
pub fn e2ee_room_id_for_swarm_token(p2p_token: &str) -> String {
    token_namespace(p2p_token)
}

/// Map a local swarm membership id to the shared E2EE room id.
pub fn e2ee_room_id_for_swarm_membership(cfg: &ClientConfig, swarm_id: &str) -> Option<String> {
    cfg.swarms
        .iter()
        .find(|s| s.swarm_id == swarm_id)
        .map(|s| e2ee_room_id_for_swarm_token(&s.p2p_token))
}

pub fn room_id_for_proxy(
    cfg: &ClientConfig,
    cluster_id: Option<&str>,
    swarm_id: Option<&str>,
) -> Option<String> {
    if let Some(cid) = cluster_id {
        return Some(cid.to_string());
    }
    swarm_id.and_then(|sid| e2ee_room_id_for_swarm_membership(cfg, sid))
}

pub fn cluster_effective_room_secret(cluster_id: &str, password: Option<&str>) -> String {
    format!("{cluster_id}:{}", password.unwrap_or(""))
}

fn cluster_password_from_membership(c: &crate::client_config::ClusterMembership) -> Option<&str> {
    c.cluster_password
        .as_deref()
        .or(c.room_secret.as_deref())
}

pub fn room_secret_for_proxy(cfg: &ClientConfig, room_id: &str) -> Option<String> {
    if let Some(c) = cfg.clusters.iter().find(|c| c.cluster_id == room_id) {
        return Some(cluster_effective_room_secret(
            &c.cluster_id,
            cluster_password_from_membership(c),
        ));
    }
    for swarm in &cfg.swarms {
        if e2ee_room_id_for_swarm_token(&swarm.p2p_token) == room_id
            || swarm.swarm_id == room_id
        {
            return Some(swarm.p2p_token.clone());
        }
    }
    None
}

pub fn decode_provider_static_public(hex: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(hex.trim()).map_err(|e| anyhow::anyhow!("invalid provider pk hex: {e}"))?;
    if bytes.len() != 32 {
        anyhow::bail!("provider static public key must be 32 bytes");
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&bytes);
    Ok(pk)
}

/// Resolve the provider's room static public key for consumer-side ECDH.
pub fn resolve_provider_static_public(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    room_id: &str,
    catalog_pk_hex: Option<&str>,
) -> anyhow::Result<[u8; 32]> {
    if let Some(hex) = catalog_pk_hex {
        if let Ok(pk) = decode_provider_static_public(hex) {
            return Ok(pk);
        }
    }
    // Swarm peers with the same room secret derive identical static keypairs.
    provider_static_public_key(tx_store, cfg, room_id)
}

pub async fn encrypt_proxy_request(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    peer_id: &str,
    req_id: &str,
    path: &str,
    body: &Value,
    room_id: &str,
    provider_static_public: &[u8; 32],
) -> anyhow::Result<(StreamMessage, [u8; 32])> {
    let room_secret = room_secret_for_proxy(cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("missing room secret for {room_id}"))?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let _static_keys = load_room_static_keypair(tx_store, room_id, &room_root)?;
    let ephemeral = generate_ephemeral_keypair();
    let ephemeral_secret = ephemeral.secret().to_bytes();
    let session_key = derive_session_key_consumer(
        ephemeral.secret(),
        provider_static_public,
        req_id,
        room_id,
    );
    let plain = serde_json::to_vec(body)?;
    let enc = encrypt_payload(&session_key, req_id, path, room_id, &plain)?;
    let auth = proxy_auth_for_room(cfg, peer_id, room_id);
    Ok((
        StreamMessage::EncryptedProxyRequest {
            req_id: req_id.to_string(),
            path: path.to_string(),
            room_id: room_id.to_string(),
            consumer_ephemeral_pk: ephemeral.public_key.to_vec(),
            nonce: enc.nonce,
            ciphertext: enc.ciphertext,
            aad_version: crate::crypto::envelope::AAD_VERSION,
            auth,
        },
        ephemeral_secret,
    ))
}

pub fn chunk_path(path: &str, seq: u32) -> String {
    format!("{path}#chunk/{seq}")
}

/// Consumer-side encryption for a single request body chunk (WebRTC E2EE).
pub fn encrypt_proxy_request_chunk(
    consumer_ephemeral_secret: &[u8; 32],
    provider_static_public: &[u8; 32],
    room_id: &str,
    req_id: &str,
    path: &str,
    seq: u32,
    plaintext: &[u8],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let ephemeral_secret = x25519_dalek::StaticSecret::from(*consumer_ephemeral_secret);
    let session_key = derive_session_key_consumer(
        &ephemeral_secret,
        provider_static_public,
        req_id,
        room_id,
    );
    let chunk_aad_path = chunk_path(path, seq);
    let enc = encrypt_payload(&session_key, req_id, &chunk_aad_path, room_id, plaintext)?;
    Ok((enc.nonce, enc.ciphertext))
}

pub async fn encrypt_proxy_chunk(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    room_id: &str,
    req_id: &str,
    path: &str,
    seq: u32,
    consumer_ephemeral_pk: &[u8],
    plaintext: &[u8],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let mut pk = [0u8; 32];
    if consumer_ephemeral_pk.len() != 32 {
        anyhow::bail!("invalid ephemeral public key");
    }
    pk.copy_from_slice(consumer_ephemeral_pk);
    let room_secret = room_secret_for_proxy(cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("missing room secret"))?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let static_keys = load_room_static_keypair(tx_store, room_id, &room_root)?;
    let session_key = derive_session_key(&static_keys.secret_key, &pk, req_id, room_id);
    let chunk_aad_path = chunk_path(path, seq);
    let enc = encrypt_payload(&session_key, req_id, &chunk_aad_path, room_id, plaintext)?;
    Ok((enc.nonce, enc.ciphertext))
}

pub async fn decrypt_proxy_chunk(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    req_id: &str,
    path: &str,
    seq: u32,
    room_id: &str,
    consumer_ephemeral_secret: &[u8; 32],
    provider_static_public: &[u8; 32],
    nonce: &[u8],
    ciphertext: &[u8],
) -> anyhow::Result<String> {
    let room_secret = room_secret_for_proxy(cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("missing room secret"))?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let _static_keys = load_room_static_keypair(tx_store, room_id, &room_root)?;
    let ephemeral_secret = x25519_dalek::StaticSecret::from(*consumer_ephemeral_secret);
    let session_key = derive_session_key_consumer(
        &ephemeral_secret,
        provider_static_public,
        req_id,
        room_id,
    );
    let chunk_aad_path = chunk_path(path, seq);
    let payload = EncryptedPayload {
        nonce: nonce.to_vec(),
        ciphertext: ciphertext.to_vec(),
    };
    let plain = decrypt_payload(&session_key, req_id, &chunk_aad_path, room_id, &payload)?;
    Ok(String::from_utf8(plain)?)
}

pub async fn decrypt_proxy_response(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    req_id: &str,
    path: &str,
    room_id: &str,
    consumer_ephemeral_secret: &[u8; 32],
    provider_static_public: &[u8; 32],
    msg: &StreamMessage,
) -> anyhow::Result<String> {
    let StreamMessage::EncryptedProxyResponse {
        nonce,
        ciphertext,
        error,
        ..
    } = msg
    else {
        anyhow::bail!("expected encrypted proxy response");
    };
    if let Some(err) = error {
        anyhow::bail!("{err}");
    }
    let room_secret = room_secret_for_proxy(cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("missing room secret"))?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let _static_keys = load_room_static_keypair(tx_store, room_id, &room_root)?;
    let ephemeral_secret = x25519_dalek::StaticSecret::from(*consumer_ephemeral_secret);
    let session_key = derive_session_key_consumer(
        &ephemeral_secret,
        provider_static_public,
        req_id,
        room_id,
    );
    let payload = EncryptedPayload {
        nonce: nonce.clone(),
        ciphertext: ciphertext.clone(),
    };
    let plain = decrypt_payload(&session_key, req_id, path, room_id, &payload)?;
    Ok(String::from_utf8(plain)?)
}

pub fn proxy_auth_for_room(
    cfg: &ClientConfig,
    peer_id: &str,
    room_id: &str,
) -> Option<ProxyAuthProof> {
    if let Some(c) = cfg.clusters.iter().find(|c| c.cluster_id == room_id) {
        let secret = cluster_effective_room_secret(
            &c.cluster_id,
            cluster_password_from_membership(c),
        );
        return Some(build_proxy_auth(&secret, peer_id));
    }
    for swarm in &cfg.swarms {
        if e2ee_room_id_for_swarm_token(&swarm.p2p_token) == room_id
            || swarm.swarm_id == room_id
        {
            return Some(build_proxy_auth(&swarm.p2p_token, peer_id));
        }
    }
    cfg.proxy_token
        .as_ref()
        .map(|t| build_proxy_auth(t, peer_id))
}

pub fn provider_static_public_key(
    tx_store: &crate::tx_db::TxStore,
    cfg: &ClientConfig,
    room_id: &str,
) -> anyhow::Result<[u8; 32]> {
    let room_secret = room_secret_for_proxy(cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("missing room secret"))?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let keys = load_room_static_keypair(tx_store, room_id, &room_root)?;
    Ok(keys.public_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::{ClusterMembership, SwarmMembership};
    use crate::security::verify_proxy_auth;

    const CLUSTER_ID: &str = "e0000005-0000-4000-8000-000000000005";

    fn cluster_cfg(password: Option<&str>) -> ClientConfig {
        ClientConfig {
            clusters: vec![ClusterMembership {
                cluster_id: CLUSTER_ID.to_string(),
                name: Some("Europe".to_string()),
                visibility: Some("public".to_string()),
                accepting_jobs: Some(true),
                connected: Some(true),
                room_secret: password.map(str::to_string),
                cluster_password: password.map(str::to_string),
                require_e2ee: Some(false),
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
    fn cluster_effective_room_secret_public_cluster() {
        assert_eq!(
            cluster_effective_room_secret(CLUSTER_ID, None),
            format!("{CLUSTER_ID}:")
        );
    }

    #[test]
    fn cluster_effective_room_secret_password_cluster() {
        assert_eq!(
            cluster_effective_room_secret(CLUSTER_ID, Some("secret")),
            format!("{CLUSTER_ID}:secret")
        );
    }

    #[test]
    fn room_secret_for_public_cluster_uses_cluster_id_colon_empty() {
        let cfg = cluster_cfg(None);
        assert_eq!(
            room_secret_for_proxy(&cfg, CLUSTER_ID).as_deref(),
            Some(format!("{CLUSTER_ID}:").as_str())
        );
    }

    #[test]
    fn room_secret_for_password_cluster_uses_cluster_id_colon_password() {
        let cfg = cluster_cfg(Some("secret"));
        assert_eq!(
            room_secret_for_proxy(&cfg, CLUSTER_ID).as_deref(),
            Some(format!("{CLUSTER_ID}:secret").as_str())
        );
    }

    #[test]
    fn public_and_password_clusters_derive_different_room_roots() {
        let public = derive_room_root_key(
            &room_secret_for_proxy(&cluster_cfg(None), CLUSTER_ID).unwrap(),
            CLUSTER_ID,
        );
        let protected = derive_room_root_key(
            &room_secret_for_proxy(&cluster_cfg(Some("secret")), CLUSTER_ID).unwrap(),
            CLUSTER_ID,
        );
        assert_ne!(public, protected);
    }

    #[test]
    fn proxy_auth_for_public_cluster_round_trip() {
        let cfg = cluster_cfg(None);
        let proof = proxy_auth_for_room(&cfg, "peer-1", CLUSTER_ID).expect("auth proof");
        let secret = room_secret_for_proxy(&cfg, CLUSTER_ID).unwrap();
        assert!(verify_proxy_auth(&proof, &secret, "peer-1"));
    }

    #[test]
    fn swarm_e2ee_room_id_is_stable_across_local_swarm_uuids() {
        let token = "shared-swarm-token";
        let cfg = ClientConfig {
            swarms: vec![
                SwarmMembership {
                    swarm_id: "local-uuid-peer-a".to_string(),
                    name: None,
                    p2p_token: token.to_string(),
                    bootnodes: vec![],
                    accepting_jobs: Some(true),
                    connected: Some(true),
                    require_e2ee: None,
                    require_tee: None,
                    required_attestation_flags: None,
                    schedule_enabled: None,
                    schedule_start: None,
                    schedule_end: None,
                },
                SwarmMembership {
                    swarm_id: "local-uuid-peer-b".to_string(),
                    name: None,
                    p2p_token: token.to_string(),
                    bootnodes: vec![],
                    accepting_jobs: Some(true),
                    connected: Some(true),
                    require_e2ee: None,
                    require_tee: None,
                    required_attestation_flags: None,
                    schedule_enabled: None,
                    schedule_start: None,
                    schedule_end: None,
                },
            ],
            ..ClientConfig::default()
        };
        let room_a = e2ee_room_id_for_swarm_membership(&cfg, "local-uuid-peer-a").unwrap();
        let room_b = e2ee_room_id_for_swarm_membership(&cfg, "local-uuid-peer-b").unwrap();
        assert_eq!(room_a, room_b);
        assert_eq!(room_a, e2ee_room_id_for_swarm_token(token));
    }
}
