use ed25519_dalek::SigningKey;
use mtrxai_attestation::peer_auth::{
    public_key_hex_from_signing_key, sign_peer_auth, signing_key_from_seed_hex, PeerAuthProof,
};
use uuid::Uuid;

use crate::tx_db::TxStore;

pub fn load_or_create_peer_signing_key(tx_store: &TxStore) -> anyhow::Result<SigningKey> {
    if let Some(seed_hex) = tx_store.get_peer_auth_key_sync()? {
        return signing_key_from_seed_hex(&seed_hex)
            .map_err(|e| anyhow::anyhow!("invalid stored peer auth key: {e}"));
    }

    let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
    let seed_hex = hex::encode(signing_key.to_bytes());
    tx_store.put_peer_auth_key_sync(&seed_hex)?;
    Ok(signing_key)
}

pub fn peer_public_key_hex(tx_store: &TxStore) -> anyhow::Result<String> {
    let key = load_or_create_peer_signing_key(tx_store)?;
    Ok(public_key_hex_from_signing_key(&key))
}

pub fn build_peer_auth_proof(tx_store: &TxStore, peer_id: &str) -> anyhow::Result<PeerAuthProof> {
    let peer_uuid = Uuid::parse_str(peer_id)?;
    let signing_key = load_or_create_peer_signing_key(tx_store)?;
    let timestamp = chrono::Utc::now().timestamp();
    Ok(sign_peer_auth(&signing_key, peer_uuid, timestamp))
}

pub fn build_ws_auth_query(tx_store: &TxStore, peer_id: &str) -> anyhow::Result<(String, String)> {
    let proof = build_peer_auth_proof(tx_store, peer_id)?;
    Ok((proof.timestamp.to_string(), proof.signature))
}

pub async fn peer_public_key_hex_async(tx_store: &TxStore) -> anyhow::Result<String> {
    let store = tx_store.clone();
    tokio::task::spawn_blocking(move || peer_public_key_hex(&store)).await?
}

pub async fn build_peer_auth_proof_async(
    tx_store: &TxStore,
    peer_id: &str,
) -> anyhow::Result<PeerAuthProof> {
    let store = tx_store.clone();
    let peer_id = peer_id.to_string();
    tokio::task::spawn_blocking(move || build_peer_auth_proof(&store, &peer_id)).await?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trip() {
        let store = TxStore::open_in_memory().unwrap();
        let pk1 = peer_public_key_hex(&store).unwrap();
        let pk2 = peer_public_key_hex(&store).unwrap();
        assert_eq!(pk1, pk2);
    }

    #[tokio::test]
    async fn key_round_trip_async() {
        let store = TxStore::open_in_memory().unwrap();
        let pk1 = peer_public_key_hex_async(&store).await.unwrap();
        let pk2 = peer_public_key_hex_async(&store).await.unwrap();
        assert_eq!(pk1, pk2);
        assert!(!pk1.is_empty());
    }
}
