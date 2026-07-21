use libp2p::identity::Keypair;
use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use libp2p::PeerId;
use sha2::{Digest, Sha256};

use crate::tx_db::TxStore;

const LIBP2P_KEY_RECORD_ID: &str = "__mtrxai_libp2p_identity__";

fn legacy_derive_keypair(peer_id: &str) -> Keypair {
    let digest = Sha256::digest(format!("mtrxai-libp2p-{peer_id}").as_bytes());
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&digest[..32]);
    Keypair::ed25519_from_bytes(seed).expect("valid ed25519 seed")
}

pub fn load_or_create_libp2p_keypair(
    tx_store: &TxStore,
    mtrxai_peer_id: &str,
) -> anyhow::Result<Keypair> {
    if let Some(protobuf) = tx_store.get_libp2p_key_sync(mtrxai_peer_id)? {
        return Keypair::from_protobuf_encoding(&protobuf)
            .map_err(|e| anyhow::anyhow!("invalid stored libp2p key: {e}"));
    }

    let keypair = Keypair::generate_ed25519();
    let protobuf = keypair
        .to_protobuf_encoding()
        .map_err(|e| anyhow::anyhow!("libp2p key encode: {e}"))?;
    tx_store.put_libp2p_key_sync(mtrxai_peer_id, &protobuf)?;
    Ok(keypair)
}

pub fn resolve_libp2p_peer_id(listen_addrs: &[String], mtrxai_peer_id: &str) -> PeerId {
    for addr_str in listen_addrs {
        if let Ok(addr) = addr_str.parse::<Multiaddr>() {
            for proto in addr.iter() {
                if let Protocol::P2p(id) = proto {
                    return id;
                }
            }
        }
    }
    legacy_derive_keypair(mtrxai_peer_id).public().to_peer_id()
}

pub fn peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| {
        if let Protocol::P2p(id) = p {
            Some(id)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libp2p_key_persists() {
        let store = TxStore::open_in_memory().unwrap();
        let kp1 = load_or_create_libp2p_keypair(&store, "peer-a").unwrap();
        let kp2 = load_or_create_libp2p_keypair(&store, "peer-a").unwrap();
        assert_eq!(kp1.public().to_peer_id(), kp2.public().to_peer_id());
    }
}
