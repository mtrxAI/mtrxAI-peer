use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

pub fn auth_hash_from_secret(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

pub fn verify_auth_hash(secret: &str, stored_hash: &str) -> bool {
    constant_time_eq(&auth_hash_from_secret(secret), stored_hash)
}

pub fn derive_room_root_key(room_secret: &str, room_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mtrxai-room-root-v1");
    hasher.update(room_secret.as_bytes());
    hasher.update(room_id.as_bytes());
    hasher.finalize().into()
}

pub struct RoomStaticKeys {
    pub public_key: [u8; 32],
    pub secret_key: [u8; 32],
}

fn derive_static_keypair_from_room_root(room_root: &[u8; 32]) -> RoomStaticKeys {
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&Sha256::digest([room_root.as_slice(), b"static-x25519"].concat())[..32]);
    let secret = StaticSecret::from(seed);
    let public = PublicKey::from(&secret);
    RoomStaticKeys {
        public_key: public.to_bytes(),
        secret_key: secret.to_bytes(),
    }
}

pub fn load_room_static_keypair(
    tx_store: &crate::tx_db::TxStore,
    room_id: &str,
    room_root: &[u8; 32],
) -> anyhow::Result<RoomStaticKeys> {
    let derived = derive_static_keypair_from_room_root(room_root);

    if let Some(stored) = tx_store.get_room_key_sync(room_id)? {
        if stored.len() == 64 {
            let mut public_key = [0u8; 32];
            let mut secret_key = [0u8; 32];
            public_key.copy_from_slice(&stored[..32]);
            secret_key.copy_from_slice(&stored[32..]);
            if public_key == derived.public_key && secret_key == derived.secret_key {
                return Ok(RoomStaticKeys {
                    public_key,
                    secret_key,
                });
            }
        }
    }

    let mut blob = Vec::with_capacity(64);
    blob.extend_from_slice(&derived.public_key);
    blob.extend_from_slice(&derived.secret_key);
    tx_store.put_room_key_sync(room_id, &blob)?;
    Ok(derived)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_hash_stable() {
        assert_eq!(
            auth_hash_from_secret("test"),
            auth_hash_from_secret("test")
        );
    }
}
