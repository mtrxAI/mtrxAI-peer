use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

pub struct EphemeralKeypair {
    pub public_key: [u8; 32],
    secret: StaticSecret,
}

impl EphemeralKeypair {
    pub fn secret(&self) -> &StaticSecret {
        &self.secret
    }
}

pub fn generate_ephemeral_keypair() -> EphemeralKeypair {
    let secret = StaticSecret::random_from_rng(rand::rngs::OsRng);
    let public = PublicKey::from(&secret);
    EphemeralKeypair {
        public_key: public.to_bytes(),
        secret,
    }
}

pub fn derive_session_key(
    room_static_secret: &[u8; 32],
    consumer_ephemeral_public: &[u8; 32],
    req_id: &str,
    room_id: &str,
) -> [u8; 32] {
    let static_secret = StaticSecret::from(*room_static_secret);
    let their_public = PublicKey::from(*consumer_ephemeral_public);
    let shared = static_secret.diffie_hellman(&their_public);
    hkdf_session_key(shared.as_bytes(), req_id, room_id)
}

pub fn derive_session_key_consumer(
    consumer_ephemeral_secret: &StaticSecret,
    provider_static_public: &[u8; 32],
    req_id: &str,
    room_id: &str,
) -> [u8; 32] {
    let their_public = PublicKey::from(*provider_static_public);
    let shared = consumer_ephemeral_secret.diffie_hellman(&their_public);
    hkdf_session_key(shared.as_bytes(), req_id, room_id)
}

fn hkdf_session_key(shared: &[u8], req_id: &str, room_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mtrxai-session-key-v1");
    hasher.update(shared);
    hasher.update(req_id.as_bytes());
    hasher.update(room_id.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_agreement() {
        let provider = StaticSecret::random_from_rng(rand::rngs::OsRng);
        let provider_public = PublicKey::from(&provider).to_bytes();
        let consumer = StaticSecret::random_from_rng(rand::rngs::OsRng);
        let consumer_public = PublicKey::from(&consumer).to_bytes();
        let provider_secret = provider.to_bytes();

        let k1 = derive_session_key(&provider_secret, &consumer_public, "req-1", "room-a");
        let k2 = derive_session_key_consumer(&consumer, &provider_public, "req-1", "room-a");
        assert_eq!(k1, k2);
    }
}
