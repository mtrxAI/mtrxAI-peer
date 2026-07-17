use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use getrandom::getrandom;
use serde::{Deserialize, Serialize};

pub const AAD_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedPayload {
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

pub fn encrypt_payload(
    session_key: &[u8; 32],
    req_id: &str,
    path: &str,
    room_id: &str,
    plaintext: &[u8],
) -> anyhow::Result<EncryptedPayload> {
    let cipher = ChaCha20Poly1305::new_from_slice(session_key)
        .map_err(|e| anyhow::anyhow!("cipher init: {e}"))?;
    let mut nonce_bytes = [0u8; 12];
    getrandom(&mut nonce_bytes).map_err(|e| anyhow::anyhow!("nonce: {e}"))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let aad = build_aad(req_id, path, room_id);
    let ciphertext = cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| anyhow::anyhow!("encrypt: {e}"))?;
    Ok(EncryptedPayload {
        nonce: nonce_bytes.to_vec(),
        ciphertext,
    })
}

pub fn decrypt_payload(
    session_key: &[u8; 32],
    req_id: &str,
    path: &str,
    room_id: &str,
    payload: &EncryptedPayload,
) -> anyhow::Result<Vec<u8>> {
    let cipher = ChaCha20Poly1305::new_from_slice(session_key)
        .map_err(|e| anyhow::anyhow!("cipher init: {e}"))?;
    if payload.nonce.len() != 12 {
        anyhow::bail!("invalid nonce length");
    }
    let nonce = Nonce::from_slice(&payload.nonce);
    let aad = build_aad(req_id, path, room_id);
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: &payload.ciphertext,
                aad: &aad,
            },
        )
        .map_err(|e| anyhow::anyhow!("decrypt: {e}"))
}

fn build_aad(req_id: &str, path: &str, room_id: &str) -> Vec<u8> {
    format!("mtrxai-v{AAD_VERSION}|{req_id}|{path}|{room_id}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trip() {
        let key = [7u8; 32];
        let plain = br#"{"model":"llama","prompt":"hi"}"#;
        let enc = encrypt_payload(&key, "r1", "/api/chat", "room-1", plain).unwrap();
        let dec = decrypt_payload(&key, "r1", "/api/chat", "room-1", &enc).unwrap();
        assert_eq!(dec, plain);
    }
}
