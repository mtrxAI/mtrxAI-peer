use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

const PROXY_AUTH_MAX_SKEW_SECS: u64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyAuthProof {
    pub peer_id: String,
    pub timestamp: u64,
    pub mac: String,
}

pub fn build_proxy_auth(p2p_token: &str, peer_id: &str) -> ProxyAuthProof {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mac = compute_mac(p2p_token, peer_id, timestamp);
    ProxyAuthProof {
        peer_id: peer_id.to_string(),
        timestamp,
        mac,
    }
}

pub fn verify_proxy_auth(
    proof: &ProxyAuthProof,
    p2p_token: &str,
    expected_peer_id: &str,
) -> bool {
    if proof.peer_id != expected_peer_id {
        return false;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if proof.timestamp.abs_diff(now) > PROXY_AUTH_MAX_SKEW_SECS {
        return false;
    }
    let expected = compute_mac(p2p_token, &proof.peer_id, proof.timestamp);
    constant_time_eq(&proof.mac, &expected)
}

fn compute_mac(p2p_token: &str, peer_id: &str, timestamp: u64) -> String {
    let mut mac = HmacSha256::new_from_slice(p2p_token.as_bytes())
        .expect("HMAC accepts any key size");
    mac.update(b"mtrxai-proxy-auth-v1");
    mac.update(peer_id.as_bytes());
    mac.update(&timestamp.to_le_bytes());
    hex::encode(mac.finalize().into_bytes())
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
    fn proxy_auth_round_trip() {
        let proof = build_proxy_auth("secret-token", "peer-1");
        assert!(verify_proxy_auth(&proof, "secret-token", "peer-1"));
        assert!(!verify_proxy_auth(&proof, "wrong", "peer-1"));
        assert!(!verify_proxy_auth(&proof, "secret-token", "peer-2"));
    }
}
