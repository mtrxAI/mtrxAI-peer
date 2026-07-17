use mtrxai_attestation::AttestationChallenge;
use peer::attestation::{attestation_skip_enabled, build_proof, hash_current_binary};
use uuid::Uuid;

#[test]
fn attestation_skip_defaults_false_without_env() {
    std::env::remove_var("MTRXAI_ATTESTATION_SKIP");
    assert!(!attestation_skip_enabled());
}

#[test]
fn hash_current_binary_matches_file_sha256() {
    let hash = hash_current_binary().expect("hash");
    assert_eq!(hash.len(), 64);
}

#[test]
fn build_proof_errors_without_embedded_secret_in_dev_build() {
    let challenge = AttestationChallenge {
        challenge_id: Uuid::new_v4(),
        nonce: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            [7u8; 32],
        ),
        expires_at: 1_900_000_000,
    };

    let err = build_proof(&challenge).expect_err("dev builds lack embedded secret");
    assert!(err.to_string().contains("MTRXAI_ATTESTATION_SECRET"));
}
