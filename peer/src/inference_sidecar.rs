//! Inference sidecar: holds session keys and talks to Ollama; P2P relay stays blind.

use crate::crypto::{
    decrypt_payload, derive_room_root_key, derive_session_key, encrypt_payload,
    load_room_static_keypair, EncryptedPayload,
};
use crate::inference_ipc::InferenceIpcServer;
use crate::p2p_protocol::StreamMessage;
use crate::security::e2ee_enabled;
use serde_json::Value;
use std::sync::Arc;

pub async fn run_inference_sidecar(
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
) -> anyhow::Result<()> {
    let state = proxy_state.clone();
    let server = InferenceIpcServer::new(move |msg| {
        let state = state.clone();
        async move { handle_sidecar_message(state, msg).await }
    });
    server.run().await
}

pub async fn handle_sidecar_message(
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    msg: StreamMessage,
) -> StreamMessage {
    match msg {
        StreamMessage::EncryptedProxyRequest {
            req_id,
            path,
            room_id,
            consumer_ephemeral_pk,
            nonce,
            ciphertext,
            aad_version,
            auth,
            ..
        } => {
            if let Some(err) = verify_inbound_auth(&proxy_state, &auth, &room_id).await {
                return StreamMessage::EncryptedProxyResponse {
                    req_id,
                    nonce: vec![],
                    ciphertext: vec![],
                    aad_version,
                    error: Some(err),
                    stream: None,
                };
            }
            let body = match decrypt_inbound_request(
                &proxy_state,
                &room_id,
                &req_id,
                &path,
                &consumer_ephemeral_pk,
                &nonce,
                &ciphertext,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    return StreamMessage::EncryptedProxyResponse {
                        req_id,
                        nonce: vec![],
                        ciphertext: vec![],
                        aad_version,
                        error: Some(e.to_string()),
                        stream: None,
                    };
                }
            };
            let model = body
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            match proxy_state.forward_chat_stream(&path, &body, &model).await {
                Ok(mut stream) => {
                    use futures_util::StreamExt;
                    let mut response_buffer = String::new();
                    while let Some(Ok(chunk_bytes)) = stream.next().await {
                        if let Ok(chunk_str) = String::from_utf8(chunk_bytes.to_vec()) {
                            response_buffer.push_str(&chunk_str);
                        }
                    }
                    match encrypt_outbound_response(
                        &proxy_state,
                        &room_id,
                        &req_id,
                        &path,
                        &consumer_ephemeral_pk,
                        response_buffer.as_bytes(),
                    )
                    .await
                    {
                        Ok((nonce, ciphertext)) => StreamMessage::EncryptedProxyResponse {
                            req_id,
                            nonce,
                            ciphertext,
                            aad_version,
                            error: None,
                            stream: None,
                        },
                        Err(e) => StreamMessage::EncryptedProxyResponse {
                            req_id,
                            nonce: vec![],
                            ciphertext: vec![],
                            aad_version,
                            error: Some(e.to_string()),
                            stream: None,
                        },
                    }
                }
                Err(e) => StreamMessage::EncryptedProxyResponse {
                    req_id,
                    nonce: vec![],
                    ciphertext: vec![],
                    aad_version,
                    error: Some(format!("Upstream error: {e}")),
                    stream: None,
                },
            }
        }
        other => other,
    }
}

async fn verify_inbound_auth(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    auth: &Option<crate::security::ProxyAuthProof>,
    _room_id: &str,
) -> Option<String> {
    let proof = auth.as_ref()?;
    let cfg = proxy_state.client_config.read().await;
    let token = cfg
        .swarms
        .iter()
        .find_map(|s| {
            if crate::security::verify_proxy_auth(proof, &s.p2p_token, &proof.peer_id) {
                Some(s.p2p_token.clone())
            } else {
                None
            }
        })
        .or_else(|| {
            cfg.proxy_token.as_ref().and_then(|t| {
                if crate::security::verify_proxy_auth(proof, t, &proof.peer_id) {
                    Some(t.clone())
                } else {
                    None
                }
            })
        });
    if token.is_some() {
        None
    } else {
        Some("Proxy auth failed".to_string())
    }
}

async fn room_secret_for_id(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    room_id: &str,
) -> anyhow::Result<String> {
    let cfg = proxy_state.client_config.read().await;
    crate::proxy_e2ee::room_secret_for_proxy(&cfg, room_id)
        .ok_or_else(|| anyhow::anyhow!("no room secret for {room_id}"))
}

pub(crate) async fn decrypt_inbound_request(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    room_id: &str,
    req_id: &str,
    path: &str,
    consumer_ephemeral_pk: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> anyhow::Result<Value> {
    if consumer_ephemeral_pk.len() != 32 {
        anyhow::bail!("invalid ephemeral public key");
    }
    let room_secret = room_secret_for_id(proxy_state, room_id).await?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let static_keys =
        load_room_static_keypair(&proxy_state.tx_store, room_id, &room_root)?;
    let mut pk = [0u8; 32];
    pk.copy_from_slice(consumer_ephemeral_pk);
    let session_key = derive_session_key(&static_keys.secret_key, &pk, req_id, room_id);
    let payload = EncryptedPayload {
        nonce: nonce.to_vec(),
        ciphertext: ciphertext.to_vec(),
    };
    let plain = decrypt_payload(&session_key, req_id, path, room_id, &payload)?;
    Ok(serde_json::from_slice(&plain)?)
}

pub(crate) async fn decrypt_inbound_request_chunk(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    room_id: &str,
    req_id: &str,
    path: &str,
    seq: u32,
    consumer_ephemeral_pk: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> anyhow::Result<String> {
    if consumer_ephemeral_pk.len() != 32 {
        anyhow::bail!("invalid ephemeral public key");
    }
    let room_secret = room_secret_for_id(proxy_state, room_id).await?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let static_keys =
        load_room_static_keypair(&proxy_state.tx_store, room_id, &room_root)?;
    let mut pk = [0u8; 32];
    pk.copy_from_slice(consumer_ephemeral_pk);
    let session_key = derive_session_key(&static_keys.secret_key, &pk, req_id, room_id);
    let chunk_aad_path = crate::proxy_e2ee::chunk_path(path, seq);
    let payload = EncryptedPayload {
        nonce: nonce.to_vec(),
        ciphertext: ciphertext.to_vec(),
    };
    let plain = decrypt_payload(&session_key, req_id, &chunk_aad_path, room_id, &payload)?;
    Ok(String::from_utf8(plain)?)
}

pub async fn encrypt_outbound_response(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    room_id: &str,
    req_id: &str,
    path: &str,
    consumer_ephemeral_pk: &[u8],
    plaintext: &[u8],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let mut pk = [0u8; 32];
    pk.copy_from_slice(consumer_ephemeral_pk);
    let room_secret = room_secret_for_id(proxy_state, room_id).await?;
    let room_root = derive_room_root_key(&room_secret, room_id);
    let static_keys =
        load_room_static_keypair(&proxy_state.tx_store, room_id, &room_root)?;
    let session_key = derive_session_key(&static_keys.secret_key, &pk, req_id, room_id);
    let enc = encrypt_payload(&session_key, req_id, path, room_id, plaintext)?;
    Ok((enc.nonce, enc.ciphertext))
}

pub fn should_use_sidecar() -> bool {
    crate::security::inference_sidecar_enabled()
}

pub fn should_use_e2ee() -> bool {
    e2ee_enabled()
}
