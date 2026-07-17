//! WebRTC data-channel E2EE for cluster proxy (mirrors swarm libp2p v2).

use crate::crypto::generate_ephemeral_keypair;
use crate::p2p_protocol::StreamMessage;
use crate::proxy_e2ee::{
    decrypt_proxy_response, encrypt_proxy_request, encrypt_proxy_request_chunk,
    resolve_provider_static_public,
};
use crate::security::{e2ee_enabled, verify_proxy_auth, ProxyAuthProof};
use anyhow::Result;
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use webrtc::data_channel::RTCDataChannel;

pub const MAX_DC_WIRE_BYTES: usize = 8 * 1024;
/// Plaintext JSON bytes per encrypted chunk; JSON byte arrays expand ~4x on the wire.
const MAX_E2EE_PLAINTEXT_CHUNK_BYTES: usize = 1024;
const STREAM_CHUNK_BYTES: usize = 1024;

#[derive(Clone, Debug)]
pub struct EncryptedConsumerState {
    pub ephemeral_secret: [u8; 32],
    pub path: String,
    pub room_id: String,
    pub provider_pk: [u8; 32],
    pub aad_version: u8,
}

#[derive(Clone, Debug)]
pub struct PendingEncryptedIncomingRequest {
    pub path: String,
    pub room_id: String,
    pub consumer_ephemeral_pk: Vec<u8>,
    pub aad_version: u8,
    pub auth: Option<ProxyAuthProof>,
    pub chunks: Vec<(u32, Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug)]
struct EncryptedStreamChunkData {
    seq: u32,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    done: bool,
}

#[derive(Clone, Debug)]
pub struct PendingEncryptedStream {
    pub path: String,
    pub room_id: String,
    pub consumer_ephemeral_secret: [u8; 32],
    pub provider_pk: [u8; 32],
    pub aad_version: u8,
    next_seq: u32,
    reorder: BTreeMap<u32, EncryptedStreamChunkData>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EncryptedStreamChunkOutcome {
    Waiting,
    Progress { parts: Vec<String> },
    Finished { parts: Vec<String> },
}

fn chunk_utf8(s: &str, max_bytes: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < s.len() {
        let mut end = (start + max_bytes).min(s.len());
        while end > start && !s.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = (start + 1).min(s.len());
            while end < s.len() && !s.is_char_boundary(end) {
                end += 1;
            }
        }
        out.push(s[start..end].to_string());
        start = end;
    }
    out
}

fn wire_fits(msg: &StreamMessage) -> bool {
    serde_json::to_string(msg)
        .map(|s| s.len() <= MAX_DC_WIRE_BYTES)
        .unwrap_or(false)
}

pub async fn send_stream_message(dc: &RTCDataChannel, msg: &StreamMessage) -> Result<()> {
    let text = serde_json::to_string(msg)?;
    if text.len() > MAX_DC_WIRE_BYTES {
        anyhow::bail!(
            "WebRTC E2EE message is {} bytes (limit {})",
            text.len(),
            MAX_DC_WIRE_BYTES
        );
    }
    dc.send_text(text).await?;
    Ok(())
}

pub async fn send_encrypted_proxy_request(
    dc: &RTCDataChannel,
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    peer_id: &str,
    req_id: &str,
    path: &str,
    body: &Value,
    room_id: &str,
    catalog_pk: Option<&str>,
) -> Result<EncryptedConsumerState> {
    let cfg = proxy_state.client_config.read().await;
    let provider_pk = resolve_provider_static_public(
        &proxy_state.tx_store,
        &cfg,
        room_id,
        catalog_pk,
    )?;
    let (stream_msg, ephemeral_secret) = encrypt_proxy_request(
        &proxy_state.tx_store,
        &cfg,
        peer_id,
        req_id,
        path,
        body,
        room_id,
        &provider_pk,
    )
    .await?;
    let aad_version = crate::crypto::envelope::AAD_VERSION;

    if wire_fits(&stream_msg) {
        let wire_len = serde_json::to_string(&stream_msg)?.len();
        crate::security::log_redact::proxy_request_wire(req_id, wire_len, 1, MAX_DC_WIRE_BYTES);
        send_stream_message(dc, &stream_msg).await?;
        let StreamMessage::EncryptedProxyRequest {
            path, room_id, ..
        } = &stream_msg
        else {
            anyhow::bail!("encrypt_proxy_request returned unexpected message");
        };
        return Ok(EncryptedConsumerState {
            ephemeral_secret,
            path: path.clone(),
            room_id: room_id.clone(),
            provider_pk,
            aad_version,
        });
    }

    let body_str = serde_json::to_string(body)?;
    let plaintext_chunks = chunk_utf8(&body_str, MAX_E2EE_PLAINTEXT_CHUNK_BYTES);
    let ephemeral = generate_ephemeral_keypair();
    let ephemeral_secret = ephemeral.secret().to_bytes();
    let consumer_ephemeral_pk = ephemeral.public_key.to_vec();
    let auth = crate::proxy_e2ee::proxy_auth_for_room(&cfg, peer_id, room_id);

    let start_msg = StreamMessage::EncryptedProxyRequestStart {
        req_id: req_id.to_string(),
        path: path.to_string(),
        room_id: room_id.to_string(),
        consumer_ephemeral_pk: consumer_ephemeral_pk.clone(),
        aad_version,
        auth,
    };
    send_stream_message(dc, &start_msg).await?;

    for (seq, chunk) in plaintext_chunks.iter().enumerate() {
        let (nonce, ciphertext) = encrypt_proxy_request_chunk(
            &ephemeral_secret,
            &provider_pk,
            room_id,
            req_id,
            path,
            seq as u32,
            chunk.as_bytes(),
        )?;
        let chunk_msg = StreamMessage::EncryptedProxyRequestChunk {
            req_id: req_id.to_string(),
            seq: seq as u32,
            nonce,
            ciphertext,
            aad_version,
        };
        send_stream_message(dc, &chunk_msg).await?;
    }

    send_stream_message(
        dc,
        &StreamMessage::EncryptedProxyRequestEnd {
            req_id: req_id.to_string(),
        },
    )
    .await?;

    crate::security::log_redact::proxy_request_wire(
        req_id,
        body_str.len(),
        plaintext_chunks.len(),
        MAX_E2EE_PLAINTEXT_CHUNK_BYTES,
    );

    Ok(EncryptedConsumerState {
        ephemeral_secret,
        path: path.to_string(),
        room_id: room_id.to_string(),
        provider_pk,
        aad_version,
    })
}

pub async fn verify_encrypted_request_auth_async(
    auth: &Option<ProxyAuthProof>,
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    room_id: &str,
) -> Option<String> {
    let auth = auth.as_ref()?;
    let cfg = proxy_state.client_config.read().await;
    let secret = crate::proxy_e2ee::room_secret_for_proxy(&cfg, room_id)?;
    if verify_proxy_auth(auth, &secret, &auth.peer_id) {
        None
    } else {
        Some("Proxy auth failed".to_string())
    }
}

pub async fn decrypt_encrypted_response(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    state: &EncryptedConsumerState,
    req_id: &str,
    msg: &StreamMessage,
) -> Result<String> {
    let StreamMessage::EncryptedProxyResponse { error, .. } = msg else {
        anyhow::bail!("expected encrypted proxy response");
    };
    if let Some(err) = error {
        anyhow::bail!("{err}");
    }
    decrypt_proxy_response(
        &proxy_state.tx_store,
        &*proxy_state.client_config.read().await,
        req_id,
        &state.path,
        &state.room_id,
        &state.ephemeral_secret,
        &state.provider_pk,
        msg,
    )
    .await
}

pub fn should_use_cluster_e2ee() -> bool {
    e2ee_enabled()
}

pub async fn handle_encrypted_request_start(
    pending: &Arc<Mutex<HashMap<String, PendingEncryptedIncomingRequest>>>,
    req_id: String,
    path: String,
    room_id: String,
    consumer_ephemeral_pk: Vec<u8>,
    aad_version: u8,
    auth: Option<ProxyAuthProof>,
) {
    pending.lock().await.insert(
        req_id,
        PendingEncryptedIncomingRequest {
            path,
            room_id,
            consumer_ephemeral_pk,
            aad_version,
            auth,
            chunks: Vec::new(),
        },
    );
}

pub async fn handle_encrypted_request_chunk(
    pending: &Arc<Mutex<HashMap<String, PendingEncryptedIncomingRequest>>>,
    req_id: &str,
    seq: u32,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
) {
    if let Some(entry) = pending.lock().await.get_mut(req_id) {
        entry.chunks.push((seq, nonce, ciphertext));
    }
}

pub async fn handle_encrypted_request_end(
    pending: &Arc<Mutex<HashMap<String, PendingEncryptedIncomingRequest>>>,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    dc: Arc<RTCDataChannel>,
    consumer_peer_id: String,
    local_peer_id: String,
    ws_write: crate::token_usage::LobbyWsWrite,
    cluster_id: String,
    approved_proxy_models: Option<Arc<Mutex<HashMap<String, String>>>>,
    req_id: String,
) {
    let Some(mut incoming) = pending.lock().await.remove(&req_id) else {
        return;
    };
    incoming.chunks.sort_by_key(|(seq, _, _)| *seq);
    let mut body_str = String::new();
    for (seq, nonce, ciphertext) in incoming.chunks {
        match crate::inference_sidecar::decrypt_inbound_request_chunk(
            &proxy_state,
            &incoming.room_id,
            &req_id,
            &incoming.path,
            seq,
            &incoming.consumer_ephemeral_pk,
            &nonce,
            &ciphertext,
        )
        .await
        {
            Ok(part) => body_str.push_str(&part),
            Err(e) => {
                let err_msg = StreamMessage::EncryptedProxyResponse {
                    req_id: req_id.clone(),
                    nonce: vec![],
                    ciphertext: vec![],
                    aad_version: incoming.aad_version,
                    error: Some(e.to_string()),
                    stream: None,
                };
                let _ = send_stream_message(&dc, &err_msg).await;
                proxy_state.stats_request_error(&req_id);
                return;
            }
        }
    }

    let body = match serde_json::from_str::<Value>(&body_str) {
        Ok(v) => v,
        Err(e) => {
            let err_msg = StreamMessage::EncryptedProxyResponse {
                req_id: req_id.clone(),
                nonce: vec![],
                ciphertext: vec![],
                aad_version: incoming.aad_version,
                error: Some(format!("Invalid JSON body: {e}")),
                stream: None,
            };
            let _ = send_stream_message(&dc, &err_msg).await;
            proxy_state.stats_request_error(&req_id);
            return;
        }
    };

    crate::security::log_redact::proxy_request_received(&req_id, &incoming.path, Some(body_str.len()));

    dispatch_cluster_encrypted_proxy_body(
        req_id,
        incoming.path,
        incoming.room_id,
        incoming.consumer_ephemeral_pk,
        incoming.aad_version,
        incoming.auth,
        body,
        proxy_state,
        dc,
        consumer_peer_id,
        local_peer_id,
        ws_write,
        cluster_id,
        approved_proxy_models,
    )
    .await;
}

pub async fn handle_encrypted_stream_chunk(
    pending_streams: &Arc<Mutex<HashMap<String, PendingEncryptedStream>>>,
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    req_id: &str,
    seq: u32,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    done: bool,
) -> Result<EncryptedStreamChunkOutcome, String> {
    {
        let mut streams = pending_streams.lock().await;
        let ctx = streams
            .get_mut(req_id)
            .ok_or_else(|| "missing encrypted stream state".to_string())?;
        ctx.reorder.insert(
            seq,
            EncryptedStreamChunkData {
                seq,
                nonce,
                ciphertext,
                done,
            },
        );
    }

    let mut parts = Vec::new();
    loop {
        let chunk = {
            let mut streams = pending_streams.lock().await;
            let ctx = streams
                .get_mut(req_id)
                .ok_or_else(|| "missing encrypted stream state".to_string())?;
            ctx.reorder.remove(&ctx.next_seq)
        };
        let Some(chunk) = chunk else {
            break;
        };

        let (path, room_id, secret, provider_pk, next_seq) = {
            let streams = pending_streams.lock().await;
            let ctx = streams
                .get(req_id)
                .ok_or_else(|| "missing encrypted stream state".to_string())?;
            (
                ctx.path.clone(),
                ctx.room_id.clone(),
                ctx.consumer_ephemeral_secret,
                ctx.provider_pk,
                ctx.next_seq,
            )
        };

        let cfg = proxy_state.client_config.read().await;
        let part = crate::proxy_e2ee::decrypt_proxy_chunk(
            &proxy_state.tx_store,
            &cfg,
            req_id,
            &path,
            chunk.seq,
            &room_id,
            &secret,
            &provider_pk,
            &chunk.nonce,
            &chunk.ciphertext,
        )
        .await
        .map_err(|e| e.to_string())?;

        let finished = chunk.done;
        {
            let mut streams = pending_streams.lock().await;
            let ctx = streams
                .get_mut(req_id)
                .ok_or_else(|| "missing encrypted stream state".to_string())?;
            ctx.next_seq = next_seq + 1;
            if finished {
                streams.remove(req_id);
            }
        }

        parts.push(part);
        if finished {
            return Ok(EncryptedStreamChunkOutcome::Finished { parts });
        }
    }

    if parts.is_empty() {
        Ok(EncryptedStreamChunkOutcome::Waiting)
    } else {
        Ok(EncryptedStreamChunkOutcome::Progress { parts })
    }
}

pub fn init_pending_encrypted_stream(state: &EncryptedConsumerState) -> PendingEncryptedStream {
    PendingEncryptedStream {
        path: state.path.clone(),
        room_id: state.room_id.clone(),
        consumer_ephemeral_secret: state.ephemeral_secret,
        provider_pk: state.provider_pk,
        aad_version: state.aad_version,
        next_seq: 0,
        reorder: BTreeMap::new(),
    }
}

async fn send_encrypted_stream_error(
    dc: &RTCDataChannel,
    req_id: &str,
    aad_version: u8,
    error: String,
) {
    let err_msg = StreamMessage::EncryptedProxyResponse {
        req_id: req_id.to_string(),
        nonce: vec![],
        ciphertext: vec![],
        aad_version,
        error: Some(error),
        stream: None,
    };
    let _ = send_stream_message(dc, &err_msg).await;
}

async fn flush_encrypted_response_chunks(
    proxy_state: &Arc<crate::llm_proxy::ProxyState>,
    cfg: &crate::client_config::ClientConfig,
    room_id: &str,
    req_id: &str,
    path: &str,
    consumer_ephemeral_pk: &[u8],
    aad_version: u8,
    pending: &mut String,
    done: bool,
    seq: &mut u32,
    dc: &RTCDataChannel,
) -> bool {
    loop {
        if pending.is_empty() {
            return true;
        }
        let should_take = if pending.len() >= STREAM_CHUNK_BYTES {
            let mut end = STREAM_CHUNK_BYTES.min(pending.len());
            while end > 0 && !pending.is_char_boundary(end) {
                end -= 1;
            }
            end
        } else if done {
            pending.len()
        } else {
            break;
        };
        if should_take == 0 {
            break;
        }
        let chunk: String = pending.drain(..should_take).collect();
        let is_done = done && pending.is_empty();
        match crate::proxy_e2ee::encrypt_proxy_chunk(
            &proxy_state.tx_store,
            cfg,
            room_id,
            req_id,
            path,
            *seq,
            consumer_ephemeral_pk,
            chunk.as_bytes(),
        )
        .await
        {
            Ok((enc_nonce, enc_ct)) => {
                let chunk_msg = StreamMessage::EncryptedProxyStreamChunk {
                    req_id: req_id.to_string(),
                    seq: *seq,
                    nonce: enc_nonce,
                    ciphertext: enc_ct,
                    aad_version,
                    done: is_done,
                };
                if send_stream_message(dc, &chunk_msg).await.is_err() {
                    return false;
                }
                *seq += 1;
            }
            Err(_) => return false,
        }
    }
    true
}

pub async fn dispatch_cluster_encrypted_proxy_request(
    msg: StreamMessage,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    dc: Arc<RTCDataChannel>,
    consumer_peer_id: String,
    local_peer_id: String,
    ws_write: crate::token_usage::LobbyWsWrite,
    cluster_id: String,
    approved_proxy_models: Option<Arc<Mutex<HashMap<String, String>>>>,
) {
    let StreamMessage::EncryptedProxyRequest {
        req_id,
        path,
        room_id,
        consumer_ephemeral_pk,
        nonce,
        ciphertext,
        aad_version,
        auth,
    } = msg
    else {
        return;
    };

    let body = match crate::inference_sidecar::decrypt_inbound_request(
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
            send_encrypted_stream_error(&dc, &req_id, aad_version, e.to_string()).await;
            proxy_state.stats_request_error(&req_id);
            return;
        }
    };

    dispatch_cluster_encrypted_proxy_body(
        req_id,
        path,
        room_id,
        consumer_ephemeral_pk,
        aad_version,
        auth,
        body,
        proxy_state,
        dc,
        consumer_peer_id,
        local_peer_id,
        ws_write,
        cluster_id,
        approved_proxy_models,
    )
    .await;
}

async fn dispatch_cluster_encrypted_proxy_body(
    req_id: String,
    path: String,
    room_id: String,
    consumer_ephemeral_pk: Vec<u8>,
    aad_version: u8,
    auth: Option<ProxyAuthProof>,
    body: Value,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    dc: Arc<RTCDataChannel>,
    consumer_peer_id: String,
    local_peer_id: String,
    ws_write: crate::token_usage::LobbyWsWrite,
    cluster_id: String,
    approved_proxy_models: Option<Arc<Mutex<HashMap<String, String>>>>,
) {
    use crate::agent_compat::{
        ensure_ollama_ctx_options_with_default, ensure_ollama_predict_options_with_default,
        ensure_stream_usage,
    };
    use crate::client_config::{cluster_accepts_jobs, effective_default_num_ctx, effective_default_num_predict, find_cluster};
    use crate::security::log_redact;

    if let Some(err) = verify_encrypted_request_auth_async(&auth, &proxy_state, &room_id).await {
        send_encrypted_stream_error(&dc, &req_id, aad_version, err).await;
        return;
    }

    if crate::inference_sidecar::should_use_sidecar() {
        let ipc_msg = StreamMessage::EncryptedProxyRequest {
            req_id: req_id.clone(),
            path: path.clone(),
            room_id: room_id.clone(),
            consumer_ephemeral_pk,
            nonce: vec![],
            ciphertext: vec![],
            aad_version,
            auth,
        };
        tokio::spawn(async move {
            match crate::inference_ipc::InferenceIpcClient::forward_request(
                req_id.clone(),
                path.clone(),
                body,
                room_id.clone(),
                consumer_peer_id.clone(),
                ipc_msg,
            )
            .await
            {
                Ok(resp) => {
                    let _ = send_stream_message(&dc, &resp).await;
                }
                Err(e) => {
                    send_encrypted_stream_error(&dc, &req_id, aad_version, format!("Vault IPC error: {e}"))
                        .await;
                }
            }
        });
        return;
    }

    tokio::spawn(async move {
        log_redact::proxy_request_received(&req_id, &path, None);

        let request_bytes = serde_json::to_string(&body).unwrap_or_default().len() as u64;
        let model = body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        proxy_state.stats_request_start(&req_id, &consumer_peer_id, &model, request_bytes);

        if proxy_state
            .tx_store
            .is_peer_blocked(&consumer_peer_id)
            .await
            .unwrap_or(false)
        {
            send_encrypted_stream_error(&dc, &req_id, aad_version, "Peer is blocked".to_string())
                .await;
            proxy_state.stats_request_error(&req_id);
            return;
        }

        let accepting = {
            let cfg = proxy_state.client_config.read().await;
            find_cluster(&cfg, &cluster_id)
                .map(cluster_accepts_jobs)
                .unwrap_or(true)
        };
        if !accepting {
            send_encrypted_stream_error(
                &dc,
                &req_id,
                aad_version,
                "Peer is in maintenance mode".to_string(),
            )
            .await;
            proxy_state.stats_request_error(&req_id);
            return;
        }

        if let Some(models) = approved_proxy_models.as_ref() {
            if let Some(expected) = models.lock().await.remove(&consumer_peer_id) {
                if expected != model {
                    send_encrypted_stream_error(
                        &dc,
                        &req_id,
                        aad_version,
                        "Model does not match approved connection".to_string(),
                    )
                    .await;
                    proxy_state.stats_request_error(&req_id);
                    return;
                }
            }
        }

        let started_at = std::time::Instant::now();
        let mut body = body;
        ensure_stream_usage(&mut body);
        let default_predict = {
            let cfg = proxy_state.client_config.read().await;
            effective_default_num_predict(&cfg)
        };
        let default_ctx = {
            let cfg = proxy_state.client_config.read().await;
            effective_default_num_ctx(&cfg)
        };
        ensure_ollama_predict_options_with_default(&mut body, default_predict);
        ensure_ollama_ctx_options_with_default(&mut body, default_ctx);

        println!(
            "Forwarding proxy request [{}] model={} path={} to backend...",
            req_id, model, path
        );

        let ack = StreamMessage::EncryptedProxyResponse {
            req_id: req_id.clone(),
            nonce: vec![],
            ciphertext: vec![],
            aad_version,
            error: None,
            stream: Some(true),
        };
        if send_stream_message(&dc, &ack).await.is_err() {
            proxy_state.stats_request_error(&req_id);
            return;
        }

        match proxy_state
            .forward_chat_stream(&path, &body, &model)
            .await
        {
            Ok(mut stream) => {
                let mut response_buffer = String::new();
                let mut pending_send = String::new();
                let mut seq = 0u32;
                let mut bytes_received = 0u64;
                let cfg = proxy_state.client_config.read().await;

                while let Some(Ok(chunk_bytes)) = stream.next().await {
                    if let Ok(chunk_str) = String::from_utf8(chunk_bytes.to_vec()) {
                        bytes_received += chunk_str.len() as u64;
                        let partial_tokens =
                            crate::token_usage::accumulate_and_parse_usage(
                                &mut response_buffer,
                                &chunk_str,
                            )
                            .map(|u| u.total_tokens)
                            .unwrap_or(0);
                        // accumulate_and_parse_usage already appended chunk_str to response_buffer
                        pending_send.push_str(&chunk_str);
                        proxy_state.stats_stream_progress(
                            &req_id,
                            chunk_str.len() as u64,
                            partial_tokens,
                        );
                    }
                    if !flush_encrypted_response_chunks(
                        &proxy_state,
                        &cfg,
                        &room_id,
                        &req_id,
                        &path,
                        &consumer_ephemeral_pk,
                        aad_version,
                        &mut pending_send,
                        false,
                        &mut seq,
                        &dc,
                    )
                    .await
                    {
                        proxy_state.stats_request_error(&req_id);
                        return;
                    }
                }

                if !flush_encrypted_response_chunks(
                    &proxy_state,
                    &cfg,
                    &room_id,
                    &req_id,
                    &path,
                    &consumer_ephemeral_pk,
                    aad_version,
                    &mut pending_send,
                    true,
                    &mut seq,
                    &dc,
                )
                .await
                {
                    proxy_state.stats_request_error(&req_id);
                    return;
                }

                let usage = crate::token_usage::parse_usage_from_buffer(&response_buffer)
                    .unwrap_or_default();

                proxy_state.stats_request_complete(
                    &req_id,
                    usage.total_tokens,
                    bytes_received,
                );

                crate::token_usage::publish_token_usage_report(
                    proxy_state.tx_store.clone(),
                    ws_write,
                    req_id,
                    "provider",
                    local_peer_id,
                    consumer_peer_id,
                    model,
                    path,
                    usage,
                    bytes_received,
                    request_bytes,
                    started_at.elapsed().as_millis() as u64,
                );
            }
            Err(e) => {
                send_encrypted_stream_error(
                    &dc,
                    &req_id,
                    aad_version,
                    format!("Proxy Error: {e}"),
                )
                .await;
                proxy_state.stats_request_error(&req_id);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p_protocol::StreamMessage;

    #[test]
    fn large_chat_body_requires_multiple_encrypted_chunks() {
        let body = serde_json::json!({
            "model": "gemma4:e4b",
            "messages": vec![serde_json::json!({"role": "user", "content": "x".repeat(70_000)})],
        });
        let body_str = serde_json::to_string(&body).unwrap();
        let chunks = chunk_utf8(&body_str, MAX_E2EE_PLAINTEXT_CHUNK_BYTES);
        assert!(
            chunks.len() > 1,
            "expected multiple chunks for ~70KB body, got {}",
            chunks.len()
        );
        for (seq, chunk) in chunks.iter().enumerate() {
            let msg = StreamMessage::EncryptedProxyRequestChunk {
                req_id: "peer:6".to_string(),
                seq: seq as u32,
                nonce: vec![1u8; 12],
                ciphertext: chunk.as_bytes().to_vec(),
                aad_version: 1,
            };
            let wire = serde_json::to_string(&msg).unwrap();
            assert!(
                wire.len() <= MAX_DC_WIRE_BYTES,
                "chunk {seq} wire {} exceeds {}",
                wire.len(),
                MAX_DC_WIRE_BYTES
            );
        }
    }
}
