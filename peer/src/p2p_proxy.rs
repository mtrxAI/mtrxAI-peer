//! Swarm-mode proxy request handling over libp2p request-response.

use crate::agent_compat::{
    ensure_ollama_ctx_options_with_default, ensure_ollama_predict_options_with_default,
    ensure_stream_usage,
};
use crate::client_config::{
    effective_default_num_ctx, effective_default_num_predict, find_swarm, swarm_accepts_jobs,
};
use crate::llm_proxy::ProxyState;
use crate::p2p_protocol::StreamMessage;
use crate::token_usage::{accumulate_and_parse_usage, parse_usage_from_buffer, TokenUsage};
use futures_util::StreamExt;
use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type RrResponseTx = mpsc::Sender<(ResponseChannel<StreamMessage>, StreamMessage)>;
pub type ReverseProxyTx = mpsc::Sender<(PeerId, StreamMessage)>;

const STREAM_CHUNK_BYTES: usize = 3000;

#[derive(Clone, Debug)]
pub struct EncryptedProxySlot {
    pub req_id: String,
    pub generation: u64,
}

async fn claim_encrypted_proxy_slot(
    slots: &Arc<Mutex<HashMap<PeerId, EncryptedProxySlot>>>,
    consumer: PeerId,
    req_id: &str,
) -> u64 {
    let mut slots = slots.lock().await;
    let slot = slots.entry(consumer).or_insert(EncryptedProxySlot {
        req_id: String::new(),
        generation: 0,
    });
    slot.generation += 1;
    slot.req_id = req_id.to_string();
    slot.generation
}

async fn encrypted_proxy_still_active(
    slots: &Arc<Mutex<HashMap<PeerId, EncryptedProxySlot>>>,
    consumer: PeerId,
    req_id: &str,
    generation: u64,
) -> bool {
    slots
        .lock()
        .await
        .get(&consumer)
        .map(|s| s.req_id == req_id && s.generation == generation)
        .unwrap_or(false)
}

pub async fn dispatch_swarm_proxy_request(
    req_id: String,
    path: String,
    mut body: serde_json::Value,
    consumer_peer_id: String,
    swarm_id: String,
    proxy_state: Arc<ProxyState>,
    rr_tx: RrResponseTx,
    response_channel: ResponseChannel<StreamMessage>,
) {
    tokio::spawn(async move {
        let req_log = req_id.clone();
        let respond = |msg: StreamMessage| async {
            if rr_tx.send((response_channel, msg)).await.is_err() {
                eprintln!("Swarm proxy: failed to queue libp2p response for {req_log}");
            }
        };

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
            proxy_state.stats_request_error(&req_id);
            let _ = respond(StreamMessage::ProxyResponse {
                req_id,
                body: String::new(),
                error: Some("Peer is blocked".to_string()),
            })
            .await;
            return;
        }

        let accepting = {
            let cfg = proxy_state.client_config.read().await;
            find_swarm(&cfg, &swarm_id)
                .map(swarm_accepts_jobs)
                .unwrap_or(true)
        };
        if !accepting {
            proxy_state.stats_request_error(&req_id);
            let _ = respond(StreamMessage::ProxyResponse {
                req_id,
                body: String::new(),
                error: Some("Peer is in maintenance mode".to_string()),
            })
            .await;
            return;
        }

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

        match proxy_state.forward_chat_stream(&path, &body, &model).await {
            Ok(mut stream) => {
                let mut response_buffer = String::new();
                while let Some(Ok(chunk_bytes)) = stream.next().await {
                    if let Ok(chunk_str) = String::from_utf8(chunk_bytes.to_vec()) {
                        let partial_tokens =
                            accumulate_and_parse_usage(&mut response_buffer, &chunk_str)
                                .map(|u| u.total_tokens)
                                .unwrap_or(0);
                        proxy_state.stats_stream_progress(
                            &req_id,
                            chunk_str.len() as u64,
                            partial_tokens,
                        );
                    }
                }
                let usage = parse_usage_from_buffer(&response_buffer).unwrap_or(TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                });
                proxy_state.stats_request_complete(
                    &req_id,
                    usage.total_tokens,
                    response_buffer.len() as u64,
                );

                let local_peer_id = proxy_state.shared_state.lock().await.peer_id.clone();
                let _ = proxy_state
                    .tx_store
                    .record_local_report(
                        req_id.clone(),
                        "provider",
                        &local_peer_id,
                        consumer_peer_id.clone(),
                        model.clone(),
                        usage.prompt_tokens,
                        usage.completion_tokens,
                        usage.total_tokens,
                    )
                    .await;

                let _ = respond(StreamMessage::ProxyResponse {
                    req_id,
                    body: response_buffer,
                    error: None,
                })
                .await;
            }
            Err(e) => {
                proxy_state.stats_request_error(&req_id);
                let _ = respond(StreamMessage::ProxyResponse {
                    req_id,
                    body: String::new(),
                    error: Some(format!("Upstream error: {e}")),
                })
                .await;
            }
        }
    });
}

pub async fn dispatch_swarm_encrypted_proxy_request(
    req_id: String,
    path: String,
    room_id: String,
    consumer_ephemeral_pk: Vec<u8>,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    aad_version: u8,
    consumer_peer_id: String,
    consumer_libp2p: PeerId,
    _swarm_id: String,
    proxy_state: Arc<ProxyState>,
    rr_tx: RrResponseTx,
    reverse_tx: ReverseProxyTx,
    pending_reverse_proxies: Arc<Mutex<HashMap<PeerId, Vec<StreamMessage>>>>,
    active_encrypted_proxies: Arc<Mutex<HashMap<PeerId, EncryptedProxySlot>>>,
    response_channel: ResponseChannel<StreamMessage>,
) {
    if crate::inference_sidecar::should_use_sidecar() {
        let msg = StreamMessage::EncryptedProxyRequest {
            req_id: req_id.clone(),
            path: path.clone(),
            room_id: room_id.clone(),
            consumer_ephemeral_pk: consumer_ephemeral_pk.clone(),
            nonce,
            ciphertext,
            aad_version,
            auth: None,
        };
        tokio::spawn(async move {
            let req_log = req_id.clone();
            let respond = |out: StreamMessage| async {
                if rr_tx.send((response_channel, out)).await.is_err() {
                    eprintln!(
                        "Swarm encrypted proxy: failed to queue libp2p response for {req_log}"
                    );
                }
            };
            match crate::inference_ipc::InferenceIpcClient::forward_request(
                req_id.clone(),
                path.clone(),
                serde_json::Value::Null,
                room_id.clone(),
                consumer_peer_id.clone(),
                msg,
            )
            .await
            {
                Ok(resp) => {
                    let _ = respond(resp).await;
                }
                Err(e) => {
                    let _ = respond(StreamMessage::EncryptedProxyResponse {
                        req_id,
                        nonce: vec![],
                        ciphertext: vec![],
                        aad_version,
                        error: Some(format!("Sidecar IPC error: {e}")),
                        stream: None,
                    })
                    .await;
                }
            }
        });
        return;
    }

    tokio::spawn(async move {
        let generation =
            claim_encrypted_proxy_slot(&active_encrypted_proxies, consumer_libp2p, &req_id).await;
        pending_reverse_proxies
            .lock()
            .await
            .remove(&consumer_libp2p);

        let req_log = req_id.clone();
        let respond = |out: StreamMessage| async {
            if !encrypted_proxy_still_active(
                &active_encrypted_proxies,
                consumer_libp2p,
                &req_id,
                generation,
            )
            .await
            {
                return;
            }
            if rr_tx.send((response_channel, out)).await.is_err() {
                eprintln!("Swarm encrypted proxy: failed to queue libp2p response for {req_log}");
            }
        };

        if proxy_state
            .tx_store
            .is_peer_blocked(&consumer_peer_id)
            .await
            .unwrap_or(false)
        {
            let _ = respond(StreamMessage::EncryptedProxyResponse {
                req_id: req_log.clone(),
                nonce: vec![],
                ciphertext: vec![],
                aad_version,
                error: Some("Peer is blocked".to_string()),
                stream: None,
            })
            .await;
            return;
        }

        let decrypted = match crate::inference_sidecar::decrypt_inbound_request(
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
                let _ = respond(StreamMessage::EncryptedProxyResponse {
                    req_id: req_id.clone(),
                    nonce: vec![],
                    ciphertext: vec![],
                    aad_version,
                    error: Some(e.to_string()),
                    stream: None,
                })
                .await;
                proxy_state.stats_request_error(&req_id);
                return;
            }
        };

        // Acknowledge the libp2p request immediately; body follows as encrypted reverse chunks.
        let _ = respond(StreamMessage::EncryptedProxyResponse {
            req_id: req_id.clone(),
            nonce: vec![],
            ciphertext: vec![],
            aad_version,
            error: None,
            stream: Some(true),
        })
        .await;
        println!("🔐 Encrypted proxy {req_id}: streaming response to {consumer_peer_id}");

        let mut body = decrypted;
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

        let model = body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        let request_bytes = serde_json::to_string(&body).unwrap_or_default().len() as u64;
        proxy_state.stats_request_start(&req_id, &consumer_peer_id, &model, request_bytes);

        let cfg = proxy_state.client_config.read().await;
        match proxy_state.forward_chat_stream(&path, &body, &model).await {
            Ok(mut stream) => {
                let mut response_buffer = String::new();
                let mut pending_send = String::new();
                let mut seq = 0u32;
                let mut chunks_queued = 0usize;

                while let Some(item) = stream.next().await {
                    if !encrypted_proxy_still_active(
                        &active_encrypted_proxies,
                        consumer_libp2p,
                        &req_id,
                        generation,
                    )
                    .await
                    {
                        eprintln!(
                            "Encrypted proxy {req_id}: superseded by newer request, stopping stream"
                        );
                        break;
                    }
                    match item {
                        Ok(chunk_bytes) => {
                            if let Ok(chunk_str) = String::from_utf8(chunk_bytes.to_vec()) {
                                response_buffer.push_str(&chunk_str);
                                pending_send.push_str(&chunk_str);
                                let partial_tokens =
                                    accumulate_and_parse_usage(&mut response_buffer, &chunk_str)
                                        .map(|u| u.total_tokens)
                                        .unwrap_or(0);
                                proxy_state.stats_stream_progress(
                                    &req_id,
                                    chunk_str.len() as u64,
                                    partial_tokens,
                                );
                                flush_encrypted_pending(
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
                                    &mut chunks_queued,
                                    &reverse_tx,
                                    consumer_libp2p,
                                    &active_encrypted_proxies,
                                    generation,
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            proxy_state.stats_request_error(&req_id);
                            eprintln!("Encrypted proxy {req_id}: Ollama stream error: {e}");
                            break;
                        }
                    }
                }

                flush_encrypted_pending(
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
                    &mut chunks_queued,
                    &reverse_tx,
                    consumer_libp2p,
                    &active_encrypted_proxies,
                    generation,
                )
                .await;

                let usage = parse_usage_from_buffer(&response_buffer).unwrap_or(TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                });
                proxy_state.stats_request_complete(
                    &req_id,
                    usage.total_tokens,
                    response_buffer.len() as u64,
                );

                println!(
                    "✅ Encrypted proxy {req_id}: queued {chunks_queued} chunk(s) ({} bytes)",
                    response_buffer.len()
                );
            }
            Err(e) => {
                proxy_state.stats_request_error(&req_id);
                eprintln!("Encrypted proxy {req_id}: upstream error: {e}");
            }
        }
    });
}

fn chunk_response_text(text: &str, max_bytes: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in text.split_inclusive('\n') {
        if !current.is_empty() && current.len() + line.len() > max_bytes {
            chunks.push(std::mem::take(&mut current));
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

async fn flush_encrypted_pending(
    proxy_state: &Arc<ProxyState>,
    cfg: &crate::client_config::ClientConfig,
    room_id: &str,
    req_id: &str,
    path: &str,
    consumer_ephemeral_pk: &[u8],
    aad_version: u8,
    pending: &mut String,
    done: bool,
    seq: &mut u32,
    chunks_queued: &mut usize,
    reverse_tx: &ReverseProxyTx,
    consumer_libp2p: PeerId,
    active_encrypted_proxies: &Arc<Mutex<HashMap<PeerId, EncryptedProxySlot>>>,
    generation: u64,
) {
    loop {
        if !encrypted_proxy_still_active(
            active_encrypted_proxies,
            consumer_libp2p,
            req_id,
            generation,
        )
        .await
        {
            break;
        }
        if pending.is_empty() {
            break;
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
                if reverse_tx
                    .send((
                        consumer_libp2p,
                        StreamMessage::EncryptedProxyStreamChunk {
                            req_id: req_id.to_string(),
                            seq: *seq,
                            nonce: enc_nonce,
                            ciphertext: enc_ct,
                            aad_version,
                            done: is_done,
                        },
                    ))
                    .await
                    .is_err()
                {
                    eprintln!("Encrypted proxy {req_id}: failed to queue chunk {seq}");
                }
                *chunks_queued += 1;
                *seq += 1;
            }
            Err(e) => {
                proxy_state.stats_request_error(req_id);
                eprintln!("Encrypted proxy {req_id}: chunk {seq} encrypt failed: {e}");
                break;
            }
        }
    }
}
