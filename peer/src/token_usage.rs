use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::tx_db::TxStore;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Lobby WebSocket write half used for `reporttokenusage` settlement messages.
pub type LobbyWsWrite = Arc<
    Mutex<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    >,
>;

/// Wire shape matching `ProtocolMessage::ReportTokenUsage` (`type: reporttokenusage`).
#[derive(Debug, Clone, Serialize)]
struct ReportTokenUsageWire {
    #[serde(rename = "type")]
    msg_type: &'static str,
    req_id: String,
    role: String,
    peer_id: String,
    remote_peer_id: String,
    model: String,
    path: String,
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
    bytes_sent: u64,
    bytes_received: u64,
    duration_ms: u64,
}

/// Record a local Activity row and send `reporttokenusage` to the lobby.
pub fn publish_token_usage_report(
    tx_store: Arc<TxStore>,
    ws_write: LobbyWsWrite,
    req_id: String,
    role: &str,
    peer_id: String,
    remote_peer_id: String,
    model: String,
    path: String,
    usage: TokenUsage,
    bytes_sent: u64,
    bytes_received: u64,
    duration_ms: u64,
) {
    let role = role.to_string();
    tokio::spawn(async move {
        if let Err(e) = tx_store
            .record_local_report(
                req_id.clone(),
                &role,
                &peer_id,
                remote_peer_id.clone(),
                model.clone(),
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens,
            )
            .await
        {
            eprintln!("tx_db record error: {}", e);
        }

        let report = ReportTokenUsageWire {
            msg_type: "reporttokenusage",
            req_id,
            role,
            peer_id,
            remote_peer_id,
            model,
            path,
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
            bytes_sent,
            bytes_received,
            duration_ms,
        };
        if let Ok(text) = serde_json::to_string(&report) {
            let _ = ws_write.lock().await.send(Message::Text(text)).await;
        }
    });
}

pub fn accumulate_and_parse_usage(buffer: &mut String, chunk: &str) -> Option<TokenUsage> {
    buffer.push_str(chunk);

    let mut latest: Option<TokenUsage> = None;
    for line in buffer.lines() {
        if let Some(value) = parse_stream_line(line) {
            if let Some(usage) = parse_usage_from_value(&value) {
                latest = Some(usage);
            }
        }
    }
    latest
}

pub fn parse_usage_from_buffer(buffer: &str) -> Option<TokenUsage> {
    let mut latest: Option<TokenUsage> = None;
    for line in buffer.lines() {
        if let Some(value) = parse_stream_line(line) {
            if let Some(usage) = parse_usage_from_value(&value) {
                latest = Some(usage);
            }
        }
    }
    latest
}

fn parse_stream_line(line: &str) -> Option<Value> {
    let line = line.trim();
    if line.is_empty() || line == "[DONE]" || line == "data: [DONE]" {
        return None;
    }
    let json_str = line.strip_prefix("data: ").unwrap_or(line);
    serde_json::from_str(json_str).ok()
}

fn parse_usage_from_value(value: &Value) -> Option<TokenUsage> {
    if let Some(usage) = value.get("usage") {
        let prompt = usage
            .get("prompt_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let completion = usage
            .get("completion_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let total = usage
            .get("total_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(prompt as u64 + completion as u64) as u32;
        if prompt > 0 || completion > 0 || total > 0 {
            return Some(TokenUsage {
                prompt_tokens: prompt,
                completion_tokens: completion,
                total_tokens: total,
            });
        }
    }

    let done = value
        .get("done")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !done {
        return None;
    }

    let prompt = value
        .get("prompt_eval_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let completion = value
        .get("eval_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;

    if prompt == 0 && completion == 0 {
        return None;
    }

    Some(TokenUsage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: prompt + completion,
    })
}

/// Metadata-safe credit report for lobby sync (no request path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactedCreditReport {
    pub req_id: String,
    pub model: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub role: String,
}

pub fn redacted_credit_report(
    req_id: &str,
    model: &str,
    usage: &TokenUsage,
    role: &str,
) -> RedactedCreditReport {
    RedactedCreditReport {
        req_id: req_id.to_string(),
        model: model.to_string(),
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
        role: role.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ollama_done_line() {
        let line = r#"{"model":"llama3","done":true,"prompt_eval_count":10,"eval_count":25}"#;
        let usage = parse_usage_from_buffer(line).unwrap();
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 25);
        assert_eq!(usage.total_tokens, 35);
    }

    #[test]
    fn parses_openai_usage() {
        let line = r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":15,"total_tokens":20}}"#;
        let usage = parse_usage_from_buffer(line).unwrap();
        assert_eq!(usage.total_tokens, 20);
    }

    #[test]
    fn parses_openai_sse_usage_line() {
        let line = r#"data: {"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}"#;
        let usage = parse_usage_from_buffer(line).unwrap();
        assert_eq!(usage.prompt_tokens, 12);
        assert_eq!(usage.completion_tokens, 8);
        assert_eq!(usage.total_tokens, 20);
    }

    #[test]
    fn report_wire_type_is_lowercase() {
        let report = ReportTokenUsageWire {
            msg_type: "reporttokenusage",
            req_id: "r1".into(),
            role: "provider".into(),
            peer_id: "p1".into(),
            remote_peer_id: "c1".into(),
            model: "m".into(),
            path: "/api/chat".into(),
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
            bytes_sent: 4,
            bytes_received: 5,
            duration_ms: 6,
        };
        let v: Value = serde_json::to_value(&report).unwrap();
        assert_eq!(v["type"], "reporttokenusage");
        assert_eq!(v["total_tokens"], 3);
    }
}
