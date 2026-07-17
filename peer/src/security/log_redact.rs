//! Redact sensitive inference payload details from operator-visible logs.

use crate::security::redact_logs;

pub fn proxy_request_received(req_id: &str, path: &str, byte_len: Option<usize>) {
    if !redact_logs() {
        if let Some(n) = byte_len {
            println!("\n📬 Proxy request received: [{req_id}] {path} ({n} bytes)");
        } else {
            println!("\n📬 Proxy request received: [{req_id}] {path}");
        }
        return;
    }
    if let Some(n) = byte_len {
        println!("\n📬 Proxy request received: [{req_id}] path={path} ({n} bytes, body redacted)");
    } else {
        println!("\n📬 Proxy request received: [{req_id}] path={path} (body redacted)");
    }
}

pub fn remote_proxy_route(model: &str, scope: &str) {
    if redact_logs() {
        println!("🔀 Remote proxy for model={model} via {scope} (payload redacted)");
    } else {
        println!("🔀 Remote proxy for '{model}' via {scope}");
    }
}

pub fn agent_remote_route(model: &str) {
    if redact_logs() {
        println!("🌐 Routing agent request to remote peer for model={model} (payload redacted)");
    } else {
        println!("🌐 Local model not found: '{model}'. Routing agent request to remote peer...");
    }
}

pub fn proxy_request_wire(req_id: &str, byte_len: usize, chunks: usize, max_chunk: usize) {
    if redact_logs() {
        println!(
            "📦 Proxy request {req_id}: {byte_len} bytes in {chunks} WebRTC chunks (payload redacted, max {max_chunk} bytes/chunk)"
        );
    } else {
        println!(
            "📦 Proxy request {req_id} is {byte_len} bytes; sending in {chunks} WebRTC chunks (max {max_chunk} bytes/chunk)"
        );
    }
}
