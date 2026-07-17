pub fn env_truthy(name: &str) -> bool {
    std::env::var(name)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn e2ee_enabled() -> bool {
    std::env::var("MTRXAI_E2EE_ENABLED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true)
}

pub fn require_tee() -> bool {
    env_truthy("MTRXAI_REQUIRE_TEE")
}

pub fn inference_sidecar_enabled() -> bool {
    std::env::var("MTRXAI_INFERENCE_SIDECAR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn redact_logs() -> bool {
    env_truthy("MTRXAI_REDACT_LOGS")
}

pub fn proxy_bind_host() -> String {
    std::env::var("MTRXAI_PROXY_BIND")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

pub fn inference_ipc_socket_path() -> Option<String> {
    std::env::var("MTRXAI_INFERENCE_IPC_SOCKET")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

pub fn inference_ipc_host() -> String {
    std::env::var("MTRXAI_INFERENCE_IPC_HOST")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

pub fn inference_ipc_bind_host() -> String {
    "127.0.0.1".to_string()
}
