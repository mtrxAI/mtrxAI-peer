/// Build HTTP/WebSocket base URLs for the lobby from `MTRXAI_LOBBY_HOST`.
///
/// Host values may include an optional scheme (`https://api.example.net`) and/or port.
/// TLS is enabled when the scheme is `https`, port is `443`, or `MTRXAI_LOBBY_TLS=1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLobbyHost {
    pub host_port: String,
    pub tls: bool,
}

fn lobby_tls_env() -> bool {
    std::env::var("MTRXAI_LOBBY_TLS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn host_uses_tls_by_default(host: &str) -> bool {
    host.eq_ignore_ascii_case("mtrxai.net") || host.to_ascii_lowercase().ends_with(".mtrxai.net")
}

fn split_host_port(host_port: &str) -> (&str, Option<u16>) {
    if host_port.starts_with('[') {
        let end = host_port.find(']').unwrap_or(host_port.len());
        let host = &host_port[..=end];
        let rest = host_port.get(end + 1..).unwrap_or("");
        let port = rest.strip_prefix(':').and_then(|p| p.parse::<u16>().ok());
        return (host, port);
    }

    if let Some((host, port)) = host_port.rsplit_once(':') {
        if let Ok(port) = port.parse::<u16>() {
            if host.contains(':') && !host.starts_with('[') {
                return (host_port, None);
            }
            return (host, Some(port));
        }
    }

    (host_port, None)
}

pub fn parse_lobby_host(raw: &str) -> ParsedLobbyHost {
    let mut input = raw.trim();
    let mut tls = false;

    if let Some(rest) = input
        .strip_prefix("https://")
        .or_else(|| input.strip_prefix("HTTPS://"))
    {
        input = rest;
        tls = true;
    } else if let Some(rest) = input
        .strip_prefix("http://")
        .or_else(|| input.strip_prefix("HTTP://"))
    {
        input = rest;
    }

    input = input.trim_end_matches('/');

    if lobby_tls_env() {
        tls = true;
    }

    let (host, port) = split_host_port(input);
    if !tls && host_uses_tls_by_default(host) {
        tls = true;
    }
    let port = port.unwrap_or(if tls { 443 } else { 8080 });
    if port == 443 {
        tls = true;
    }

    let host_port = if input.contains(':') {
        format!("{host}:{port}")
    } else {
        format!("{host}:{port}")
    };

    ParsedLobbyHost { host_port, tls }
}

pub fn normalize_lobby_host(raw: &str) -> String {
    parse_lobby_host(raw).host_port
}

pub fn lobby_http_base(host_port: &str) -> String {
    let parsed = parse_lobby_host(host_port);
    let scheme = if parsed.tls { "https" } else { "http" };
    format!("{scheme}://{}", parsed.host_port)
}

pub fn lobby_ws_base(host_port: &str) -> String {
    let parsed = parse_lobby_host(host_port);
    let scheme = if parsed.tls { "wss" } else { "ws" };
    format!("{scheme}://{}", parsed.host_port)
}

pub fn lobby_api_url(host_port: &str, path: &str) -> String {
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    format!("{}{}", lobby_http_base(host_port), path)
}

const DEFAULT_LOBBY_HTTP_TIMEOUT_SECS: u64 = 30;
const DEFAULT_OLLAMA_HTTP_TIMEOUT_SECS: u64 = 3600;
const DEFAULT_HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Total request timeout for Ollama pull/load/generate (env `MTRXAI_OLLAMA_HTTP_TIMEOUT_SECS`).
pub fn ollama_http_timeout_secs() -> u64 {
    std::env::var("MTRXAI_OLLAMA_HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_OLLAMA_HTTP_TIMEOUT_SECS)
}

/// Shared HTTP client for lobby API calls (TLS, timeouts).
pub fn build_lobby_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(
            DEFAULT_LOBBY_HTTP_TIMEOUT_SECS,
        ))
        .connect_timeout(std::time::Duration::from_secs(
            DEFAULT_HTTP_CONNECT_TIMEOUT_SECS,
        ))
        .build()
        .map_err(Into::into)
}

/// HTTP client for long-running Ollama operations (model pull, load, generate).
pub fn build_ollama_http_client(insecure_tls: bool) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(ollama_http_timeout_secs()))
        .connect_timeout(std::time::Duration::from_secs(
            DEFAULT_HTTP_CONNECT_TIMEOUT_SECS,
        ));
    if insecure_tls {
        builder = builder.danger_accept_invalid_certs(true);
    }
    builder.build().map_err(Into::into)
}

pub fn log_lobby_target(host_port: &str) {
    let base = lobby_http_base(host_port);
    let tls_env = std::env::var("MTRXAI_LOBBY_TLS").unwrap_or_else(|_| "(unset)".into());
    println!(" Lobby API: {base} (MTRXAI_LOBBY_TLS={tls_env})");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_dev_defaults_to_http_8080() {
        let parsed = parse_lobby_host("127.0.0.1:8080");
        assert_eq!(parsed.host_port, "127.0.0.1:8080");
        assert!(!parsed.tls);
        assert_eq!(
            lobby_api_url("127.0.0.1:8080", "/api/public/config"),
            "http://127.0.0.1:8080/api/public/config"
        );
    }

    #[test]
    fn https_scheme_and_port_443_use_tls() {
        let parsed = parse_lobby_host("https://lobby.example.com");
        assert_eq!(parsed.host_port, "lobby.example.com:443");
        assert!(parsed.tls);
        assert_eq!(
            lobby_http_base("lobby.example.com:443"),
            "https://lobby.example.com:443"
        );
        assert_eq!(
            lobby_ws_base("lobby.example.com:443"),
            "wss://lobby.example.com:443"
        );
    }

    #[test]
    fn normalize_strips_scheme_and_adds_default_port() {
        assert_eq!(
            normalize_lobby_host("https://lobby.example.com/"),
            "lobby.example.com:443"
        );
        assert_eq!(
            normalize_lobby_host("mtrxai-server:8080"),
            "mtrxai-server:8080"
        );
    }

    #[test]
    fn mtrxai_hostnames_default_to_https_443() {
        assert_eq!(
            lobby_http_base("app.mtrxai.net"),
            "https://app.mtrxai.net:443"
        );
        assert_eq!(
            lobby_http_base("api.mtrxai.net"),
            "https://api.mtrxai.net:443"
        );
    }

    #[test]
    fn ollama_http_timeout_defaults_to_one_hour() {
        let _guard = EnvGuard::unset("MTRXAI_OLLAMA_HTTP_TIMEOUT_SECS");
        assert_eq!(ollama_http_timeout_secs(), 3600);
    }

    #[test]
    fn ollama_http_timeout_reads_env_override() {
        let _guard = EnvGuard::set("MTRXAI_OLLAMA_HTTP_TIMEOUT_SECS", "7200");
        assert_eq!(ollama_http_timeout_secs(), 7200);
    }

    #[test]
    fn build_ollama_http_client_succeeds() {
        let _guard = EnvGuard::unset("MTRXAI_OLLAMA_HTTP_TIMEOUT_SECS");
        assert!(build_ollama_http_client(false).is_ok());
        assert!(build_ollama_http_client(true).is_ok());
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            // SAFETY: test-only; serialized by test harness.
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            // SAFETY: test-only; serialized by test harness.
            unsafe { std::env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }
}
