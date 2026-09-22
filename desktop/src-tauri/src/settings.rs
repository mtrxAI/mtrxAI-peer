use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tauri::{AppHandle, Manager};

pub const DEFAULT_LOBBY_HOST: &str = "api.mtrxai.net";
pub const DEFAULT_PROXY_PORT: u16 = 11345;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopSettings {
    #[serde(default = "default_lobby_host")]
    pub lobby_host: String,
    #[serde(default = "default_proxy_port")]
    pub proxy_port: u16,
}

impl Default for DesktopSettings {
    fn default() -> Self {
        Self {
            lobby_host: default_lobby_host(),
            proxy_port: default_proxy_port(),
        }
    }
}

fn default_lobby_host() -> String {
    DEFAULT_LOBBY_HOST.to_string()
}

fn default_proxy_port() -> u16 {
    DEFAULT_PROXY_PORT
}

pub fn settings_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = app
        .path()
        .app_config_dir()
        .context("failed to resolve app config directory")?;
    fs::create_dir_all(&dir).context("failed to create app config directory")?;
    Ok(dir.join("desktop_settings.json"))
}

pub fn load(app: &AppHandle) -> Result<DesktopSettings> {
    let path = settings_path(app)?;
    if !path.exists() {
        return Ok(DesktopSettings::default());
    }
    let raw = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let settings: DesktopSettings =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    Ok(settings)
}

pub fn save(app: &AppHandle, settings: &DesktopSettings) -> Result<()> {
    let path = settings_path(app)?;
    let raw = serde_json::to_string_pretty(settings)?;
    fs::write(&path, raw).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub fn normalize_lobby_host(raw: &str) -> Result<String> {
    let mut host = raw.trim().to_string();
    if host.is_empty() {
        anyhow::bail!("Lobby server address is required.");
    }

    for prefix in ["http://", "https://"] {
        if host.to_ascii_lowercase().starts_with(prefix) {
            host = host[prefix.len()..].to_string();
        }
    }

    host = host.trim_end_matches('/').to_string();
    if host.is_empty() {
        anyhow::bail!("Lobby server address is required.");
    }

    if !host.contains(':') {
        if host.eq_ignore_ascii_case("mtrxai.net")
            || host.to_ascii_lowercase().ends_with(".mtrxai.net")
        {
            host = format!("{host}:443");
        } else {
            host = format!("{host}:8080");
        }
    }

    Ok(host)
}

pub fn resolve_lobby_host(app: &AppHandle) -> Result<Option<String>> {
    let path = settings_path(app)?;
    if path.exists() {
        let saved = load(app)?;
        if !saved.lobby_host.trim().is_empty() {
            return Ok(Some(normalize_lobby_host(&saved.lobby_host)?));
        }
    }

    if let Ok(host) = std::env::var("MTRXAI_LOBBY_HOST") {
        let host = host.trim().to_string();
        if !host.is_empty() {
            return Ok(Some(normalize_lobby_host(&host)?));
        }
    }

    Ok(Some(normalize_lobby_host(DEFAULT_LOBBY_HOST)?))
}

pub fn resolve_proxy_port(app: &AppHandle) -> Result<u16> {
    let saved = load(app)?;
    if saved.proxy_port != 0 {
        return Ok(saved.proxy_port);
    }

    if let Ok(port) = std::env::var("MTRXAI_PROXY_PORT") {
        if let Ok(port) = port.parse::<u16>() {
            if port != 0 {
                return Ok(port);
            }
        }
    }

    Ok(DEFAULT_PROXY_PORT)
}

pub fn client_config_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = app
        .path()
        .app_config_dir()
        .context("failed to resolve app config directory")?;
    fs::create_dir_all(&dir).context("failed to create app config directory")?;
    Ok(dir.join("client_config.json"))
}

pub fn tx_db_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = app
        .path()
        .app_config_dir()
        .context("failed to resolve app config directory")?;
    fs::create_dir_all(&dir).context("failed to create app config directory")?;
    Ok(dir.join("mtrxai_transactions.db"))
}
