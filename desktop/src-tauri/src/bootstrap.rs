use anyhow::{Context, Result};
use std::sync::Mutex;
use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use tokio::task::JoinHandle;

use crate::settings;

pub struct ClientRuntime {
    _handle: JoinHandle<()>,
    _proxy_port: u16,
}

pub struct BootstrapState {
    starting: Mutex<bool>,
}

impl BootstrapState {
    pub fn new() -> Self {
        Self {
            starting: Mutex::new(false),
        }
    }
}

fn is_production_lobby(lobby_host: &str) -> bool {
    let host = lobby_host.split(':').next().unwrap_or(lobby_host);
    host.eq_ignore_ascii_case("api.mtrxai.net") || lobby_host.ends_with(":443")
}

/// Default attestation skip: preserve an existing env value; otherwise skip for
/// local/dev lobbies (matches container compose) and require attestation for prod.
fn default_attestation_skip(lobby_host: &str) -> &'static str {
    if std::env::var_os("MTRXAI_ATTESTATION_SKIP").is_some() {
        return "";
    }
    if is_production_lobby(lobby_host) {
        "0"
    } else {
        "1"
    }
}

pub fn apply_client_env(app: &AppHandle, lobby_host: &str, proxy_port: u16) -> Result<()> {
    let config_path = settings::client_config_path(app)?;
    let lobby_host = settings::normalize_lobby_host(lobby_host)?;
    let lobby_tls = lobby_host.ends_with(":443");
    let attestation_skip = default_attestation_skip(&lobby_host);

    // SAFETY: called on the main/setup path before spawning the client task.
    unsafe {
        std::env::set_var("MTRXAI_LOBBY_HOST", &lobby_host);
        std::env::set_var("MTRXAI_PROXY_PORT", proxy_port.to_string());
        std::env::set_var("MTRXAI_CONFIG_PATH", config_path.to_string_lossy().as_ref());
        if !attestation_skip.is_empty() {
            std::env::set_var("MTRXAI_ATTESTATION_SKIP", attestation_skip);
        }
        if lobby_tls {
            std::env::set_var("MTRXAI_LOBBY_TLS", "1");
        }
    }

    Ok(())
}

pub fn spawn_client() -> JoinHandle<()> {
    tokio::spawn(async {
        if let Err(e) = peer::run().await {
            eprintln!("Client error: {e}");
        }
    })
}

pub async fn wait_for_health(port: u16) -> Result<()> {
    let url = format!("http://127.0.0.1:{port}/health");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut delay = std::time::Duration::from_millis(200);

    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for client at {url}");
        }

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(2));
            }
        }
    }
}

pub async fn start_client_and_open_main(
    app: AppHandle,
    lobby_host: String,
    proxy_port: u16,
) -> Result<()> {
    if app.try_state::<ClientRuntime>().is_some() {
        open_main_window(&app, proxy_port)?;
        return Ok(());
    }

    let bootstrap = app.state::<BootstrapState>();
    {
        let mut starting = bootstrap
            .starting
            .lock()
            .map_err(|_| anyhow::anyhow!("bootstrap lock poisoned"))?;
        if *starting {
            anyhow::bail!("client startup already in progress");
        }
        *starting = true;
    }

    let result = async {
        apply_client_env(&app, &lobby_host, proxy_port)?;

        let client_handle = spawn_client();
        if let Err(e) = wait_for_health(proxy_port).await {
            client_handle.abort();
            return Err(e);
        }

        app.manage(ClientRuntime {
            _handle: client_handle,
            _proxy_port: proxy_port,
        });

        open_main_window(&app, proxy_port)?;
        Ok(())
    }
    .await;

    if let Ok(mut starting) = bootstrap.starting.lock() {
        *starting = false;
    }

    result
}

pub fn open_main_window(app: &AppHandle, proxy_port: u16) -> Result<()> {
    if app.get_webview_window("main").is_some() {
        return Ok(());
    }

    let url = format!("http://127.0.0.1:{proxy_port}")
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid client URL: {e}"))?;

    WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url))
        .title("mtrxAI")
        .inner_size(1280.0, 860.0)
        .resizable(true)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to create main window: {e}"))?;

    Ok(())
}

pub fn open_setup_window(app: &AppHandle) -> Result<()> {
    if app.get_webview_window("setup").is_some() {
        return Ok(());
    }

    WebviewWindowBuilder::new(app, "setup", WebviewUrl::App("setup.html".into()))
        .title("mtrxAI — Lobby Server")
        .inner_size(480.0, 320.0)
        .resizable(false)
        .always_on_top(true)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to create setup window: {e}"))?;

    Ok(())
}

pub fn show_error_and_exit(app: &AppHandle, message: &str) {
    app.dialog()
        .message(message)
        .title("mtrxAI")
        .kind(MessageDialogKind::Error)
        .blocking_show();

    app.exit(1);
}

pub fn persist_lobby(app: &AppHandle, lobby_host: &str, proxy_port: u16) -> Result<()> {
    let lobby_host = settings::normalize_lobby_host(lobby_host)?;
    let mut desktop = settings::load(app)?;
    desktop.lobby_host = lobby_host;
    desktop.proxy_port = proxy_port;
    settings::save(app, &desktop)
}

/// Optional lobby reachability probe (not used as a boot gate).
#[allow(dead_code)]
pub async fn verify_lobby_reachable(lobby_host: &str) -> Result<()> {
    let lobby_host = settings::normalize_lobby_host(lobby_host)?;
    let scheme = if lobby_host.ends_with(":443") {
        "https"
    } else {
        "http"
    };
    let url = format!("{scheme}://{lobby_host}/api/public/clusters");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let resp = client.get(&url).send().await.with_context(|| {
        format!("Could not reach lobby server at {lobby_host}. Start the lobby server and use host:port only (no http://).")
    })?;

    if resp.status().is_success() {
        return Ok(());
    }

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    anyhow::bail!("Lobby server at {lobby_host} returned {status}: {body}");
}

pub fn resolve_startup_port(app: &AppHandle) -> Result<u16> {
    settings::resolve_proxy_port(app)
}
