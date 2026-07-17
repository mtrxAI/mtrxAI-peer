#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod bootstrap;
mod settings;

use bootstrap::{
    open_setup_window, persist_lobby, resolve_startup_port, show_error_and_exit,
    start_client_and_open_main, BootstrapState,
};
use serde::Serialize;
use settings::{normalize_lobby_host, DEFAULT_LOBBY_HOST, DEFAULT_PROXY_PORT};
use tauri::{AppHandle, Manager};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SetupDefaults {
    lobby_host: String,
    proxy_port: u16,
}

#[tauri::command]
fn get_setup_defaults(app: AppHandle) -> Result<SetupDefaults, String> {
    let desktop = settings::load(&app).map_err(|e| e.to_string())?;
    let lobby_host = if desktop.lobby_host.trim().is_empty() {
        DEFAULT_LOBBY_HOST.to_string()
    } else {
        normalize_lobby_host(&desktop.lobby_host).unwrap_or(desktop.lobby_host)
    };
    let proxy_port = if desktop.proxy_port == 0 {
        DEFAULT_PROXY_PORT
    } else {
        desktop.proxy_port
    };

    Ok(SetupDefaults {
        lobby_host,
        proxy_port,
    })
}

#[tauri::command]
async fn submit_lobby_setup(
    app: AppHandle,
    lobby_host: String,
    proxy_port: u16,
) -> Result<(), String> {
    let lobby_host = lobby_host.trim().to_string();
    if lobby_host.is_empty() {
        return Err("Lobby server address is required.".into());
    }
    let lobby_host = normalize_lobby_host(&lobby_host).map_err(|e| e.to_string())?;
    if proxy_port == 0 {
        return Err("Client port must be between 1 and 65535.".into());
    }

    persist_lobby(&app, &lobby_host, proxy_port).map_err(|e| e.to_string())?;

    start_client_and_open_main(app.clone(), lobby_host, proxy_port)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(setup) = app.get_webview_window("setup") {
        let _ = setup.close();
    }

    Ok(())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(BootstrapState::new())
        .invoke_handler(tauri::generate_handler![get_setup_defaults, submit_lobby_setup])
        .setup(|app| {
            let handle = app.handle().clone();

            match settings::resolve_lobby_host(&handle) {
                Ok(Some(lobby_host)) => {
                    let proxy_port = resolve_startup_port(&handle).unwrap_or(DEFAULT_PROXY_PORT);
                    tauri::async_runtime::spawn(async move {
                        if let Err(e) =
                            start_client_and_open_main(handle.clone(), lobby_host, proxy_port).await
                        {
                            show_error_and_exit(
                                &handle,
                                &format!("Failed to start mtrxAI client:\n{e}"),
                            );
                        }
                    });
                }
                Ok(None) => {
                    if let Err(e) = open_setup_window(&handle) {
                        show_error_and_exit(
                            &handle,
                            &format!("Failed to open lobby setup window:\n{e}"),
                        );
                    }
                }
                Err(e) => {
                    show_error_and_exit(
                        &handle,
                        &format!("Failed to load desktop settings:\n{e}"),
                    );
                }
            }

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
