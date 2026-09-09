mod alias;

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    middleware,
    response::{Html, IntoResponse},
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

use crate::client_config::{
    apply_register_response, beta_signup_with_lobby, create_cluster_with_lobby,
    create_swarm_membership, ensure_p2p_mode_for_swarms, fetch_lobby_config, fetch_service_credits,
    find_cluster_mut, find_swarm_by_token, find_swarm_mut, has_attached_llm_servers,
    is_inference_cell_kind, is_invite_only_registration, join_cluster_with_lobby,
    join_private_cluster, join_public_cluster_by_name, list_clusters_with_lobby,
    membership_from_response, probe_lobby_reachable, register_with_lobby, remove_cluster,
    remove_swarm, requires_credentials_registration, resolve_swarm_bootnodes, save_client_config,
    upsert_cluster, upsert_swarm, validate_custom_server_entry, ClusterMembership,
    CustomModelEntry, LlmServerEntry, PublicClusterView, SwarmMembership, DEFAULT_PUBLIC_CLUSTER,
};
use crate::cluster_manager::{cluster_membership_from_response, sync_peer_connections_with_store};
use crate::llm_backend::{normalize_backend_label, LlmBackend};
use crate::llm_discovery;
use crate::llm_proxy::ProxyState;
use crate::llm_registry::{LlmServerView, ModelCollision};
use crate::ollama_client::{fetch_show_info, is_embed_only, supports_embed, supports_generate};
use crate::shared::{ConnectionAction, ModelStartAction, RuntimeEvent};
use crate::swarm_manager::sync_swarm_status_for_proxy;

const INDEX_HTML: &str = include_str!("../../ui/v2/index.html");

fn lobby_gateway_err(context: &str, err: impl std::fmt::Display) -> (StatusCode, String) {
    let mut msg = format!("{context}: {err}");
    let err_s = err.to_string();
    if err_s.contains("build_id not on allowlist") {
        msg.push_str(
            ". Register this client binary on the lobby: export MTRXAI_ADMIN_KEY=... && scripts/register-allowed-build.sh release/docker/allowed_build.json",
        );
    }
    eprintln!("❌ {msg}");
    (
        StatusCode::BAD_GATEWAY,
        serde_json::json!({ "error": msg }).to_string(),
    )
}

pub fn router(state: ProxyState) -> Router {
    Router::new()
        .route("/api/client/setup", get(get_setup))
        .route("/api/client/lobby-config", get(get_lobby_config))
        .route("/api/client/beta-signup", post(post_beta_signup))
        .route("/api/client/register", post(post_register))
        .route("/api/client/clusters", get(get_clusters))
        .route("/api/client/clusters/public", get(get_public_clusters))
        .route("/api/client/clusters/create", post(post_cluster_create))
        .route("/api/client/clusters/join", post(post_cluster_join))
        .route("/api/client/clusters/:cluster_id", delete(delete_cluster))
        .route(
            "/api/client/clusters/:cluster_id/disconnect",
            post(post_cluster_disconnect),
        )
        .route(
            "/api/client/clusters/:cluster_id/connect",
            post(post_cluster_connect),
        )
        .route(
            "/api/client/clusters/:cluster_id/maintenance",
            post(post_cluster_maintenance),
        )
        .route(
            "/api/client/swarms",
            get(get_swarms).post(post_swarm_create),
        )
        .route("/api/client/swarms/join", post(post_swarm_join))
        .route("/api/client/swarms/:swarm_id", delete(delete_swarm))
        .route(
            "/api/client/swarms/:swarm_id/disconnect",
            post(post_swarm_disconnect),
        )
        .route(
            "/api/client/swarms/:swarm_id/connect",
            post(post_swarm_connect),
        )
        .route(
            "/api/client/swarms/:swarm_id/maintenance",
            post(post_swarm_maintenance),
        )
        .route(
            "/api/client/network/disconnect-all",
            post(post_network_disconnect_all),
        )
        .route(
            "/api/client/network/connect-all",
            post(post_network_connect_all),
        )
        .route(
            "/api/client/network/pause-all",
            post(post_network_pause_all),
        )
        .route(
            "/api/client/clusters/:cluster_id/schedule",
            put(put_cluster_schedule),
        )
        .route(
            "/api/client/swarms/:swarm_id/schedule",
            put(put_swarm_schedule),
        )
        .route("/api/client/chat/models", get(get_chat_models))
        .route("/api/client/chat/sessions", get(get_chat_sessions).post(post_chat_session))
        .route(
            "/api/client/chat/sessions/:id",
            get(get_chat_session)
                .put(put_chat_session)
                .delete(delete_chat_session),
        )
        .route("/api/client/chat", post(crate::llm_proxy::chat_completion))
        .route("/api/client/llm/scan", get(get_llm_scan))
        .route("/api/client/llm/select", post(post_llm_select))
        .route(
            "/api/client/llm/servers",
            get(get_llm_servers).post(post_llm_server),
        )
        .route(
            "/api/client/llm/servers/:id/attach",
            post(post_llm_server_attach),
        )
        .route(
            "/api/client/llm/servers/:id/detach",
            post(post_llm_server_detach),
        )
        .route(
            "/api/client/llm/servers/:id",
            patch(patch_llm_server).delete(delete_llm_server),
        )
        .route(
            "/api/client/llm/servers/:id/api-key",
            put(put_llm_server_api_key),
        )
        .route(
            "/api/client/llm/servers/:id/admin-token",
            put(put_llm_server_admin_token),
        )
        .route("/api/client/models/catalog", get(get_models_catalog))
        .route(
            "/api/client/models/catalog/:name",
            get(get_models_catalog_entry),
        )
        .route("/api/client/models/hf-catalog", get(get_hf_models_catalog))
        .route(
            "/api/client/models/hf-catalog/quants",
            get(get_hf_model_quants),
        )
        .route("/api/client/models/request", post(post_model_request))
        .route("/api/client/models/run-local", post(post_model_run_local))
        .route(
            "/api/client/models/visibility",
            patch(patch_model_visibility),
        )
        .route(
            "/api/client/models/visibility/bulk",
            post(post_model_visibility_bulk),
        )
        .route("/api/client/models/:name/load", post(load_local_model))
        .route("/api/client/models/:name/unload", post(unload_local_model))
        .route("/api/client/models/:name", delete(delete_local_model))
        .route("/api/client/models/respond", post(post_model_respond))
        .route(
            "/api/client/connection/respond",
            post(post_connection_respond),
        )
        .route("/api/client/settings", get(get_settings))
        .route("/api/client/settings", put(put_settings))
        .route("/api/client/status", get(get_status))
        .route("/api/client/peers/blocked", get(get_blocked_peers))
        .route("/api/client/peers/:peer_id/block", post(post_block_peer))
        .route(
            "/api/client/peers/:peer_id/block",
            delete(delete_block_peer),
        )
        .route("/api/client/peers/:peer_id/report", post(post_report_peer))
        .route("/api/client/transactions", get(get_transactions))
        .route("/api/client/transactions/stats", get(get_transaction_stats))
        .route("/assets/logo.svg", get(logo_asset))
        .route("/", get(index_page))
        // Prefer `/api/peer/*` going forward; `/api/client/*` remains for the embedded UI.
        .layer(middleware::from_fn(alias::alias_api_peer_to_client))
        .with_state(state)
}

async fn logo_asset() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "image/svg+xml")],
        include_bytes!("../../ui/assets/logo.svg").as_slice(),
    )
}

async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

#[derive(Serialize)]
struct SetupResponse {
    complete: bool,
    step: u8,
    has_peer: bool,
    has_clusters: bool,
    has_swarms: bool,
    p2p_mode: crate::shared::NetworkMode,
    has_llm: bool,
}

async fn get_setup(State(state): State<ProxyState>) -> Json<SetupResponse> {
    let cfg = state.client_config.read().await.clone();
    let has_peer = cfg.peer_id.is_some() && cfg.service_id.is_some();
    let has_clusters = !cfg.clusters.is_empty();
    let has_swarms = !cfg.swarms.is_empty();
    let step = if !has_peer {
        1
    } else if !has_clusters && !has_swarms {
        2
    } else if !cfg.setup_complete {
        3
    } else {
        4
    };
    Json(SetupResponse {
        complete: cfg.setup_complete,
        step,
        has_peer,
        has_clusters,
        has_swarms,
        p2p_mode: cfg.p2p_mode,
        has_llm: has_attached_llm_servers(&cfg),
    })
}

async fn sync_registry_after_config_change(
    state: &ProxyState,
    cfg: &crate::client_config::ClientConfig,
) {
    if let Err(e) = state.llm_registry.inner.sync_from_config(cfg).await {
        eprintln!("⚠️ Registry sync failed: {}", e);
        return;
    }
    if state.llm_registry.inner.attached_count().await > 0 {
        if let Ok(catalog) = state
            .llm_registry
            .inner
            .rebuild_catalog(crate::ollama_client::gpu_probe_mode())
            .await
        {
            let mut app = state.shared_state.lock().await;
            app.local_models = catalog.model_names.clone();
            app.local_models_full = catalog.models.clone();
            app.last_gpu_host = catalog.gpu_host.clone();
            app.local_model_collisions = catalog.collisions.clone();
        }
    } else {
        let mut app = state.shared_state.lock().await;
        app.local_models.clear();
        app.local_models_full.clear();
        app.local_model_collisions.clear();
    }
}

#[derive(Serialize)]
struct LobbyConfigProxyResponse {
    peer_registration: String,
}

async fn get_lobby_config(
    State(state): State<ProxyState>,
) -> Result<Json<LobbyConfigProxyResponse>, (StatusCode, String)> {
    let lobby_host = state.lobby_host.clone();
    let cfg = fetch_lobby_config(&state.http_client, &lobby_host)
        .await
        .map_err(|e| lobby_gateway_err("Lobby config fetch failed", e))?;
    Ok(Json(LobbyConfigProxyResponse {
        peer_registration: cfg.peer_registration,
    }))
}

#[derive(Deserialize)]
struct RegisterBody {
    #[serde(default)]
    registered: bool,
    has_existing_service: bool,
    #[serde(default)]
    service_name: Option<String>,
    #[serde(default)]
    service_password: Option<String>,
    #[serde(default)]
    peer_id: Option<String>,
    #[serde(default)]
    cluster_name: Option<String>,
    #[serde(default)]
    contact_email: Option<String>,
    #[serde(default)]
    terms_accepted: bool,
}

#[derive(Deserialize)]
struct BetaSignupBody {
    email: String,
    service_name: String,
    service_password: String,
    #[serde(default)]
    terms_accepted: bool,
}

#[derive(Serialize)]
struct BetaSignupResponse {
    status: String,
    message: String,
}

async fn post_beta_signup(
    State(state): State<ProxyState>,
    Json(body): Json<BetaSignupBody>,
) -> Result<Json<BetaSignupResponse>, (StatusCode, String)> {
    if !body.terms_accepted {
        return Err((
            StatusCode::BAD_REQUEST,
            "terms acceptance required".to_string(),
        ));
    }
    beta_signup_with_lobby(
        &state.http_client,
        &state.lobby_host,
        body.email.trim(),
        body.service_name.trim(),
        body.service_password.trim(),
        body.terms_accepted,
    )
    .await
    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(Json(BetaSignupResponse {
        status: "pending".into(),
        message: "Your request will be reviewed and you will be contacted soon.".into(),
    }))
}

#[derive(Serialize)]
struct RegisterResponse {
    peer_id: String,
    service_id: String,
    service_name: String,
    credit_balance: i64,
    created_new_service: bool,
}

fn parse_service_credentials(body: &RegisterBody) -> Result<(&str, &str), (StatusCode, String)> {
    let name = body
        .service_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((StatusCode::BAD_REQUEST, "service name required".to_string()))?;
    let password = body
        .service_password
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "service password required".to_string(),
        ))?;
    Ok((name, password))
}

fn parse_contact_email(body: &RegisterBody) -> Result<&str, (StatusCode, String)> {
    body.contact_email
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "contact email required".to_string(),
        ))
}

async fn post_register(
    State(state): State<ProxyState>,
    Json(body): Json<RegisterBody>,
) -> Result<Json<RegisterResponse>, (StatusCode, String)> {
    let lobby_host = state.lobby_host.clone();
    let tx_store = state.tx_store.as_ref();

    let lobby_cfg = match fetch_lobby_config(&state.http_client, &lobby_host).await {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            eprintln!("⚠️ Lobby config unavailable, using registration defaults: {e}");
            None
        }
    };

    if let Some(ref lobby_cfg) = lobby_cfg {
        if requires_credentials_registration(lobby_cfg) {
            let (service_name, service_password) = parse_service_credentials(&body)?;

            let peer_id = body
                .peer_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let reg = register_with_lobby(
                &state.http_client,
                &lobby_host,
                peer_id,
                Some(service_name),
                Some(service_password),
                false,
                None,
                tx_store,
            )
            .await
            .map_err(|e| lobby_gateway_err("Peer registration failed", e))?;

            return finish_register(state, body, reg, false).await;
        }
    }

    let create_new_service = !body.has_existing_service && !body.registered;
    if create_new_service && !body.terms_accepted {
        return Err((
            StatusCode::BAD_REQUEST,
            "terms acceptance required".to_string(),
        ));
    }
    let (service_name, service_password) =
        if create_new_service || body.has_existing_service || body.registered {
            parse_service_credentials(&body)?
        } else {
            ("", "")
        };
    let contact_email = if create_new_service {
        Some(parse_contact_email(&body)?)
    } else {
        None
    };

    let reg = register_with_lobby(
        &state.http_client,
        &lobby_host,
        body.peer_id.as_deref().filter(|s| !s.trim().is_empty()),
        if create_new_service || body.has_existing_service || body.registered {
            Some(service_name)
        } else {
            None
        },
        if create_new_service || body.has_existing_service || body.registered {
            Some(service_password)
        } else {
            None
        },
        create_new_service,
        contact_email,
        tx_store,
    )
    .await
    .map_err(|e| lobby_gateway_err("Peer registration failed", e))?;

    finish_register(state, body, reg, create_new_service).await
}

async fn finish_register(
    state: ProxyState,
    body: RegisterBody,
    reg: crate::client_config::RegisterPeerResponse,
    created_new_service: bool,
) -> Result<Json<RegisterResponse>, (StatusCode, String)> {
    let lobby_host = state.lobby_host.clone();
    {
        let mut cfg = state.client_config.write().await;
        apply_register_response(&mut cfg, &reg);
        cfg.lobby_host = lobby_host;
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let cluster_name = body
        .cluster_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_PUBLIC_CLUSTER)
        .to_string();

    let cluster_resp =
        join_public_cluster_by_name(&state.http_client, &state.lobby_host, &cluster_name, None)
            .await
            .map_err(|e| lobby_gateway_err("Cluster join failed", e))?;

    {
        let mut cfg = state.client_config.write().await;
        upsert_cluster(
            &mut cfg,
            membership_from_response(&cluster_resp, Some(String::new())),
        );
        cfg.cluster = Some(cluster_name);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;

    {
        let mut app = state.shared_state.lock().await;
        app.peer_id = reg.peer_id.to_string();
        app.service_id = reg.service_id.to_string();
        app.my_id = reg.peer_id.to_string();
        app.credit_balance = reg.credit_balance;
    }

    let setup_complete = {
        let mut cfg = state.client_config.write().await;
        cfg.setup_complete = cfg.peer_id.is_some()
            && cfg.service_id.is_some()
            && !cfg.clusters.is_empty()
            && has_attached_llm_servers(&cfg);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        cfg.setup_complete
    };

    if setup_complete {
        state
            .setup_complete
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = state
            .runtime_event_tx
            .send(RuntimeEvent::SetupComplete)
            .await;
    }

    Ok(Json(RegisterResponse {
        peer_id: reg.peer_id.to_string(),
        service_id: reg.service_id.to_string(),
        service_name: reg.service_name.clone(),
        credit_balance: reg.credit_balance,
        created_new_service,
    }))
}

#[derive(Serialize)]
struct ClustersResponse {
    clusters: Vec<ClusterMembership>,
}

async fn get_clusters(State(state): State<ProxyState>) -> Json<ClustersResponse> {
    let clusters = state.client_config.read().await.clusters.clone();
    Json(ClustersResponse { clusters })
}

async fn get_public_clusters(
    State(state): State<ProxyState>,
) -> Result<Json<Vec<PublicClusterView>>, (StatusCode, String)> {
    let peer_id = state.client_config.read().await.peer_id.clone();
    list_clusters_with_lobby(
        &state.http_client,
        &state.lobby_host,
        peer_id.as_deref(),
        Some(state.tx_store.as_ref()),
    )
    .await
    .map(|clusters| {
        Json(
            clusters
                .into_iter()
                .map(|c| PublicClusterView {
                    cluster_id: c.cluster_id,
                    name: c.name,
                    continent: c.continent,
                })
                .collect(),
        )
    })
    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
}

#[derive(Deserialize)]
struct CreateClusterBody {
    name: Option<String>,
    #[serde(default)]
    visibility: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    required_attestation_flags: Option<u64>,
}

#[derive(Serialize)]
struct ClusterActionResponse {
    cluster_id: String,
    name: Option<String>,
    visibility: Option<String>,
}

async fn notify_clusters_changed(state: &ProxyState) {
    state.bump_cluster_state();
    let _ = state.cluster_notify_tx.send(()).await;
}

async fn post_cluster_create(
    State(state): State<ProxyState>,
    Json(body): Json<CreateClusterBody>,
) -> Result<Json<ClusterActionResponse>, (StatusCode, String)> {
    let lobby_host = state.lobby_host.clone();
    let peer_id = state
        .client_config
        .read()
        .await
        .peer_id
        .clone()
        .ok_or((StatusCode::BAD_REQUEST, "peer not registered".to_string()))?;

    let visibility = body.visibility.as_deref().unwrap_or("private");
    if visibility.eq_ignore_ascii_case("public") {
        return Err((
            StatusCode::FORBIDDEN,
            "public clusters can only be created via admin API".to_string(),
        ));
    }
    let resp = create_cluster_with_lobby(
        &state.http_client,
        &lobby_host,
        body.name.as_deref(),
        visibility,
        Some(peer_id.as_str()),
        body.password.as_deref(),
        body.required_attestation_flags,
    )
    .await
    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    let membership = cluster_membership_from_response(&resp, body.password.clone());
    {
        let mut cfg = state.client_config.write().await;
        upsert_cluster(&mut cfg, membership.clone());
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;

    Ok(Json(ClusterActionResponse {
        cluster_id: membership.cluster_id,
        name: membership.name,
        visibility: membership.visibility,
    }))
}

#[derive(Deserialize)]
struct JoinClusterBody {
    cluster_id: Option<String>,
    name: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

async fn post_cluster_join(
    State(state): State<ProxyState>,
    Json(body): Json<JoinClusterBody>,
) -> Result<Json<ClusterActionResponse>, (StatusCode, String)> {
    let lobby_host = state.lobby_host.clone();
    let resp = match (
        body.cluster_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
        body.name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
    ) {
        (Some(id), Some(name)) => join_private_cluster(
            &state.http_client,
            &lobby_host,
            id,
            name,
            body.password.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?,
        (Some(id), None) => join_cluster_with_lobby(
            &state.http_client,
            &lobby_host,
            id,
            body.password.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?,
        (None, Some(name)) => join_public_cluster_by_name(
            &state.http_client,
            &lobby_host,
            name,
            body.password.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?,
        (None, None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "cluster_id or name required (private clusters need both)".to_string(),
            ));
        }
    };

    let membership = cluster_membership_from_response(&resp, body.password.clone());
    {
        let mut cfg = state.client_config.write().await;
        upsert_cluster(&mut cfg, membership.clone());
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;

    Ok(Json(ClusterActionResponse {
        cluster_id: membership.cluster_id,
        name: membership.name,
        visibility: membership.visibility,
    }))
}

async fn post_cluster_disconnect(
    State(state): State<ProxyState>,
    Path(cluster_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let cluster = find_cluster_mut(&mut cfg, &cluster_id)
            .ok_or((StatusCode::NOT_FOUND, "cluster not found".to_string()))?;
        cluster.connected = Some(false);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_cluster_connect(
    State(state): State<ProxyState>,
    Path(cluster_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let cluster = find_cluster_mut(&mut cfg, &cluster_id)
            .ok_or((StatusCode::NOT_FOUND, "cluster not found".to_string()))?;
        cluster.connected = Some(true);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct MaintenanceBody {
    enabled: bool,
}

async fn post_cluster_maintenance(
    State(state): State<ProxyState>,
    Path(cluster_id): Path<String>,
    Json(body): Json<MaintenanceBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let cluster = find_cluster_mut(&mut cfg, &cluster_id)
            .ok_or((StatusCode::NOT_FOUND, "cluster not found".to_string()))?;
        cluster.accepting_jobs = Some(!body.enabled);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_cluster(
    State(state): State<ProxyState>,
    Path(cluster_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        remove_cluster(&mut cfg, &cluster_id);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct SwarmsResponse {
    p2p_mode: crate::shared::NetworkMode,
    swarms: Vec<crate::shared::SwarmStatus>,
}

async fn get_swarms(State(state): State<ProxyState>) -> Json<SwarmsResponse> {
    sync_swarm_status_for_proxy(&state).await;
    let cfg = state.client_config.read().await;
    let swarms = state.shared_state.lock().await.swarms.clone();
    Json(SwarmsResponse {
        p2p_mode: cfg.p2p_mode,
        swarms,
    })
}

#[derive(Deserialize)]
struct CreateSwarmBody {
    name: Option<String>,
    p2p_token: Option<String>,
}

#[derive(Serialize)]
struct SwarmActionResponse {
    swarm_id: String,
    name: Option<String>,
    p2p_token: String,
}

async fn notify_swarms_changed(state: &ProxyState) {
    sync_swarm_status_for_proxy(state).await;
    state.bump_cluster_state();
    let _ = state.swarm_notify_tx.send(()).await;
    let _ = state
        .runtime_event_tx
        .send(RuntimeEvent::SwarmsChanged)
        .await;
}

async fn post_swarm_create(
    State(state): State<ProxyState>,
    Json(body): Json<CreateSwarmBody>,
) -> Result<Json<SwarmActionResponse>, (StatusCode, String)> {
    let membership = create_swarm_membership(body.name, body.p2p_token);
    {
        let mut cfg = state.client_config.write().await;
        upsert_swarm(&mut cfg, membership.clone());
        ensure_p2p_mode_for_swarms(&mut cfg);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(Json(SwarmActionResponse {
        swarm_id: membership.swarm_id,
        name: membership.name,
        p2p_token: membership.p2p_token,
    }))
}

#[derive(Deserialize)]
struct JoinSwarmBody {
    p2p_token: String,
    name: Option<String>,
    #[serde(default)]
    bootnodes: Vec<String>,
}

async fn post_swarm_join(
    State(state): State<ProxyState>,
    Json(body): Json<JoinSwarmBody>,
) -> Result<Json<SwarmActionResponse>, (StatusCode, String)> {
    let token = body.p2p_token.trim();
    if token.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "p2p_token required".to_string()));
    }
    let membership = {
        let mut cfg = state.client_config.write().await;
        let membership = if let Some(existing) = find_swarm_by_token(&cfg, token) {
            let swarm_id = existing.swarm_id.clone();
            let entry = find_swarm_mut(&mut cfg, &swarm_id).ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                "swarm lookup failed".to_string(),
            ))?;
            if body.name.is_some() {
                entry.name = body.name.clone();
            }
            entry.connected = Some(true);
            if !body.bootnodes.is_empty() {
                entry.bootnodes = body.bootnodes.clone();
            }
            entry.clone()
        } else {
            let mut membership =
                create_swarm_membership(body.name.clone(), Some(token.to_string()));
            membership.bootnodes = body.bootnodes.clone();
            if membership.bootnodes.is_empty() {
                membership.bootnodes =
                    resolve_swarm_bootnodes(&state.http_client, &cfg.lobby_host, &membership).await;
            }
            upsert_swarm(&mut cfg, membership.clone());
            membership
        };
        ensure_p2p_mode_for_swarms(&mut cfg);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        membership
    };
    notify_swarms_changed(&state).await;
    Ok(Json(SwarmActionResponse {
        swarm_id: membership.swarm_id,
        name: membership.name,
        p2p_token: membership.p2p_token,
    }))
}

async fn post_swarm_disconnect(
    State(state): State<ProxyState>,
    Path(swarm_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let swarm = find_swarm_mut(&mut cfg, &swarm_id)
            .ok_or((StatusCode::NOT_FOUND, "swarm not found".to_string()))?;
        swarm.connected = Some(false);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_swarm_connect(
    State(state): State<ProxyState>,
    Path(swarm_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let swarm = find_swarm_mut(&mut cfg, &swarm_id)
            .ok_or((StatusCode::NOT_FOUND, "swarm not found".to_string()))?;
        swarm.connected = Some(true);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_swarm_maintenance(
    State(state): State<ProxyState>,
    Path(swarm_id): Path<String>,
    Json(body): Json<MaintenanceBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let swarm = find_swarm_mut(&mut cfg, &swarm_id)
            .ok_or((StatusCode::NOT_FOUND, "swarm not found".to_string()))?;
        swarm.accepting_jobs = Some(!body.enabled);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_network_disconnect_all(
    State(state): State<ProxyState>,
) -> Result<StatusCode, (StatusCode, String)> {
    crate::network_actions::disconnect_all(
        &state.client_config,
        &state.cluster_notify_tx,
        &state.swarm_notify_tx,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_network_connect_all(
    State(state): State<ProxyState>,
) -> Result<StatusCode, (StatusCode, String)> {
    crate::network_actions::connect_all(
        &state.client_config,
        &state.cluster_notify_tx,
        &state.swarm_notify_tx,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_network_pause_all(
    State(state): State<ProxyState>,
    Json(body): Json<MaintenanceBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    crate::network_actions::set_pause_all(
        &state.client_config,
        &state.cluster_notify_tx,
        &state.swarm_notify_tx,
        body.enabled,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ScheduleBody {
    enabled: Option<bool>,
    start: Option<String>,
    end: Option<String>,
}

async fn put_cluster_schedule(
    State(state): State<ProxyState>,
    Path(cluster_id): Path<String>,
    Json(body): Json<ScheduleBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let cluster = find_cluster_mut(&mut cfg, &cluster_id)
            .ok_or((StatusCode::NOT_FOUND, "cluster not found".to_string()))?;
        if let Some(v) = body.enabled {
            cluster.schedule_enabled = Some(v);
        }
        if let Some(v) = body.start {
            cluster.schedule_start = Some(v);
        }
        if let Some(v) = body.end {
            cluster.schedule_end = Some(v);
        }
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_clusters_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_swarm_schedule(
    State(state): State<ProxyState>,
    Path(swarm_id): Path<String>,
    Json(body): Json<ScheduleBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let swarm = find_swarm_mut(&mut cfg, &swarm_id)
            .ok_or((StatusCode::NOT_FOUND, "swarm not found".to_string()))?;
        if let Some(v) = body.enabled {
            swarm.schedule_enabled = Some(v);
        }
        if let Some(v) = body.start {
            swarm.schedule_start = Some(v);
        }
        if let Some(v) = body.end {
            swarm.schedule_end = Some(v);
        }
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_swarm(
    State(state): State<ProxyState>,
    Path(swarm_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        remove_swarm(&mut cfg, &swarm_id);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    notify_swarms_changed(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_llm_scan(
    State(state): State<ProxyState>,
) -> Json<Vec<llm_discovery::DiscoveredServer>> {
    let servers = llm_discovery::scan_local_servers(&state.http_client).await;
    Json(servers)
}

#[derive(Deserialize)]
struct LlmSelectBody {
    kind: String,
    url: String,
}

#[derive(Serialize)]
struct LlmSelectResponse {
    ok: bool,
    backend: String,
    url: String,
}

async fn post_llm_select(
    State(state): State<ProxyState>,
    Json(body): Json<LlmSelectBody>,
) -> Result<Json<LlmSelectResponse>, (StatusCode, String)> {
    let label = normalize_backend_label(&body.kind);
    let url = body.url.trim_end_matches('/').to_string();
    let client = state.ollama_http_client.read().await.clone();
    let backend = LlmBackend::from_config(&label, &url, client)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    backend.health_check().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("Health check failed: {}", e),
        )
    })?;

    let server_id = {
        let mut cfg = state.client_config.write().await;
        let id = state
            .llm_registry
            .inner
            .find_or_add_entry(
                &mut cfg,
                LlmServerEntry {
                    id: String::new(),
                    kind: label.clone(),
                    url: url.clone(),
                    label: None,
                    attached: false,
                    order: 0,
                    source: "scan".to_string(),
                    api_type: None,
                    models: Vec::new(),
                    advertise_to_cluster: true,
                },
            )
            .await;
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        id
    };

    {
        let mut cfg = state.client_config.write().await;
        state
            .llm_registry
            .inner
            .attach(&mut cfg, &server_id)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
        cfg.setup_complete = cfg.peer_id.is_some()
            && cfg.service_id.is_some()
            && !cfg.clusters.is_empty()
            && has_attached_llm_servers(&cfg);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        sync_registry_after_config_change(&state, &cfg).await;
    }

    if state.client_config.read().await.setup_complete {
        let _ = state
            .runtime_event_tx
            .send(RuntimeEvent::SetupComplete)
            .await;
    }

    Ok(Json(LlmSelectResponse {
        ok: true,
        backend: label,
        url,
    }))
}

#[derive(Deserialize)]
struct AddLlmServerBody {
    kind: String,
    url: String,
    label: Option<String>,
    api_key: Option<String>,
    api_type: Option<String>,
    models: Option<Vec<CustomModelEntry>>,
}

#[derive(Deserialize)]
struct PutLlmServerApiKeyBody {
    api_key: Option<String>,
}

#[derive(Serialize)]
struct LlmServersResponse {
    servers: Vec<LlmServerView>,
}

async fn get_llm_servers(State(state): State<ProxyState>) -> Json<LlmServersResponse> {
    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Json(LlmServersResponse { servers })
}

async fn post_llm_server(
    State(state): State<ProxyState>,
    Json(body): Json<AddLlmServerBody>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    let label = normalize_backend_label(&body.kind);
    let url = body.url.trim_end_matches('/').to_string();
    let models = body.models.unwrap_or_default();
    let entry = LlmServerEntry {
        id: String::new(),
        kind: label.clone(),
        url: url.clone(),
        label: body.label.clone(),
        attached: false,
        order: 0,
        source: "manual".to_string(),
        api_type: body.api_type.clone(),
        models: models.clone(),
        advertise_to_cluster: true,
    };
    validate_custom_server_entry(&entry).map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    if !crate::client_config::is_custom_server_kind(&label) {
        let client = state.ollama_http_client.read().await.clone();
        let backend = if is_inference_cell_kind(&label) {
            LlmBackend::from_server_entry_probed(&entry, body.api_key.clone(), client)
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        } else {
            LlmBackend::from_config(&label, &url, client)
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        };
        backend.health_check().await.map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Health check failed: {}", e),
            )
        })?;
    } else {
        let client = state.ollama_http_client.read().await.clone();
        let backend = LlmBackend::from_server_entry(&entry, body.api_key.clone(), client)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        backend.health_check().await.map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Health check failed: {}", e),
            )
        })?;
    }

    let server_id = {
        let mut cfg = state.client_config.write().await;
        let id = state
            .llm_registry
            .inner
            .find_or_add_entry(&mut cfg, entry)
            .await;
        if let Some(api_key) = body.api_key.as_deref().filter(|k| !k.is_empty()) {
            state
                .tx_store
                .put_server_api_key(
                    &id,
                    api_key,
                    cfg.peer_id.as_deref(),
                    cfg.service_id.as_deref(),
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        id
    };
    let _ = server_id;

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

async fn put_llm_server_api_key(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
    Json(body): Json<PutLlmServerApiKeyBody>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let cfg = state.client_config.read().await;
        if !cfg.llm_servers.iter().any(|s| s.id == id) {
            return Err((StatusCode::NOT_FOUND, "server not found".to_string()));
        }
        match body.api_key.as_deref().filter(|k| !k.is_empty()) {
            Some(api_key) => {
                state
                    .tx_store
                    .put_server_api_key(
                        &id,
                        api_key,
                        cfg.peer_id.as_deref(),
                        cfg.service_id.as_deref(),
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
            None => {
                state
                    .tx_store
                    .delete_server_api_key(&id)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
        }
    }

    let cfg = state.client_config.read().await.clone();
    if cfg.llm_servers.iter().any(|s| s.id == id && s.attached) {
        sync_registry_after_config_change(&state, &cfg).await;
    }
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

#[derive(Deserialize)]
struct PutLlmServerAdminTokenBody {
    admin_token: Option<String>,
}

async fn put_llm_server_admin_token(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
    Json(body): Json<PutLlmServerAdminTokenBody>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let cfg = state.client_config.read().await;
        let Some(entry) = cfg.llm_servers.iter().find(|s| s.id == id) else {
            return Err((StatusCode::NOT_FOUND, "server not found".to_string()));
        };
        if !is_inference_cell_kind(&entry.kind) {
            return Err((
                StatusCode::BAD_REQUEST,
                "admin token is only supported for inference-cell servers".to_string(),
            ));
        }
        match body.admin_token.as_deref().filter(|k| !k.is_empty()) {
            Some(admin_token) => {
                state
                    .tx_store
                    .put_server_admin_token(
                        &id,
                        admin_token,
                        cfg.peer_id.as_deref(),
                        cfg.service_id.as_deref(),
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
            None => {
                state
                    .tx_store
                    .delete_server_admin_token(&id)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
        }
    }

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

async fn post_llm_server_attach(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        state
            .llm_registry
            .inner
            .attach(&mut cfg, &id)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
        cfg.setup_complete = cfg.peer_id.is_some()
            && cfg.service_id.is_some()
            && !cfg.clusters.is_empty()
            && has_attached_llm_servers(&cfg);
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        sync_registry_after_config_change(&state, &cfg).await;
    }

    if state.client_config.read().await.setup_complete {
        let _ = state
            .runtime_event_tx
            .send(RuntimeEvent::SetupComplete)
            .await;
    }

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

async fn post_llm_server_detach(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        state
            .llm_registry
            .inner
            .detach(&mut cfg, &id)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        sync_registry_after_config_change(&state, &cfg).await;
        state.bump_cluster_state();
    }

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

#[derive(Deserialize)]
struct PatchLlmServerBody {
    advertise_to_cluster: Option<bool>,
}

async fn patch_llm_server(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
    Json(body): Json<PatchLlmServerBody>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        let server = cfg
            .llm_servers
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or((StatusCode::NOT_FOUND, "Server not found".into()))?;
        if let Some(advertise) = body.advertise_to_cluster {
            server.advertise_to_cluster = advertise;
        }
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        if cfg.llm_servers.iter().any(|s| s.id == id && s.attached) {
            sync_registry_after_config_change(&state, &cfg).await;
            state.bump_cluster_state();
        }
    }

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

async fn delete_llm_server(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
) -> Result<Json<LlmServersResponse>, (StatusCode, String)> {
    {
        let mut cfg = state.client_config.write().await;
        state
            .llm_registry
            .inner
            .remove_server(&mut cfg, &id)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        sync_registry_after_config_change(&state, &cfg).await;
        state.bump_cluster_state();
    }

    let cfg = state.client_config.read().await.clone();
    let servers = state.llm_registry.inner.server_views(&cfg).await;
    Ok(Json(LlmServersResponse { servers }))
}

#[derive(Serialize)]
struct StatusResponse {
    peer_id: String,
    service_id: String,
    service_name: String,
    lobby_host: String,
    lobby_api_url: String,
    proxy_port: u16,
    proxy_bind: String,
    client_url: String,
    clusters: Vec<crate::shared::ClusterStatus>,
    swarms: Vec<crate::shared::SwarmStatus>,
    p2p_mode: crate::shared::NetworkMode,
    credit_balance: i64,
    lobby_connected: bool,
    setup_complete: bool,
    llm_servers: Vec<LlmServerView>,
    attached_server_count: usize,
    local_model_collisions: Vec<ModelCollision>,
    local_models: Vec<Value>,
    network_models: Vec<Value>,
    hidden_models: Vec<String>,
    gpu_host: Option<crate::shared::GpuHostStatus>,
    gpu_history: crate::shared::GpuHistory,
    connections: ConnectionsView,
    model_start_requests: Vec<crate::shared::ModelStartRequestState>,
    incoming_model_offers: Vec<crate::shared::ModelStartOfferState>,
    incoming_connection_offers: Vec<crate::shared::IncomingConnectionOfferState>,
    local_model_runs: Vec<crate::shared::LocalModelRunState>,
    auto_approve_run_model_request: bool,
    auto_approve_inference_connections: bool,
    allow_unattested_peers: bool,
    ollama_tls_insecure: bool,
    default_num_predict: Option<u64>,
    default_num_ctx: Option<u64>,
    network_paused: bool,
    network_disconnected: bool,
    gpu_thermal_guard: crate::shared::GpuThermalGuardStatus,
    default_schedule_enabled: Option<bool>,
    default_schedule_start: Option<String>,
    default_schedule_end: Option<String>,
    gpu_thermal_guard_enabled: Option<bool>,
    gpu_thermal_threshold_c: Option<u8>,
    gpu_thermal_duration_secs: Option<u64>,
    gpu_thermal_cooldown_c: Option<u8>,
    gpu_thermal_auto_resume: Option<bool>,
}

#[derive(Serialize)]
struct ConnectionsView {
    outbound_count: usize,
    inbound_count: usize,
    peers: Vec<crate::shared::PeerConnectionView>,
}

fn client_display_url(proxy_bind: &str, proxy_port: u16) -> String {
    let host = match proxy_bind {
        "0.0.0.0" | "::" | "[::]" => "127.0.0.1",
        other => other,
    };
    format!("http://{host}:{proxy_port}")
}

async fn get_status(State(state): State<ProxyState>) -> Json<StatusResponse> {
    sync_swarm_status_for_proxy(&state).await;
    let cfg = state.client_config.read().await.clone();
    let service_id_for_credits = {
        let app = state.shared_state.lock().await;
        app.service_id.clone()
    };

    if !service_id_for_credits.is_empty() {
        if let Some(fresh) =
            fetch_service_credits(&state.http_client, &cfg.lobby_host, &service_id_for_credits)
                .await
        {
            state.shared_state.lock().await.credit_balance = fresh;
        }
    }

    {
        let reachable = probe_lobby_reachable(&state.http_client, &cfg.lobby_host).await;
        state.shared_state.lock().await.lobby_connected = reachable;
    }

    sync_peer_connections_with_store(
        &state.shared_state,
        &state.peer_registry,
        &state.tx_store,
        &state.peer_stats,
    )
    .await;

    let (
        peer_id,
        service_id,
        clusters,
        swarms,
        credit_balance,
        lobby_connected,
        local_models,
        network_models,
        gpu_host,
        gpu_history,
        peer_connections,
        outgoing_model_requests,
        incoming_model_offers,
        incoming_connection_offers,
        local_model_runs,
        local_model_collisions,
        gpu_thermal_guard,
    ) = {
        let app = state.shared_state.lock().await;
        (
            app.peer_id.clone(),
            app.service_id.clone(),
            app.clusters.clone(),
            app.swarms.clone(),
            app.credit_balance,
            app.lobby_connected,
            app.local_models_full.clone(),
            app.network_models.clone(),
            app.last_gpu_host.clone(),
            app.gpu_history.clone(),
            app.peer_connections.clone(),
            app.outgoing_model_requests.clone(),
            app.incoming_model_offers.clone(),
            app.incoming_connection_offers.clone(),
            app.local_model_runs.clone(),
            app.local_model_collisions.clone(),
            app.gpu_thermal_guard.clone(),
        )
    };

    let llm_servers = state.llm_registry.inner.server_views(&cfg).await;
    let attached_server_count = llm_servers.iter().filter(|s| s.attached).count();
    let lobby_host = cfg.lobby_host.clone();
    let lobby_api_url = crate::lobby_url::lobby_http_base(&lobby_host);
    let proxy_port = state.proxy_port;
    let proxy_bind = state.proxy_bind.clone();
    let client_url = client_display_url(&proxy_bind, proxy_port);

    Json(StatusResponse {
        peer_id,
        service_id,
        service_name: cfg.service_name.clone().unwrap_or_default(),
        lobby_host,
        lobby_api_url,
        proxy_port,
        proxy_bind,
        client_url,
        clusters,
        swarms,
        p2p_mode: cfg.p2p_mode,
        credit_balance,
        lobby_connected,
        setup_complete: cfg.setup_complete,
        llm_servers,
        attached_server_count,
        local_model_collisions,
        local_models,
        network_models,
        hidden_models: cfg.hidden_models.clone(),
        gpu_host,
        gpu_history,
        connections: ConnectionsView {
            outbound_count: peer_connections
                .iter()
                .filter(|p| p.direction == "outbound")
                .count(),
            inbound_count: peer_connections
                .iter()
                .filter(|p| p.direction == "inbound")
                .count(),
            peers: peer_connections,
        },
        model_start_requests: outgoing_model_requests,
        incoming_model_offers,
        incoming_connection_offers,
        local_model_runs,
        auto_approve_run_model_request: cfg.auto_approve_run_model_request,
        auto_approve_inference_connections: cfg.auto_approve_inference_connections,
        allow_unattested_peers: cfg.allow_unattested_peers,
        ollama_tls_insecure: cfg.ollama_tls_insecure,
        default_num_predict: cfg.default_num_predict,
        default_num_ctx: cfg.default_num_ctx,
        network_paused: crate::network_actions::network_all_paused(&cfg),
        network_disconnected: crate::network_actions::network_all_disconnected(&cfg),
        gpu_thermal_guard,
        default_schedule_enabled: cfg.default_schedule_enabled,
        default_schedule_start: cfg.default_schedule_start.clone(),
        default_schedule_end: cfg.default_schedule_end.clone(),
        gpu_thermal_guard_enabled: cfg.gpu_thermal_guard_enabled,
        gpu_thermal_threshold_c: cfg.gpu_thermal_threshold_c,
        gpu_thermal_duration_secs: cfg.gpu_thermal_duration_secs,
        gpu_thermal_cooldown_c: cfg.gpu_thermal_cooldown_c,
        gpu_thermal_auto_resume: cfg.gpu_thermal_auto_resume,
    })
}

#[derive(Serialize)]
struct ChatModelsResponse {
    models: Vec<crate::chat_models::ChatModelEntry>,
}

async fn get_chat_models(State(state): State<ProxyState>) -> Json<ChatModelsResponse> {
    let cfg = state.client_config.read().await.clone();
    let (local_models, network_models, clusters, swarms) = {
        let app = state.shared_state.lock().await;
        (
            app.local_models_full.clone(),
            app.network_models.clone(),
            app.clusters.clone(),
            app.swarms.clone(),
        )
    };
    let llm_servers = state.llm_registry.inner.server_views(&cfg).await;
    let servers: Vec<(String, String, Option<String>)> = llm_servers
        .iter()
        .map(|s| (s.id.clone(), s.kind.clone(), s.label.clone()))
        .collect();

    let mut models =
        crate::chat_models::build_chat_models(&local_models, &network_models, &servers);

    for entry in &mut models {
        if entry.source_kind == "cluster" {
            if let Some(c) = clusters.iter().find(|c| c.cluster_id == entry.source_label) {
                if let Some(name) = c.name.as_deref().filter(|n| !n.is_empty()) {
                    entry.source_label = name.to_string();
                }
            }
        } else if entry.source_kind == "swarm" {
            if let Some(s) = swarms.iter().find(|s| s.swarm_id == entry.source_label) {
                if let Some(name) = s.name.as_deref().filter(|n| !n.is_empty()) {
                    entry.source_label = name.to_string();
                }
            }
        }
    }

    Json(ChatModelsResponse { models })
}

#[derive(Serialize)]
struct ChatSessionsResponse {
    sessions: Vec<crate::tx_db::ChatSessionSummary>,
}

async fn get_chat_sessions(
    State(state): State<ProxyState>,
) -> Result<Json<ChatSessionsResponse>, (StatusCode, String)> {
    let sessions = state
        .tx_store
        .list_chat_sessions()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ChatSessionsResponse { sessions }))
}

#[derive(Deserialize)]
struct CreateChatSessionBody {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

async fn post_chat_session(
    State(state): State<ProxyState>,
    Json(body): Json<CreateChatSessionBody>,
) -> Result<Json<crate::tx_db::ChatSessionView>, (StatusCode, String)> {
    let id = Uuid::new_v4().to_string();
    let title = body
        .title
        .filter(|t| !t.trim().is_empty())
        .unwrap_or_else(|| "New chat".to_string());
    let model = body.model.unwrap_or_default();
    let session = state
        .tx_store
        .upsert_chat_session(id, title, model, Vec::new(), None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(session))
}

async fn get_chat_session(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
) -> Result<Json<crate::tx_db::ChatSessionView>, (StatusCode, String)> {
    match state
        .tx_store
        .get_chat_session(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some(session) => Ok(Json(session)),
        None => Err((StatusCode::NOT_FOUND, "chat session not found".to_string())),
    }
}

#[derive(Deserialize)]
struct PutChatSessionBody {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    messages: Vec<crate::tx_db::ChatMessage>,
}

fn auto_chat_title(messages: &[crate::tx_db::ChatMessage], current: &str) -> String {
    let blank = current.trim().is_empty() || current == "New chat";
    if !blank {
        return current.to_string();
    }
    let Some(first_user) = messages.iter().find(|m| m.role == "user") else {
        return if current.trim().is_empty() {
            "New chat".to_string()
        } else {
            current.to_string()
        };
    };
    let trimmed = first_user.content.trim();
    if trimmed.is_empty() {
        return "New chat".to_string();
    }
    let mut title: String = trimmed.chars().take(48).collect();
    if trimmed.chars().count() > 48 {
        title.push('…');
    }
    title
}

async fn put_chat_session(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
    Json(body): Json<PutChatSessionBody>,
) -> Result<Json<crate::tx_db::ChatSessionView>, (StatusCode, String)> {
    let existing = state
        .tx_store
        .get_chat_session(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (created_at, prev_title, prev_model) = match &existing {
        Some(s) => (Some(s.created_at_unix), s.title.clone(), s.model.clone()),
        None => (None, "New chat".to_string(), String::new()),
    };

    let title_input = body.title.unwrap_or(prev_title);
    let title = auto_chat_title(&body.messages, &title_input);
    let model = body
        .model
        .filter(|m| !m.is_empty())
        .unwrap_or(prev_model);

    let session = state
        .tx_store
        .upsert_chat_session(id, title, model, body.messages, created_at)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(session))
}

async fn delete_chat_session(
    State(state): State<ProxyState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let deleted = state
        .tx_store
        .delete_chat_session(&id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "chat session not found".to_string()))
    }
}

#[derive(Serialize)]
struct BlockedPeersResponse {
    peers: Vec<crate::tx_db::BlockedPeerView>,
}

async fn get_blocked_peers(State(state): State<ProxyState>) -> Json<BlockedPeersResponse> {
    let peers = state
        .tx_store
        .list_blocked_peers()
        .await
        .unwrap_or_default();
    Json(BlockedPeersResponse { peers })
}

#[derive(Deserialize)]
struct BlockPeerBody {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Serialize)]
struct BlockPeerResponse {
    peer_id: String,
    blocked: bool,
}

async fn post_block_peer(
    State(state): State<ProxyState>,
    Path(peer_id): Path<String>,
    body: Option<Json<BlockPeerBody>>,
) -> Result<Json<BlockPeerResponse>, StatusCode> {
    let local_peer_id = state.shared_state.lock().await.peer_id.clone();
    if peer_id == local_peer_id {
        return Err(StatusCode::BAD_REQUEST);
    }
    let reason = body.and_then(|b| b.reason.clone());
    state
        .tx_store
        .block_peer(&peer_id, "local", reason.as_deref())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let _ = state
        .peer_moderation_tx
        .send(crate::shared::PeerModerationAction::CloseConnections {
            peer_id: peer_id.clone(),
        })
        .await;
    Ok(Json(BlockPeerResponse {
        peer_id,
        blocked: true,
    }))
}

async fn delete_block_peer(
    State(state): State<ProxyState>,
    Path(peer_id): Path<String>,
) -> Result<Json<BlockPeerResponse>, StatusCode> {
    let removed = state
        .tx_store
        .unblock_peer(&peer_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !removed {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(BlockPeerResponse {
        peer_id,
        blocked: false,
    }))
}

#[derive(Deserialize)]
struct ReportPeerBody {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Serialize)]
struct ReportPeerResponse {
    peer_id: String,
    reported: bool,
    blocked_locally: bool,
}

async fn post_report_peer(
    State(state): State<ProxyState>,
    Path(peer_id): Path<String>,
    body: Option<Json<ReportPeerBody>>,
) -> Result<Json<ReportPeerResponse>, StatusCode> {
    let local_peer_id = state.shared_state.lock().await.peer_id.clone();
    if peer_id == local_peer_id {
        return Err(StatusCode::BAD_REQUEST);
    }
    let reason = body
        .and_then(|b| b.reason.clone())
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty());
    let Some(reason) = reason else {
        return Err(StatusCode::BAD_REQUEST);
    };
    state
        .tx_store
        .block_peer(&peer_id, "local", Some(reason.as_str()))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let _ = state
        .peer_moderation_tx
        .send(crate::shared::PeerModerationAction::Report {
            peer_id: peer_id.clone(),
            reason: Some(reason),
        })
        .await;
    Ok(Json(ReportPeerResponse {
        peer_id,
        reported: true,
        blocked_locally: true,
    }))
}

#[derive(Deserialize)]
struct TransactionsQuery {
    #[serde(default = "default_tx_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_tx_limit() -> usize {
    50
}

async fn get_transactions(
    State(state): State<ProxyState>,
    Query(params): Query<TransactionsQuery>,
) -> Result<Json<Vec<crate::tx_db::TransactionListItem>>, (StatusCode, String)> {
    let limit = params.limit.clamp(1, 500);
    state
        .tx_store
        .list_transactions(limit, params.offset)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn get_transaction_stats(
    State(state): State<ProxyState>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<crate::tx_db::TransactionStats>, (StatusCode, String)> {
    let local_peer_id = state.shared_state.lock().await.peer_id.clone();
    state
        .tx_store
        .stats(&local_peer_id, params.since_unix)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Deserialize)]
struct StatsQuery {
    since_unix: Option<u64>,
}

#[derive(Serialize)]
struct SettingsResponse {
    auto_approve_run_model_request: bool,
    auto_approve_inference_connections: bool,
    allow_unattested_peers: bool,
    ollama_tls_insecure: bool,
    default_num_predict: Option<u64>,
    default_num_ctx: Option<u64>,
    default_schedule_enabled: Option<bool>,
    default_schedule_start: Option<String>,
    default_schedule_end: Option<String>,
    gpu_thermal_guard_enabled: Option<bool>,
    gpu_thermal_threshold_c: Option<u8>,
    gpu_thermal_duration_secs: Option<u64>,
    gpu_thermal_cooldown_c: Option<u8>,
    gpu_thermal_auto_resume: Option<bool>,
}

#[derive(Deserialize)]
struct SettingsUpdateBody {
    auto_approve_run_model_request: Option<bool>,
    auto_approve_inference_connections: Option<bool>,
    allow_unattested_peers: Option<bool>,
    ollama_tls_insecure: Option<bool>,
    default_num_predict: Option<Option<u64>>,
    default_num_ctx: Option<Option<u64>>,
    default_schedule_enabled: Option<bool>,
    default_schedule_start: Option<String>,
    default_schedule_end: Option<String>,
    gpu_thermal_guard_enabled: Option<bool>,
    gpu_thermal_threshold_c: Option<u8>,
    gpu_thermal_duration_secs: Option<u64>,
    gpu_thermal_cooldown_c: Option<u8>,
    gpu_thermal_auto_resume: Option<bool>,
}

async fn get_settings(State(state): State<ProxyState>) -> Json<SettingsResponse> {
    let cfg = state.client_config.read().await;
    Json(SettingsResponse {
        auto_approve_run_model_request: cfg.auto_approve_run_model_request,
        auto_approve_inference_connections: cfg.auto_approve_inference_connections,
        allow_unattested_peers: cfg.allow_unattested_peers,
        ollama_tls_insecure: cfg.ollama_tls_insecure,
        default_num_predict: cfg.default_num_predict,
        default_num_ctx: cfg.default_num_ctx,
        default_schedule_enabled: cfg.default_schedule_enabled,
        default_schedule_start: cfg.default_schedule_start.clone(),
        default_schedule_end: cfg.default_schedule_end.clone(),
        gpu_thermal_guard_enabled: cfg.gpu_thermal_guard_enabled,
        gpu_thermal_threshold_c: cfg.gpu_thermal_threshold_c,
        gpu_thermal_duration_secs: cfg.gpu_thermal_duration_secs,
        gpu_thermal_cooldown_c: cfg.gpu_thermal_cooldown_c,
        gpu_thermal_auto_resume: cfg.gpu_thermal_auto_resume,
    })
}

async fn put_settings(
    State(state): State<ProxyState>,
    Json(body): Json<SettingsUpdateBody>,
) -> Result<Json<SettingsResponse>, (StatusCode, String)> {
    let mut cfg = state.client_config.write().await;
    if let Some(v) = body.auto_approve_run_model_request {
        cfg.auto_approve_run_model_request = v;
    }
    if let Some(v) = body.auto_approve_inference_connections {
        cfg.auto_approve_inference_connections = v;
    }
    if let Some(v) = body.allow_unattested_peers {
        cfg.allow_unattested_peers = v;
    }
    let tls_changed = if let Some(v) = body.ollama_tls_insecure {
        let changed = cfg.ollama_tls_insecure != v;
        cfg.ollama_tls_insecure = v;
        changed
    } else {
        false
    };
    if let Some(v) = body.default_num_predict {
        cfg.default_num_predict = v.filter(|&n| n > 0);
    }
    if let Some(v) = body.default_num_ctx {
        cfg.default_num_ctx = v.filter(|&n| n > 0);
    }
    if let Some(v) = body.default_schedule_enabled {
        cfg.default_schedule_enabled = Some(v);
    }
    if let Some(v) = body.default_schedule_start {
        cfg.default_schedule_start = Some(v);
    }
    if let Some(v) = body.default_schedule_end {
        cfg.default_schedule_end = Some(v);
    }
    if let Some(v) = body.gpu_thermal_guard_enabled {
        cfg.gpu_thermal_guard_enabled = Some(v);
    }
    if let Some(v) = body.gpu_thermal_threshold_c {
        cfg.gpu_thermal_threshold_c = Some(v);
    }
    if let Some(v) = body.gpu_thermal_duration_secs {
        cfg.gpu_thermal_duration_secs = Some(v);
    }
    if let Some(v) = body.gpu_thermal_cooldown_c {
        cfg.gpu_thermal_cooldown_c = Some(v);
    }
    if let Some(v) = body.gpu_thermal_auto_resume {
        cfg.gpu_thermal_auto_resume = Some(v);
    }
    save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    drop(cfg);
    if tls_changed {
        state
            .refresh_ollama_tls_client()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    let cfg = state.client_config.read().await;
    Ok(Json(SettingsResponse {
        auto_approve_run_model_request: cfg.auto_approve_run_model_request,
        auto_approve_inference_connections: cfg.auto_approve_inference_connections,
        allow_unattested_peers: cfg.allow_unattested_peers,
        ollama_tls_insecure: cfg.ollama_tls_insecure,
        default_num_predict: cfg.default_num_predict,
        default_num_ctx: cfg.default_num_ctx,
        default_schedule_enabled: cfg.default_schedule_enabled,
        default_schedule_start: cfg.default_schedule_start.clone(),
        default_schedule_end: cfg.default_schedule_end.clone(),
        gpu_thermal_guard_enabled: cfg.gpu_thermal_guard_enabled,
        gpu_thermal_threshold_c: cfg.gpu_thermal_threshold_c,
        gpu_thermal_duration_secs: cfg.gpu_thermal_duration_secs,
        gpu_thermal_cooldown_c: cfg.gpu_thermal_cooldown_c,
        gpu_thermal_auto_resume: cfg.gpu_thermal_auto_resume,
    }))
}

#[derive(Deserialize)]
struct CatalogQuery {
    q: Option<String>,
    limit: Option<i64>,
    min_vram_mb: Option<i64>,
    max_vram_mb: Option<i64>,
}

async fn require_hf_inference_cell_server(
    state: &ProxyState,
    server_id: &str,
) -> Result<(), (StatusCode, String)> {
    let cfg = state.client_config.read().await;
    let entry = cfg
        .llm_servers
        .iter()
        .find(|s| s.id == server_id)
        .ok_or((StatusCode::BAD_REQUEST, "server not found".to_string()))?;
    if !is_inference_cell_kind(&entry.kind) {
        return Err((
            StatusCode::BAD_REQUEST,
            "HF catalog requires a llama.cpp inference-cell server".to_string(),
        ));
    }
    drop(cfg);

    let engine = state
        .llm_registry
        .inner
        .inference_engine_for_server(server_id)
        .await;
    let ollama_backend = state
        .llm_registry
        .inner
        .backend_for_server(server_id)
        .await
        .is_some_and(|backend| backend.supports_ollama_native());
    if engine.as_deref() == Some("ollama") || (engine.is_none() && ollama_backend) {
        return Err((
            StatusCode::BAD_REQUEST,
            "HF catalog is for llama.cpp inference-cell; use Ollama library catalog for Ollama engine".to_string(),
        ));
    }

    crate::inference_cell_run::find_inference_cell_server(
        &*state.client_config.read().await,
        server_id,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

async fn get_hf_models_catalog(
    State(state): State<ProxyState>,
    Query(params): Query<HfCatalogQuery>,
) -> Result<Json<Vec<crate::hf_catalog::HfCatalogEntry>>, (StatusCode, String)> {
    if let Some(server_id) = params.server_id.as_deref().filter(|s| !s.is_empty()) {
        require_hf_inference_cell_server(&state, server_id).await?;
    }

    let q = params.q.unwrap_or_default();
    let limit = params.limit.unwrap_or(50);
    let hf_token = crate::hf_catalog::effective_hf_token();
    crate::hf_catalog::search_hf_gguf_models(&state.http_client, &q, limit, hf_token.as_deref())
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
        .map(Json)
}

#[derive(Deserialize)]
struct HfCatalogQuery {
    q: Option<String>,
    limit: Option<i64>,
    server_id: Option<String>,
}

#[derive(Deserialize)]
struct HfQuantsQuery {
    repo: String,
    server_id: Option<String>,
}

async fn get_hf_model_quants(
    State(state): State<ProxyState>,
    Query(params): Query<HfQuantsQuery>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    if params.repo.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "repo required".to_string()));
    }
    if let Some(server_id) = params.server_id.as_deref().filter(|s| !s.is_empty()) {
        require_hf_inference_cell_server(&state, server_id).await?;
    }

    let hf_token = crate::hf_catalog::effective_hf_token();
    crate::hf_catalog::list_hf_quants(&state.http_client, params.repo.trim(), hf_token.as_deref())
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
        .map(Json)
}

async fn get_models_catalog(
    State(state): State<ProxyState>,
    Query(params): Query<CatalogQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let cfg = state.client_config.read().await;
    let q = params.q.unwrap_or_default();
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let url = crate::lobby_url::lobby_api_url(&cfg.lobby_host, "/api/public/models/catalog");
    let mut query: Vec<(&str, String)> = vec![("q", q), ("limit", limit.to_string())];
    if let Some(min) = params.min_vram_mb {
        query.push(("min_vram_mb", min.to_string()));
    }
    if let Some(max) = params.max_vram_mb {
        query.push(("max_vram_mb", max.to_string()));
    }
    let resp = state
        .http_client
        .get(&url)
        .query(&query)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("lobby catalog error: {}", resp.status()),
        ));
    }
    resp.json()
        .await
        .map(Json)
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
}

async fn get_models_catalog_entry(
    State(state): State<ProxyState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let cfg = state.client_config.read().await;
    let encoded: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c.to_string()
            } else {
                format!("%{:02X}", c as u8)
            }
        })
        .collect();
    let url = crate::lobby_url::lobby_api_url(
        &cfg.lobby_host,
        &format!("/api/public/models/catalog/{encoded}"),
    );
    let resp = state
        .http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err((StatusCode::NOT_FOUND, "Model not found in catalog".into()));
    }
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("lobby catalog error: {}", resp.status()),
        ));
    }
    resp.json()
        .await
        .map(Json)
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
}

async fn delete_local_model(
    State(state): State<ProxyState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    local_model_action(&state, &name, LocalModelAction::Delete).await
}

async fn load_local_model(
    State(state): State<ProxyState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    local_model_action(&state, &name, LocalModelAction::Load).await
}

async fn unload_local_model(
    State(state): State<ProxyState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    local_model_action(&state, &name, LocalModelAction::Unload).await
}

#[derive(Deserialize)]
struct ModelVisibilityBody {
    name: String,
    visible: bool,
}

#[derive(Deserialize)]
struct ModelVisibilityBulkBody {
    visible: bool,
}

#[derive(Serialize)]
struct ModelVisibilityResponse {
    hidden_models: Vec<String>,
}

fn set_model_hidden(hidden: &mut Vec<String>, name: &str, hide: bool) {
    let name = name.trim();
    if name.is_empty() {
        return;
    }
    if hide {
        if !hidden.iter().any(|n| n == name) {
            hidden.push(name.to_string());
        }
    } else {
        hidden.retain(|n| n != name);
    }
}

fn collect_unified_model_names(state: &crate::shared::AppState) -> Vec<String> {
    use std::collections::BTreeSet;
    let mut names = BTreeSet::new();
    for m in &state.local_models_full {
        if let Some(n) = m.get("name").and_then(|v| v.as_str()) {
            if !n.is_empty() {
                names.insert(n.to_string());
            }
        }
    }
    for m in &state.network_models {
        if let Some(n) = m.get("name").and_then(|v| v.as_str()) {
            if !n.is_empty() {
                names.insert(n.to_string());
            }
        }
    }
    names.into_iter().collect()
}

async fn patch_model_visibility(
    State(state): State<ProxyState>,
    Json(body): Json<ModelVisibilityBody>,
) -> Result<Json<ModelVisibilityResponse>, (StatusCode, String)> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name is required".to_string()));
    }
    let mut cfg = state.client_config.write().await;
    set_model_hidden(&mut cfg.hidden_models, &name, !body.visible);
    save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ModelVisibilityResponse {
        hidden_models: cfg.hidden_models.clone(),
    }))
}

async fn post_model_visibility_bulk(
    State(state): State<ProxyState>,
    Json(body): Json<ModelVisibilityBulkBody>,
) -> Result<Json<ModelVisibilityResponse>, (StatusCode, String)> {
    let catalog_names = {
        let app = state.shared_state.lock().await;
        collect_unified_model_names(&app)
    };
    let mut cfg = state.client_config.write().await;
    if body.visible {
        cfg.hidden_models.clear();
    } else {
        cfg.hidden_models = catalog_names;
    }
    save_client_config(&cfg).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ModelVisibilityResponse {
        hidden_models: cfg.hidden_models.clone(),
    }))
}

enum LocalModelAction {
    Delete,
    Load,
    Unload,
}

async fn model_show_info(state: &ProxyState, name: &str) -> Option<Value> {
    let cached = {
        let app = state.shared_state.lock().await;
        app.local_models_full
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(name))
            .and_then(|m| m.get("_show_info").cloned())
    };
    if cached.is_some() {
        return cached;
    }
    let backend = state
        .llm_registry
        .inner
        .resolve_ollama_backend_for_model(name)
        .await?;
    let client = state.ollama_http_client.read().await;
    fetch_show_info(&client, backend.base_url(), name)
        .await
        .ok()
}

async fn local_model_action(
    state: &ProxyState,
    name: &str,
    action: LocalModelAction,
) -> Result<StatusCode, (StatusCode, String)> {
    let meta = {
        let st = state.shared_state.lock().await;
        st.local_models_full
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(name))
            .cloned()
    };

    let kind = meta
        .as_ref()
        .and_then(|m| m.get("_source_kind"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if kind == "inference-cell" {
        return inference_cell_model_action(state, name, action, meta).await;
    }

    let backend = state
        .llm_registry
        .inner
        .resolve_ollama_backend_for_model(name)
        .await
        .ok_or((
            StatusCode::BAD_REQUEST,
            "No Ollama backend available for this model".into(),
        ))?;
    if !backend.supports_ollama_native() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Load, unload, and delete are only supported for Ollama models".into(),
        ));
    }

    match action {
        LocalModelAction::Delete => {
            let resp = backend
                .forward_delete_raw("/api/delete", name)
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!("Ollama delete failed: {status} {text}"),
                ));
            }
        }
        LocalModelAction::Load => {
            let show = model_show_info(state, name).await;
            let show_ref = show.as_ref();
            if is_embed_only(show_ref) {
                let body = serde_json::json!({
                    "model": name,
                    "input": ".",
                    "keep_alive": "30m"
                })
                .to_string();
                let resp = backend
                    .forward_post_raw("/api/embed", body)
                    .await
                    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        format!("Ollama embed load failed: {status} {text}"),
                    ));
                }
            } else if !supports_generate(show_ref) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("{name} is an embedding model — use /api/embed, not chat generation"),
                ));
            } else {
                let body = serde_json::json!({
                    "model": name,
                    "prompt": ".",
                    "stream": false,
                    "keep_alive": "30m"
                })
                .to_string();
                let resp = backend
                    .forward_post_raw("/api/generate", body)
                    .await
                    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        format!("Ollama load failed: {status} {text}"),
                    ));
                }
            }
        }
        LocalModelAction::Unload => {
            let show = model_show_info(state, name).await;
            let show_ref = show.as_ref();
            if is_embed_only(show_ref) || (supports_embed(show_ref) && !supports_generate(show_ref))
            {
                let body = serde_json::json!({
                    "model": name,
                    "input": ".",
                    "keep_alive": 0
                })
                .to_string();
                let resp = backend
                    .forward_post_raw("/api/embed", body)
                    .await
                    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        format!("Ollama embed unload failed: {status} {text}"),
                    ));
                }
            } else if !supports_generate(show_ref) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("{name} does not support generate unload"),
                ));
            } else {
                let body = serde_json::json!({
                    "model": name,
                    "prompt": ".",
                    "stream": false,
                    "keep_alive": 0
                })
                .to_string();
                let resp = backend
                    .forward_post_raw("/api/generate", body)
                    .await
                    .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        format!("Ollama unload failed: {status} {text}"),
                    ));
                }
            }
        }
    }

    let cfg = state.client_config.read().await.clone();
    sync_registry_after_config_change(state, &cfg).await;
    state.bump_cluster_state();
    Ok(StatusCode::NO_CONTENT)
}

async fn inference_cell_model_action(
    state: &ProxyState,
    name: &str,
    action: LocalModelAction,
    meta: Option<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    let server_id = meta
        .as_ref()
        .and_then(|m| m.get("_source_server"))
        .and_then(|v| v.as_str())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "Missing inference-cell source server id".to_string(),
        ))?;
    let base_url = meta
        .as_ref()
        .and_then(|m| m.get("_source_url"))
        .and_then(|v| v.as_str())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "Missing inference-cell source URL".to_string(),
        ))?
        .trim_end_matches('/')
        .to_string();

    let cfg = state.client_config.read().await;
    let admin_token = state
        .tx_store
        .get_server_admin_token(server_id, cfg.peer_id.as_deref(), cfg.service_id.as_deref())
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    drop(cfg);

    let client = state.ollama_http_client.read().await.clone();

    let (url, body) = match action {
        LocalModelAction::Load => (
            format!("{base_url}/mtrxai/v1/models/load"),
            Some(serde_json::json!({ "model": name })),
        ),
        LocalModelAction::Unload => (format!("{base_url}/mtrxai/v1/models/unload"), None),
        LocalModelAction::Delete => (
            format!("{base_url}/mtrxai/v1/models/delete"),
            Some(serde_json::json!({ "filename": name })),
        ),
    };

    let mut req = client.post(url);
    if let Some(token) = admin_token.as_deref().filter(|t| !t.is_empty()) {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(body) = body {
        req = req.json(&body);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Inference-cell action failed: {status} {text}"),
        ));
    }

    // Refresh model list immediately so UI updates "loaded" badges.
    if let Ok(catalog) = state
        .llm_registry
        .inner
        .rebuild_catalog(crate::ollama_client::gpu_probe_mode())
        .await
    {
        let mut st = state.shared_state.lock().await;
        st.local_models = catalog.model_names.clone();
        st.local_models_full = catalog.models.clone();
        st.last_gpu_host = catalog.gpu_host.clone();
        st.local_model_collisions = catalog.collisions.clone();
    }
    state.bump_cluster_state();

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ModelRequestBody {
    model: String,
    #[serde(default)]
    cluster_id: Option<String>,
    #[serde(default)]
    swarm_id: Option<String>,
}

#[derive(Serialize)]
struct ModelRequestResponse {
    req_id: String,
}

async fn post_model_request(
    State(state): State<ProxyState>,
    Json(body): Json<ModelRequestBody>,
) -> Result<Json<ModelRequestResponse>, (StatusCode, String)> {
    let req_id = Uuid::new_v4().to_string();
    let action = ModelStartAction::Request {
        req_id: req_id.clone(),
        model: body.model,
        cluster_id: body.cluster_id,
        swarm_id: body.swarm_id.clone(),
    };
    let tx = {
        let app = state.shared_state.lock().await;
        if body.swarm_id.is_some() {
            app.swarm_model_start_tx.clone()
        } else {
            app.model_start_tx.clone()
        }
    };
    tx.send(action)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(Json(ModelRequestResponse { req_id }))
}

#[derive(Deserialize)]
struct ModelRespondBody {
    req_id: String,
    accept: bool,
    #[serde(default)]
    cluster_id: Option<String>,
    #[serde(default)]
    swarm_id: Option<String>,
}

async fn post_model_respond(
    State(state): State<ProxyState>,
    Json(body): Json<ModelRespondBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tx = {
        let app = state.shared_state.lock().await;
        if body.swarm_id.is_some() {
            app.swarm_model_start_tx.clone()
        } else {
            app.model_start_tx.clone()
        }
    };
    tx.send(ModelStartAction::Respond {
        req_id: body.req_id,
        accept: body.accept,
        cluster_id: body.cluster_id,
        swarm_id: body.swarm_id,
    })
    .await
    .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct ConnectionRespondBody {
    req_id: String,
    accept: bool,
    #[serde(default)]
    cluster_id: Option<String>,
}

async fn post_connection_respond(
    State(state): State<ProxyState>,
    Json(body): Json<ConnectionRespondBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tx = state.shared_state.lock().await.connection_tx.clone();
    tx.send(ConnectionAction::Respond {
        req_id: body.req_id,
        accept: body.accept,
        cluster_id: body.cluster_id,
    })
    .await
    .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct ModelRunLocalBody {
    model: String,
    server_id: Option<String>,
    hf_repo: Option<String>,
    quant: Option<String>,
}

#[derive(Serialize)]
struct ModelRunLocalResponse {
    job_id: String,
}

async fn post_model_run_local(
    State(state): State<ProxyState>,
    Json(body): Json<ModelRunLocalBody>,
) -> Result<Json<ModelRunLocalResponse>, (StatusCode, String)> {
    if state.llm_registry.inner.attached_count().await == 0 {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "No LLM servers attached".to_string(),
        ));
    }

    if let Some(server_id) = body.server_id.as_deref().filter(|s| !s.is_empty()) {
        let cfg = state.client_config.read().await;
        let entry = cfg
            .llm_servers
            .iter()
            .find(|s| s.id == server_id)
            .ok_or((StatusCode::BAD_REQUEST, "server not found".to_string()))?
            .clone();
        if !is_inference_cell_kind(&entry.kind) {
            return Err((
                StatusCode::BAD_REQUEST,
                "server_id is only valid for inference-cell servers".to_string(),
            ));
        }
        drop(cfg);

        let engine = state
            .llm_registry
            .inner
            .inference_engine_for_server(server_id)
            .await;
        let ollama_backend = state
            .llm_registry
            .inner
            .backend_for_server(server_id)
            .await
            .is_some_and(|backend| backend.supports_ollama_native());
        let use_ollama_catalog =
            engine.as_deref() == Some("ollama") || (engine.is_none() && ollama_backend);

        if use_ollama_catalog {
            let model = body.model.trim().to_string();
            if model.is_empty() {
                return Err((StatusCode::BAD_REQUEST, "model required".to_string()));
            }
            let job_id = Uuid::new_v4().to_string();
            crate::model_run::spawn_local_model_run_on_server(
                state.shared_state.clone(),
                Arc::new(state.clone()),
                job_id.clone(),
                model,
                Some(server_id.to_string()),
            );
            return Ok(Json(ModelRunLocalResponse { job_id }));
        }

        let cfg = state.client_config.read().await;
        crate::inference_cell_run::find_inference_cell_server(&cfg, server_id)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        drop(cfg);

        let hf_repo = body
            .hf_repo
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                let model = body.model.trim();
                if model.is_empty() {
                    None
                } else {
                    Some(model.to_string())
                }
            })
            .ok_or((StatusCode::BAD_REQUEST, "hf_repo required".to_string()))?;
        let quant = body
            .quant
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("Q4_K_M")
            .to_string();
        let display_name = if body.model.trim().is_empty() {
            hf_repo.clone()
        } else {
            body.model.trim().to_string()
        };

        let job_id = Uuid::new_v4().to_string();
        crate::inference_cell_run::spawn_inference_cell_run(
            state.shared_state.clone(),
            Arc::new(state.clone()),
            job_id.clone(),
            server_id.to_string(),
            hf_repo,
            quant,
            display_name,
        );
        return Ok(Json(ModelRunLocalResponse { job_id }));
    }

    let model = body.model.trim().to_string();
    if model.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "model required".to_string()));
    }
    let job_id = Uuid::new_v4().to_string();
    crate::model_run::spawn_local_model_run(
        state.shared_state.clone(),
        Arc::new(state.clone()),
        job_id.clone(),
        model,
    );
    Ok(Json(ModelRunLocalResponse { job_id }))
}
