//! Path aliases for the peer UI HTTP API.

use axum::{
    body::Body,
    http::{Request, Uri},
    middleware::Next,
    response::Response,
};

/// Rewrite `/api/peer/...` → `/api/client/...` so both prefixes hit the same handlers.
pub async fn alias_api_peer_to_client(mut req: Request<Body>, next: Next) -> Response {
    let uri = req.uri().clone();
    if let Some(rest) = uri.path().strip_prefix("/api/peer") {
        let new_path = format!("/api/client{rest}");
        let pq = match uri.query() {
            Some(q) => format!("{new_path}?{q}"),
            None => new_path,
        };
        if let Ok(new_uri) = pq.parse::<Uri>() {
            *req.uri_mut() = new_uri;
        }
    }
    next.run(req).await
}
