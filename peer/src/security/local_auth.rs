use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

pub fn local_proxy_token() -> Option<String> {
    std::env::var("MTRXAI_PROXY_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn local_proxy_auth_enabled() -> bool {
    local_proxy_token().is_some()
}

pub fn verify_local_proxy_auth(header_value: Option<&str>) -> bool {
    let Some(expected) = local_proxy_token() else {
        return true;
    };
    let Some(raw) = header_value else {
        return false;
    };
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .unwrap_or(raw)
        .trim();
    constant_time_eq(token, &expected)
}

pub async fn local_proxy_auth_middleware(
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if !local_proxy_auth_enabled() {
        return Ok(next.run(req).await);
    }
    let path = req.uri().path();
    if path == "/health" || path.starts_with("/api/client") {
        return Ok(next.run(req).await);
    }
    let auth = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if verify_local_proxy_auth(auth) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}
