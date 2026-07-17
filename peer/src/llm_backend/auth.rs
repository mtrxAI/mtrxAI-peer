use reqwest::RequestBuilder;

pub fn apply_api_key(mut req: RequestBuilder, api_key: Option<&str>) -> RequestBuilder {
    if let Some(key) = api_key.filter(|k| !k.is_empty()) {
        req = req.header("Authorization", format!("Bearer {}", key));
    }
    req
}
