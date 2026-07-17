use crate::shared::PeerInfo;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

const DEFAULT_GEO_URL: &str =
    "http://ip-api.com/json/?fields=status,lat,lon,as,city,country";

#[derive(Debug, Deserialize)]
struct IpApiResponse {
    status: String,
    lat: f64,
    lon: f64,
    #[serde(rename = "as")]
    asn: String,
    city: Option<String>,
    country: Option<String>,
}

pub fn geo_lookup_url() -> String {
    std::env::var("MTRXAI_GEO_LOOKUP_URL").unwrap_or_else(|_| DEFAULT_GEO_URL.to_string())
}

pub async fn discover_peer_location() -> Option<PeerInfo> {
    let url = geo_lookup_url();
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .ok()?;

    for attempt in 0..2 {
        match client.get(&url).send().await {
            Ok(res) if res.status().is_success() => {
                if let Ok(body) = res.json::<IpApiResponse>().await {
                    if body.status == "success" {
                        println!(
                            "🌍 Peer location: {}, {} ({})",
                            body.city.as_deref().unwrap_or("?"),
                            body.country.as_deref().unwrap_or("?"),
                            body.asn
                        );
                        return Some(PeerInfo {
                            lat: body.lat,
                            lon: body.lon,
                            asn: body.asn,
                            city: body.city,
                            country: body.country,
                        });
                    }
                }
            }
            Ok(_) => {}
            Err(e) if attempt == 0 => {
                eprintln!("⚠️ Geo lookup failed (retrying): {}", e);
            }
            Err(e) => {
                eprintln!("⚠️ Geo lookup failed: {}", e);
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    None
}
