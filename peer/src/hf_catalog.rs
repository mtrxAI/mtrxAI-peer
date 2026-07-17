use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const DEFAULT_QUANT: &str = "Q4_K_M";
const GGUF_PROBE_LIMIT: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HfCatalogEntry {
    pub name: String,
    pub description: Option<String>,
    pub repo: String,
    pub quants: Vec<String>,
    pub default_quant: String,
    pub catalog: String,
}

#[derive(Debug, Deserialize)]
struct HfSearchModel {
    id: String,
    #[serde(default, rename = "modelId")]
    model_id: Option<String>,
    #[serde(default)]
    pipeline_tag: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct HfModelInfo {
    #[serde(default)]
    siblings: Vec<HfSibling>,
    #[serde(default, rename = "cardData")]
    card_data: Option<HfCardData>,
}

#[derive(Debug, Deserialize)]
struct HfCardData {
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HfSibling {
    #[serde(rename = "rfilename")]
    rfilename: String,
}

pub fn effective_hf_token() -> Option<String> {
    std::env::var("MTRXAI_HF_TOKEN")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

pub fn extract_quants_from_gguf_names(names: &[String]) -> Vec<String> {
    let mut quants = HashSet::new();
    for name in names {
        let lower = name.to_ascii_lowercase();
        if lower.contains("mmproj") {
            continue;
        }
        for part in name.split('-').flat_map(|s| s.split('_')) {
            let upper = part.to_ascii_uppercase();
            if looks_like_quant(&upper) {
                quants.insert(upper);
            }
        }
        if let Some(quant) = infer_quant_from_filename(name) {
            quants.insert(quant);
        }
    }
    let mut sorted: Vec<String> = quants.into_iter().collect();
    sorted.sort();
    sorted
}

fn looks_like_quant(s: &str) -> bool {
    matches!(
        s,
        "Q2_K"
            | "Q3_K_S"
            | "Q3_K_M"
            | "Q3_K_L"
            | "Q4_0"
            | "Q4_1"
            | "Q4_K_S"
            | "Q4_K_M"
            | "Q5_0"
            | "Q5_1"
            | "Q5_K_S"
            | "Q5_K_M"
            | "Q6_K"
            | "Q8_0"
            | "F16"
            | "F32"
    )
}

fn infer_quant_from_filename(name: &str) -> Option<String> {
    let upper = name.to_ascii_uppercase();
    for quant in [
        "Q8_0", "Q6_K", "Q5_K_M", "Q5_K_S", "Q5_1", "Q5_0", "Q4_K_M", "Q4_K_S", "Q4_1", "Q4_0",
        "Q3_K_L", "Q3_K_M", "Q3_K_S", "Q2_K", "F16", "F32",
    ] {
        if upper.contains(quant) {
            return Some(quant.to_string());
        }
    }
    None
}

pub fn default_quant_for(quants: &[String]) -> String {
    quants
        .iter()
        .find(|q| q.eq_ignore_ascii_case(DEFAULT_QUANT))
        .cloned()
        .or_else(|| quants.first().cloned())
        .unwrap_or_else(|| DEFAULT_QUANT.to_string())
}

pub async fn search_hf_gguf_models(
    client: &Client,
    query: &str,
    limit: i64,
    hf_token: Option<&str>,
) -> Result<Vec<HfCatalogEntry>> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(Vec::new());
    }

    let limit = limit.clamp(1, 50) as usize;
    let mut req = client.get("https://huggingface.co/api/models").query(&[
        ("search", q),
        ("limit", &limit.to_string()),
        ("full", "false"),
    ]);
    if let Some(token) = hf_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let models: Vec<HfSearchModel> = req
        .send()
        .await
        .context("huggingface search request")?
        .error_for_status()
        .context("huggingface search failed")?
        .json()
        .await
        .context("parse huggingface search response")?;

    let mut results = Vec::new();
    for model in models.into_iter().take(GGUF_PROBE_LIMIT) {
        let repo = model.model_id.unwrap_or(model.id);
        if let Some(entry) = build_catalog_entry(client, &repo, hf_token).await? {
            results.push(entry);
        }
        if results.len() >= limit {
            break;
        }
    }

    Ok(results)
}

pub async fn list_hf_quants(
    client: &Client,
    repo: &str,
    hf_token: Option<&str>,
) -> Result<Vec<String>> {
    let ggufs = fetch_gguf_filenames(client, repo, hf_token).await?;
    Ok(extract_quants_from_gguf_names(&ggufs))
}

async fn build_catalog_entry(
    client: &Client,
    repo: &str,
    hf_token: Option<&str>,
) -> Result<Option<HfCatalogEntry>> {
    let ggufs = fetch_gguf_filenames(client, repo, hf_token).await?;
    if ggufs.is_empty() {
        return Ok(None);
    }

    let quants = extract_quants_from_gguf_names(&ggufs);
    if quants.is_empty() {
        return Ok(None);
    }

    let description = fetch_repo_description(client, repo, hf_token).await.ok();
    let default_quant = default_quant_for(&quants);

    Ok(Some(HfCatalogEntry {
        name: repo.to_string(),
        description,
        repo: repo.to_string(),
        quants,
        default_quant,
        catalog: "huggingface".to_string(),
    }))
}

async fn fetch_gguf_filenames(
    client: &Client,
    repo: &str,
    hf_token: Option<&str>,
) -> Result<Vec<String>> {
    validate_repo(repo)?;
    let url = format!("https://huggingface.co/api/models/{repo}");
    let mut req = client.get(&url);
    if let Some(token) = hf_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }

    let info: HfModelInfo = req
        .send()
        .await
        .context("huggingface model metadata request")?
        .error_for_status()
        .context("huggingface model metadata failed")?
        .json()
        .await
        .context("parse huggingface model metadata")?;

    Ok(info
        .siblings
        .into_iter()
        .map(|s| s.rfilename)
        .filter(|name| name.ends_with(".gguf") && !name.to_ascii_lowercase().contains("mmproj"))
        .collect())
}

async fn fetch_repo_description(
    client: &Client,
    repo: &str,
    hf_token: Option<&str>,
) -> Result<String> {
    let url = format!("https://huggingface.co/api/models/{repo}");
    let mut req = client.get(&url);
    if let Some(token) = hf_token {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    let info: HfModelInfo = req.send().await?.error_for_status()?.json().await?;
    info.card_data
        .and_then(|c| c.description)
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| anyhow!("no description"))
}

fn validate_repo(repo: &str) -> Result<()> {
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
        return Err(anyhow!("invalid huggingface repo id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_quants_from_names() {
        let names = vec![
            "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf".to_string(),
            "Meta-Llama-3-8B-Instruct.Q8_0.gguf".to_string(),
        ];
        let quants = extract_quants_from_gguf_names(&names);
        assert!(quants.contains(&"Q4_K_M".to_string()));
        assert!(quants.contains(&"Q8_0".to_string()));
    }

    #[test]
    fn default_quant_prefers_q4_k_m() {
        let quants = vec!["Q8_0".to_string(), "Q4_K_M".to_string()];
        assert_eq!(default_quant_for(&quants), "Q4_K_M");
    }

    #[test]
    fn validate_repo_rejects_invalid() {
        assert!(validate_repo("not-a-repo").is_err());
        assert!(validate_repo("org/repo").is_ok());
    }
}
