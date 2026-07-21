use peer::client_config::ClientConfig;
use peer::llm_backend::normalize_backend_label;
use peer::llm_registry::{merge_catalog_snapshots, migrate_config, ModelCollision};
use peer::ollama_client::ModelCatalogSnapshot;
use serde_json::json;

fn entry(id: &str, kind: &str, order: u32) -> peer::client_config::LlmServerEntry {
    peer::client_config::LlmServerEntry {
        id: id.to_string(),
        kind: kind.to_string(),
        url: format!("http://127.0.0.1:{}", 8000 + order),
        label: None,
        attached: true,
        order,
        source: "test".to_string(),
        api_type: None,
        models: Vec::new(),
        advertise_to_cluster: true,
    }
}

fn snap(names: &[&str]) -> ModelCatalogSnapshot {
    ModelCatalogSnapshot {
        models: names.iter().map(|n| json!({ "name": n })).collect(),
        model_names: names.iter().map(|s| s.to_string()).collect(),
        gpu_host: None,
    }
}

#[test]
fn registry_merge_routes_first_server_model() {
    let merged = merge_catalog_snapshots(&[
        (entry("srv-a", "ollama", 0), snap(&["alpha"])),
        (entry("srv-b", "vllm", 1), snap(&["beta"])),
    ]);
    assert_eq!(merged.model_names, vec!["alpha", "beta"]);
    assert!(merged.collisions.is_empty());
    assert_eq!(
        merged.models[1]
            .get("_source_server")
            .and_then(|v| v.as_str()),
        Some("srv-b")
    );
}

#[test]
fn registry_merge_records_collisions() {
    let merged = merge_catalog_snapshots(&[
        (entry("srv-a", "ollama", 0), snap(&["shared"])),
        (entry("srv-b", "vllm", 1), snap(&["shared", "unique"])),
    ]);
    assert_eq!(merged.model_names, vec!["shared", "unique"]);
    assert_eq!(merged.collisions.len(), 1);
    let ModelCollision {
        name,
        skipped_server,
    } = &merged.collisions[0];
    assert_eq!(name, "shared");
    assert!(skipped_server.contains("vllm"));
}

#[test]
fn config_migration_from_legacy_fields() {
    let mut cfg = ClientConfig {
        llm_backend: Some("tensorrt".to_string()),
        llm_url: Some("http://127.0.0.1:8000".to_string()),
        ..ClientConfig::default()
    };
    migrate_config(&mut cfg);
    assert_eq!(cfg.llm_servers.len(), 1);
    assert_eq!(cfg.llm_servers[0].kind, normalize_backend_label("tensorrt"));
}
