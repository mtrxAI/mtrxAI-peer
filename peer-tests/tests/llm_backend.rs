use peer::client_config::ClientConfig;
use peer::llm_backend::{ensure_backend, normalize_backend_label, parse_kind, LlmBackendKind};
use reqwest::Client;

#[test]
fn parse_kind_recognizes_backends() {
    assert_eq!(parse_kind("ollama"), LlmBackendKind::Ollama);
    assert_eq!(parse_kind("Ollama"), LlmBackendKind::Ollama);
    assert_eq!(parse_kind("vllm"), LlmBackendKind::OpenAiCompat);
    assert_eq!(parse_kind("localai"), LlmBackendKind::LocalAi);
    assert_eq!(parse_kind("unknown"), LlmBackendKind::Ollama);
}

#[test]
fn normalize_backend_label_maps_aliases() {
    assert_eq!(normalize_backend_label("vllm"), "vllm");
    assert_eq!(normalize_backend_label("llama.cpp"), "llamacpp");
    assert_eq!(normalize_backend_label("lm-studio"), "lmstudio");
    assert_eq!(normalize_backend_label("openai"), "openai");
    assert_eq!(normalize_backend_label("something-else"), "ollama");
}

#[test]
fn ensure_backend_from_config() {
    let config = ClientConfig {
        llm_backend: Some("ollama".to_string()),
        llm_url: Some("http://127.0.0.1:11434".to_string()),
        ..Default::default()
    };
    let backend = ensure_backend(&config, Client::new()).unwrap();
    assert_eq!(backend.kind_str(), "ollama");
    assert_eq!(backend.base_url(), "http://127.0.0.1:11434");
}

#[test]
fn ensure_backend_requires_url() {
    let config = ClientConfig::default();
    let err = ensure_backend(&config, Client::new()).unwrap_err();
    assert!(err.to_string().contains("LLM URL not configured"));
}
