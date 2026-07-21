use peer::token_usage::{parse_usage_from_buffer, TokenUsage};

#[test]
fn parses_ollama_done_line() {
    let line = r#"{"model":"llama3","done":true,"prompt_eval_count":10,"eval_count":25}"#;
    let usage = parse_usage_from_buffer(line).unwrap();
    assert_eq!(usage.prompt_tokens, 10);
    assert_eq!(usage.completion_tokens, 25);
    assert_eq!(usage.total_tokens, 35);
}

#[test]
fn parses_openai_usage() {
    let line =
        r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":15,"total_tokens":20}}"#;
    let usage = parse_usage_from_buffer(line).unwrap();
    assert_eq!(usage.total_tokens, 20);
}

#[test]
fn returns_none_for_malformed_json() {
    assert!(parse_usage_from_buffer("not json").is_none());
}

#[test]
fn returns_none_for_incomplete_ollama_line() {
    let line = r#"{"model":"llama3","done":false,"prompt_eval_count":10,"eval_count":25}"#;
    assert!(parse_usage_from_buffer(line).is_none());
}

#[test]
fn returns_none_for_zero_tokens() {
    let line = r#"{"model":"llama3","done":true,"prompt_eval_count":0,"eval_count":0}"#;
    assert!(parse_usage_from_buffer(line).is_none());
}

#[test]
fn accumulate_prefers_latest_usage() {
    let mut buf = String::new();
    let chunk1 = r#"{"done":false}
"#;
    let chunk2 = r#"{"done":true,"prompt_eval_count":3,"eval_count":7}
"#;
    let usage = peer::token_usage::accumulate_and_parse_usage(&mut buf, chunk1);
    assert!(usage.is_none());
    let usage = peer::token_usage::accumulate_and_parse_usage(&mut buf, chunk2).unwrap();
    assert_eq!(
        usage,
        TokenUsage {
            prompt_tokens: 3,
            completion_tokens: 7,
            total_tokens: 10,
        }
    );
}
