mod common;

use peer::agent_compat::{
    normalize_chat_request, rewrite_chat_response, sse_bytes_to_completion, SseTransformState,
};
use serde_json::json;

#[test]
fn converts_input_to_messages() {
    let body = json!({
        "model": "llama3",
        "input": [{"role": "user", "content": "hi"}],
        "tools": [{"type": "function", "name": "run_in_terminal", "parameters": {"type": "object", "properties": {}}}]
    });
    let (out, summary) = normalize_chat_request(body);
    assert!(summary.normalized);
    assert!(out.get("input").is_none());
    assert_eq!(out["messages"][0]["role"], "user");
    assert!(out["tools"][0]["function"].is_object());
}

#[test]
fn rewrites_text_tool_call_in_response() {
    let (out, summary) = rewrite_chat_response(common::text_tool_response());
    assert!(summary.rewrote_text_tool_call);
    let tc = &out["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(tc["function"]["name"], "run_in_terminal");
    assert_eq!(out["choices"][0]["finish_reason"], "tool_calls");
}

#[test]
fn end_to_end_cursor_style_request_and_text_tool_response() {
    let (normalized, req_summary) = normalize_chat_request(common::cursor_style_request());
    assert!(req_summary.normalized);
    assert_eq!(
        normalized["tools"][0]["function"]["name"],
        "run_in_terminal"
    );

    let (final_response, resp_summary) = rewrite_chat_response(common::text_tool_response());
    assert!(resp_summary.rewrote_text_tool_call);
    assert_eq!(
        final_response["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "run_in_terminal"
    );
}

#[test]
fn streaming_suppresses_json_content_until_finish() {
    let mut state = SseTransformState::default();
    let chunk1 = json!({"choices": [{"delta": {"content": "{\""}, "finish_reason": null}]});
    assert!(state.process_line(&format!("data: {}", chunk1)).is_empty());

    let chunk2 = json!({"choices": [{"delta": {"content": "name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]});
    assert!(state.process_line(&format!("data: {}", chunk2)).is_empty());

    let chunk3 = json!({"choices": [{"delta": {}, "finish_reason": "stop"}]});
    let out = state.process_line(&format!("data: {}", chunk3));
    assert!(out.len() >= 2);
    assert!(out.iter().any(|line| line.contains("run_in_terminal")));
    assert!(state.take_summary().unwrap().rewrote_text_tool_call);
}

#[test]
fn parses_sse_stream_into_completion() {
    let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"{\\\"name\\\": \\\"run_in_terminal\\\"\"},\"finish_reason\":null}]}\n\n\
               data: {\"choices\":[{\"delta\":{\"content\": \", \\\"arguments\\\": {\\\"command\\\": \\\"ls\\\"}}\"},\"finish_reason\":null}]}\n\n\
               data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
               data: [DONE]\n\n";
    let completion = sse_bytes_to_completion(sse);
    let (_, summary) = rewrite_chat_response(completion);
    assert!(summary.rewrote_text_tool_call);
}
