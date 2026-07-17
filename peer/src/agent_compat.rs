//! Request/response normalization for Cursor, VS Code agent extensions, and Ollama.
//!
//! Translates Responses/Anthropic-style tool payloads into OpenAI Chat Completions,
//! rewrites text-based pseudo tool calls into structured `tool_calls`, and fixes
//! streaming content types for agent clients.

use serde_json::{json, Map, Value};
use uuid::Uuid;

const RESPONSES_ONLY_FIELDS: &[&str] = &[
    "store",
    "include",
    "prompt_cache_retention",
    "previous_response_id",
    "truncation",
    "reasoning",
    "text",
    "prompt",
    "metadata",
];

/// Agent client response wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentResponseFormat {
    #[default]
    ChatCompletions,
    ResponsesApi,
}

#[derive(Default, Clone)]
struct StreamChunkMeta {
    id: Option<String>,
    model: Option<String>,
    created: Option<u64>,
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct AgentRequestSummary {
    pub model: Option<String>,
    pub stream: bool,
    pub message_count: usize,
    pub tool_count: usize,
    pub normalized: bool,
    pub had_input_field: bool,
    pub response_format: AgentResponseFormat,
}

/// Summary of a chat response for debug logging.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AgentResponseSummary {
    pub had_structured_tool_calls: bool,
    pub rewrote_text_tool_call: bool,
    pub finish_reason: Option<String>,
    pub content_preview: Option<String>,
}

/// Last captured agent traffic (for `/debug/agent`).
#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct AgentDebugSnapshot {
    pub request: Option<AgentRequestSummary>,
    pub response: Option<AgentResponseSummary>,
}

/// Normalize an incoming `/v1/chat/completions` body for Ollama / OpenAI-compat upstream.
pub fn normalize_chat_request(body: Value) -> (Value, AgentRequestSummary) {
    normalize_chat_request_with_default(
        body,
        default_num_predict_from_env(),
        default_num_ctx_from_env(),
    )
}

pub fn normalize_chat_request_with_default(
    mut body: Value,
    default_predict: Option<u64>,
    default_ctx: Option<u64>,
) -> (Value, AgentRequestSummary) {
    let mut normalized = false;
    let had_input_field = body.get("input").is_some();

    if had_input_field {
        if let Some(input) = body.get("input").cloned() {
            body["messages"] = input_to_messages(input);
            normalized = true;
        }
        if let Some(obj) = body.as_object_mut() {
            obj.remove("input");
        }
    }

    if let Some(tools) = body.get("tools").cloned() {
        let normalized_tools = normalize_tools_array(&tools);
        body["tools"] = normalized_tools;
        normalized = true;
    }

    if strip_responses_only_fields(&mut body) {
        normalized = true;
    }

    normalize_tool_messages(&mut body);
    if ensure_stream_usage(&mut body) {
        normalized = true;
    }
    if ensure_ollama_predict_options_with_default(&mut body, default_predict) {
        normalized = true;
    }
    if ensure_ollama_ctx_options_with_default(&mut body, default_ctx) {
        normalized = true;
    }

    let summary = AgentRequestSummary {
        model: body
            .get("model")
            .and_then(|m| m.as_str())
            .map(str::to_string),
        stream: body.get("stream").and_then(|s| s.as_bool()).unwrap_or(false),
        message_count: body
            .get("messages")
            .and_then(|m| m.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        tool_count: body
            .get("tools")
            .and_then(|t| t.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        normalized,
        had_input_field,
        response_format: AgentResponseFormat::ChatCompletions,
    };

    (body, summary)
}

/// Ensure OpenAI-compatible streaming requests ask for a final usage chunk.
pub fn ensure_stream_usage(body: &mut Value) -> bool {
    if !body.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        return false;
    }
    let Some(obj) = body.as_object_mut() else {
        return false;
    };

    let mut changed = false;
    if !obj.contains_key("stream_options") {
        changed = true;
    }
    let stream_options = obj
        .entry("stream_options")
        .or_insert_with(|| json!({}));
    if let Some(opts) = stream_options.as_object_mut() {
        if !opts.contains_key("include_usage") {
            opts.insert("include_usage".to_string(), json!(true));
            changed = true;
        }
    }
    changed
}

/// Ensure Ollama `/api/chat` and `/api/generate` requests have an explicit output cap.
///
/// Without this, Ollama uses the model Modelfile default (often 4096), which truncates
/// long agent answers mid-stream. Maps OpenAI `max_tokens`, bumps low client
/// `options.num_predict` values up to `MTRXAI_DEFAULT_NUM_PREDICT`, or injects the default
/// when neither is set. Preserves `-1` (unlimited in Ollama).
pub fn ensure_ollama_predict_options(body: &mut Value) -> bool {
    ensure_ollama_predict_options_with_default(body, default_num_predict_from_env())
}

pub fn ensure_ollama_predict_options_with_default(
    body: &mut Value,
    default_predict: Option<u64>,
) -> bool {
    let Some(obj) = body.as_object_mut() else {
        return false;
    };

    let existing = obj
        .get("options")
        .and_then(|o| o.get("num_predict"))
        .and_then(|v| v.as_i64());

    // Ollama uses -1 for unlimited; never override.
    if existing == Some(-1) {
        return false;
    }

    let resolved = if let Some(n) = existing {
        if n <= 0 {
            obj.get("max_tokens")
                .and_then(|v| v.as_u64())
                .or(default_predict)
        } else if let Some(default) = default_predict {
            if (n as u64) >= default {
                return false;
            }
            Some(default)
        } else {
            return false;
        }
    } else {
        obj.get("max_tokens")
            .and_then(|v| v.as_u64())
            .or(default_predict)
    };

    let Some(num_predict) = resolved else {
        return false;
    };

    let options = obj.entry("options").or_insert_with(|| json!({}));
    if let Some(opts) = options.as_object_mut() {
        opts.insert("num_predict".to_string(), json!(num_predict));
        return true;
    }
    false
}

/// Ensure Ollama `/api/chat` and `/api/generate` requests have an explicit context window.
///
/// Without this, Ollama uses the model Modelfile default (often 4096). Long chat histories
/// fill the window and truncate the prompt, leaving no room for generation.
pub fn ensure_ollama_ctx_options(body: &mut Value) -> bool {
    ensure_ollama_ctx_options_with_default(body, default_num_ctx_from_env())
}

pub fn ensure_ollama_ctx_options_with_default(
    body: &mut Value,
    default_ctx: Option<u64>,
) -> bool {
    let Some(default) = default_ctx.filter(|&n| n > 0) else {
        return false;
    };
    let Some(obj) = body.as_object_mut() else {
        return false;
    };

    let existing = obj
        .get("options")
        .and_then(|o| o.get("num_ctx"))
        .and_then(|v| v.as_u64());

    if existing.is_some_and(|n| n >= default) {
        return false;
    }

    let options = obj.entry("options").or_insert_with(|| json!({}));
    if let Some(opts) = options.as_object_mut() {
        opts.insert("num_ctx".to_string(), json!(default));
        return true;
    }
    false
}

/// Parse and apply Ollama output defaults to a raw JSON request body.
pub fn apply_ollama_body_defaults(body: String) -> String {
    apply_ollama_body_defaults_with_default(
        body,
        default_num_predict_from_env(),
        default_num_ctx_from_env(),
    )
}

pub fn apply_ollama_body_defaults_with_default(
    body: String,
    default_predict: Option<u64>,
    default_ctx: Option<u64>,
) -> String {
    let Ok(mut json) = serde_json::from_str::<Value>(&body) else {
        return body;
    };
    ensure_ollama_predict_options_with_default(&mut json, default_predict);
    ensure_ollama_ctx_options_with_default(&mut json, default_ctx);
    serde_json::to_string(&json).unwrap_or(body)
}

fn default_num_ctx_from_env() -> Option<u64> {
    std::env::var("MTRXAI_DEFAULT_NUM_CTX")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
}

fn default_num_predict_from_env() -> Option<u64> {
    std::env::var("MTRXAI_DEFAULT_NUM_PREDICT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
}

fn input_to_messages(input: Value) -> Value {
    match input {
        Value::String(s) => json!([{"role": "user", "content": s}]),
        Value::Array(arr) => Value::Array(arr.into_iter().map(normalize_input_item).collect()),
        other => json!([{"role": "user", "content": other.to_string()}]),
    }
}

fn normalize_input_item(item: Value) -> Value {
    if item.get("role").is_some() {
        return normalize_message_roles(item);
    }
    if let Some(content) = item.get("content") {
        return json!({"role": "user", "content": content});
    }
    json!({"role": "user", "content": item.to_string()})
}

fn normalize_message_roles(mut msg: Value) -> Value {
    if let Some(role) = msg.get("role").and_then(|r| r.as_str()) {
        if role == "developer" {
            msg["role"] = json!("system");
        }
    }
    msg
}

fn normalize_tool_messages(body: &mut Value) {
    let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) else {
        return;
    };

    for msg in messages.iter_mut() {
        *msg = normalize_message_roles(msg.clone());

        // Anthropic tool_result blocks → OpenAI tool role
        if msg.get("role").and_then(|r| r.as_str()) == Some("user") {
            if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                let mut tool_results = Vec::new();
                let mut other_parts = Vec::new();
                for part in content {
                    if part.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                        tool_results.push(json!({
                            "role": "tool",
                            "tool_call_id": part.get("tool_use_id").cloned().unwrap_or(json!("")),
                            "content": part.get("content").cloned().unwrap_or(json!(""))
                        }));
                    } else {
                        other_parts.push(part.clone());
                    }
                }
                if !tool_results.is_empty() && other_parts.is_empty() && tool_results.len() == 1 {
                    *msg = tool_results.remove(0);
                }
            }
        }
    }
}

fn strip_responses_only_fields(body: &mut Value) -> bool {
    let Some(obj) = body.as_object_mut() else {
        return false;
    };
    let mut removed = false;
    for key in RESPONSES_ONLY_FIELDS {
        if obj.remove(*key).is_some() {
            removed = true;
        }
    }
    removed
}

fn normalize_tools_array(tools: &Value) -> Value {
    let Some(arr) = tools.as_array() else {
        return json!([]);
    };
    let out: Vec<Value> = arr.iter().filter_map(normalize_tool).collect();
    Value::Array(out)
}

fn normalize_tool(tool: &Value) -> Option<Value> {
    if tool.get("type").and_then(|t| t.as_str()) == Some("custom") {
        return None;
    }

    if tool.get("function").is_some() {
        let mut t = tool.clone();
        if let Some(func) = t.get_mut("function") {
            if let Some(params) = func.get_mut("parameters") {
                *params = clean_schema(params.clone());
            }
            if func.get("parameters").is_none() {
                if let Some(schema) = func.get("input_schema").cloned() {
                    func.as_object_mut()?.insert("parameters".into(), clean_schema(schema));
                    func.as_object_mut()?.remove("input_schema");
                }
            }
        }
        return Some(t);
    }

    let name = tool
        .get("name")
        .and_then(|n| n.as_str())
        .or_else(|| tool.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()))?;

    let description = tool
        .get("description")
        .or_else(|| tool.get("desc"))
        .cloned()
        .unwrap_or(json!(""));

    let parameters = tool
        .get("parameters")
        .or_else(|| tool.get("input_schema"))
        .cloned()
        .unwrap_or(json!({"type": "object", "properties": {}}));

    Some(json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": clean_schema(parameters)
        }
    }))
}

/// Recursively strip JSON Schema keys that strict OpenAI/Ollama parsers reject.
pub fn clean_schema(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                if k == "$schema" || k == "title" {
                    continue;
                }
                if k == "additionalProperties" && v == json!(false) {
                    continue;
                }
                out.insert(k, clean_schema(v));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(clean_schema).collect()),
        other => other,
    }
}

/// Convert a chat-completions body (after rewrite) into OpenAI Responses API shape.
pub fn chat_completion_to_responses(body: Value) -> Value {
    let choice = body
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first());
    let message = choice.and_then(|c| c.get("message"));
    let mut output = Vec::new();

    if let Some(msg) = message {
        let text = message_text_content(msg);
        if !text.is_empty() {
            output.push(json!({
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}]
            }));
        }

        if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in tool_calls {
                let call_id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("call_unknown");
                let name = tc
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let args = tc
                    .get("function")
                    .and_then(|f| f.get("arguments"))
                    .and_then(|a| a.as_str())
                    .unwrap_or("{}");
                output.push(json!({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": args,
                    "status": "completed"
                }));
            }
        }
    }

    json!({
        "id": format!("resp_{}", Uuid::new_v4().simple()),
        "object": "response",
        "created_at": unix_now_secs(),
        "status": "completed",
        "model": body.get("model").cloned().unwrap_or(Value::Null),
        "output": output
    })
}

/// Rewrite a non-streaming `/v1/chat/completions` response.
pub fn rewrite_chat_response(mut body: Value) -> (Value, AgentResponseSummary) {
    let mut summary = AgentResponseSummary {
        had_structured_tool_calls: false,
        rewrote_text_tool_call: false,
        finish_reason: None,
        content_preview: None,
    };

    let choices = match body.get_mut("choices").and_then(|c| c.as_array_mut()) {
        Some(c) if !c.is_empty() => c,
        _ => return (body, summary),
    };

    let choice = &mut choices[0];
    summary.finish_reason = choice
        .get("finish_reason")
        .and_then(|f| f.as_str())
        .map(str::to_string);

    let message = match choice.get_mut("message") {
        Some(m) => m,
        None => return (body, summary),
    };

    if message.get("tool_calls").and_then(|t| t.as_array()).is_some_and(|a| !a.is_empty()) {
        summary.had_structured_tool_calls = true;
        return (body, summary);
    }

    let content = message_text_content(message);

    summary.content_preview = Some(truncate_preview(&content, 200));

    if let Some(tool_calls) = text_content_to_tool_calls(&content) {
        message["content"] = json!(null);
        message["tool_calls"] = tool_calls.clone();
        choice["finish_reason"] = json!("tool_calls");
        summary.rewrote_text_tool_call = true;
        summary.had_structured_tool_calls = true;
    }

    (body, summary)
}

fn message_text_content(message: &Value) -> String {
    match message.get("content") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| {
                if part.get("type").and_then(|t| t.as_str()) == Some("text") {
                    part.get("text").and_then(|t| t.as_str())
                } else {
                    part.as_str()
                }
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn text_content_to_tool_calls(content: &str) -> Option<Value> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(calls) = parse_tool_calls_json(trimmed) {
        return Some(calls);
    }

    // Some models wrap JSON in markdown fences
    if trimmed.starts_with("```") {
        let inner = trimmed
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        if let Some(calls) = parse_tool_calls_json(inner) {
            return Some(calls);
        }
    }

    // Models often emit prose before the tool JSON, e.g. "I'll run that | { \"name\": ... }"
    if let Some(start) = trimmed.find('{') {
        if let Some(calls) = parse_tool_calls_json(&trimmed[start..]) {
            return Some(calls);
        }
    }

    None
}

fn parse_tool_calls_json(raw: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        if let Some(calls) = value_to_tool_calls(&v) {
            return Some(calls);
        }
    }

    if let Some(json_str) = extract_balanced_json_object(raw) {
        if let Ok(v) = serde_json::from_str::<Value>(&json_str) {
            return value_to_tool_calls(&v);
        }
    }

    None
}

fn extract_balanced_json_object(s: &str) -> Option<String> {
    let s = s.trim_start();
    if !s.starts_with('{') {
        return None;
    }

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;
    let mut end_idx = None;

    for (i, ch) in s.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }

    end_idx.map(|i| s[..=i].to_string())
}

fn value_to_tool_calls(v: &Value) -> Option<Value> {
    match v {
        Value::Object(map) if map.contains_key("name") => {
            let name = map.get("name")?.as_str()?;
            let args = map.get("arguments").cloned().unwrap_or(json!({}));
            Some(single_tool_call(name, &args))
        }
        Value::Array(arr) => {
            let mut calls = Vec::new();
            for (i, item) in arr.iter().enumerate() {
                if let Value::Object(map) = item {
                    if let Some(name) = map.get("name").and_then(|n| n.as_str()) {
                        let args = map.get("arguments").cloned().unwrap_or(json!({}));
                        calls.push(tool_call_entry(i, name, &args));
                    }
                }
            }
            if calls.is_empty() {
                None
            } else {
                Some(Value::Array(calls))
            }
        }
        _ => None,
    }
}

fn single_tool_call(name: &str, args: &Value) -> Value {
    json!([tool_call_entry(0, name, args)])
}

fn tool_call_entry(index: usize, name: &str, args: &Value) -> Value {
    let args_str = if args.is_string() {
        args.as_str().unwrap_or("{}").to_string()
    } else {
        serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string())
    };

    json!({
        "index": index,
        "id": format!("call_{}", Uuid::new_v4().simple()),
        "type": "function",
        "function": {
            "name": name,
            "arguments": args_str
        }
    })
}

/// Emit OpenAI-style incremental SSE chunks for structured tool calls.
fn streaming_tool_call_sse_lines(state: &SseTransformState, tool_calls: &Value) -> Vec<String> {
    if state.response_format == AgentResponseFormat::ResponsesApi {
        return responses_stream_from_tool_calls(tool_calls);
    }

    let Some(calls) = tool_calls.as_array() else {
        return vec![];
    };

    let mut lines = Vec::new();
    lines.push(format!(
        "data: {}",
        state.chat_stream_chunk(json!([{
            "index": 0,
            "delta": {"role": "assistant"},
            "finish_reason": null
        }]))
    ));

    for tc in calls {
        let index = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
        let id = tc
            .get("id")
            .and_then(|i| i.as_str())
            .unwrap_or("call_unknown");
        let name = tc
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let args = tc
            .get("function")
            .and_then(|f| f.get("arguments"))
            .and_then(|a| a.as_str())
            .unwrap_or("{}");

        lines.push(format!(
            "data: {}",
            state.chat_stream_chunk(json!([{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": index,
                        "id": id,
                        "type": "function",
                        "function": {"name": name, "arguments": ""}
                    }]
                },
                "finish_reason": null
            }]))
        ));

        if !args.is_empty() {
            lines.push(format!(
                "data: {}",
                state.chat_stream_chunk(json!([{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": index,
                            "function": {"arguments": args}
                        }]
                    },
                    "finish_reason": null
                }]))
            ));
        }
    }

    lines.push(format!(
        "data: {}",
        state.chat_stream_chunk(json!([{
            "index": 0,
            "delta": {},
            "finish_reason": "tool_calls"
        }]))
    ));
    lines
}

fn responses_stream_from_tool_calls(tool_calls: &Value) -> Vec<String> {
    let Some(calls) = tool_calls.as_array() else {
        return vec![];
    };

    let mut output_items = Vec::new();
    let mut lines = Vec::new();

    for (i, tc) in calls.iter().enumerate() {
        let call_id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("call_unknown");
        let name = tc
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let args = tc
            .get("function")
            .and_then(|f| f.get("arguments"))
            .and_then(|a| a.as_str())
            .unwrap_or("{}");

        lines.push(format!(
            "data: {}",
            json!({
                "type": "response.output_item.added",
                "output_index": i,
                "item": {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": ""
                }
            })
        ));
        lines.push(format!(
            "data: {}",
            json!({
                "type": "response.function_call_arguments.done",
                "call_id": call_id,
                "name": name,
                "arguments": args
            })
        ));
        output_items.push(json!({
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": args,
            "status": "completed"
        }));
    }

    lines.push(format!(
        "data: {}",
        json!({
            "type": "response.completed",
            "response": {
                "id": format!("resp_{}", Uuid::new_v4().simple()),
                "object": "response",
                "status": "completed",
                "output": output_items
            }
        })
    ));
    lines
}

fn truncate_preview(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

/// Build a non-streaming chat completion JSON object from an SSE byte stream.
pub fn sse_bytes_to_completion(sse: &str) -> Value {
    let mut accumulated = String::new();
    let mut tool_calls: Option<Value> = None;
    let mut finish = "stop".to_string();

    for line in sse.lines() {
        let Some(payload) = sse_payload_from_line(line) else {
            continue;
        };
        if payload == "[DONE]" {
            continue;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        if let Some(choice) = chunk.get("choices").and_then(|c| c.get(0)) {
            if let Some(delta) = choice.get("delta") {
                if let Some(tc) = delta.get("tool_calls") {
                    tool_calls = Some(tc.clone());
                }
                if let Some(c) = delta.get("content").and_then(|c| c.as_str()) {
                    accumulated.push_str(c);
                }
            }
            if let Some(fr) = choice.get("finish_reason").and_then(|f| f.as_str()) {
                if !fr.is_empty() {
                    finish = fr.to_string();
                }
            }
        }
    }

    let message = if let Some(tc) = tool_calls {
        json!({"role": "assistant", "content": null, "tool_calls": tc})
    } else {
        json!({"role": "assistant", "content": accumulated})
    };

    json!({"choices": [{"message": message, "finish_reason": finish}]})
}

pub fn parse_chat_response_body(body: &str) -> Value {
    let trimmed = body.trim();
    if trimmed.starts_with("data:") || trimmed.contains("\ndata:") {
        return sse_bytes_to_completion(trimmed);
    }
    if trimmed.starts_with('{') && trimmed.contains("\"choices\"") && trimmed.contains("\"delta\"") {
        return sse_bytes_to_completion(trimmed);
    }
    serde_json::from_str(trimmed).unwrap_or(Value::Null)
}

pub fn agent_debug_enabled() -> bool {
    std::env::var("MTRXAI_AGENT_DEBUG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn log_request_summary(summary: &AgentRequestSummary) {
    if agent_debug_enabled() {
        tracing::info!(
            model = ?summary.model,
            stream = summary.stream,
            messages = summary.message_count,
            tools = summary.tool_count,
            normalized = summary.normalized,
            "agent request"
        );
    }
}

pub fn log_response_summary(summary: &AgentResponseSummary) {
    if agent_debug_enabled() {
        tracing::info!(
            structured_tool_calls = summary.had_structured_tool_calls,
            rewrote = summary.rewrote_text_tool_call,
            finish_reason = ?summary.finish_reason,
            "agent response"
        );
    }
}

fn normalize_sse_data_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed == "[DONE]" {
        return Some("data: [DONE]".to_string());
    }
    if let Some(payload) = trimmed.strip_prefix("data:") {
        let payload = payload.trim();
        if payload == "[DONE]" {
            return Some("data: [DONE]".to_string());
        }
        return Some(format!("data: {payload}"));
    }
    if trimmed.starts_with('{') && trimmed.contains("\"choices\"") {
        return Some(format!("data: {trimmed}"));
    }
    None
}

fn sse_payload_from_line(line: &str) -> Option<&str> {
    line.trim()
        .strip_prefix("data:")
        .map(str::trim)
        .or_else(|| {
            let trimmed = line.trim();
            if trimmed.starts_with('{') && trimmed.contains("\"choices\"") {
                Some(trimmed)
            } else {
                None
            }
        })
}
#[derive(Default)]
pub struct SseTransformState {
    accumulated: String,
    suppress_content: bool,
    last_summary: Option<AgentResponseSummary>,
    response_format: AgentResponseFormat,
    stream_meta: StreamChunkMeta,
}

impl SseTransformState {
    pub fn with_response_format(response_format: AgentResponseFormat) -> Self {
        Self {
            response_format,
            ..Default::default()
        }
    }

    fn capture_stream_meta(&mut self, chunk: &Value) {
        if self.stream_meta.id.is_some() {
            return;
        }
        if let Some(id) = chunk.get("id").and_then(|v| v.as_str()) {
            self.stream_meta.id = Some(id.to_string());
        }
        if let Some(model) = chunk.get("model").and_then(|v| v.as_str()) {
            self.stream_meta.model = Some(model.to_string());
        }
        if let Some(created) = chunk.get("created").and_then(|v| v.as_u64()) {
            self.stream_meta.created = Some(created);
        }
    }

    fn chat_stream_chunk(&self, choices: Value) -> Value {
        json!({
            "id": self.stream_meta.id.as_deref().unwrap_or("chatcmpl-mtrxAI"),
            "object": "chat.completion.chunk",
            "created": self.stream_meta.created.unwrap_or_else(unix_now_secs),
            "model": self.stream_meta.model.as_deref().unwrap_or("unknown"),
            "choices": choices
        })
    }

    pub fn take_summary(&mut self) -> Option<AgentResponseSummary> {
        self.last_summary.take()
    }

    /// Process one SSE line (`data: ...`, bare JSON, or empty). Returns lines to forward to the client.
    pub fn process_line(&mut self, line: &str) -> Vec<String> {
        let Some(normalized) = normalize_sse_data_line(line) else {
            return vec![line.to_string()];
        };

        if normalized == "data: [DONE]" {
            let mut out = self.flush_on_stream_end();
            out.push("data: [DONE]".to_string());
            return out;
        }

        let payload = normalized.trim_start_matches("data:").trim();
        let Ok(mut chunk) = serde_json::from_str::<Value>(payload) else {
            return vec![normalized];
        };

        self.capture_stream_meta(&chunk);

        let choices = match chunk.get_mut("choices").and_then(|c| c.as_array_mut()) {
            Some(c) if !c.is_empty() => c,
            _ => return vec![normalized],
        };

        let choice = &mut choices[0];

        if let Some(delta) = choice.get("delta") {
            if delta
                .get("tool_calls")
                .and_then(|t| t.as_array())
                .is_some_and(|a| !a.is_empty())
            {
                self.last_summary = Some(AgentResponseSummary {
                    had_structured_tool_calls: true,
                    rewrote_text_tool_call: false,
                    finish_reason: choice
                        .get("finish_reason")
                        .and_then(|f| f.as_str())
                        .map(str::to_string),
                    content_preview: None,
                });
                return vec![format!("data: {}", chunk)];
            }

            if let Some(piece) = delta.get("content").and_then(|c| c.as_str()) {
                if !piece.is_empty() {
                    if !self.suppress_content {
                        if piece.trim_start().starts_with('{') {
                            self.suppress_content = true;
                            self.accumulated.push_str(piece);
                            return vec![];
                        }
                        if let Some(brace_idx) = piece.find('{') {
                            let before = &piece[..brace_idx];
                            let json_part = &piece[brace_idx..];
                            self.suppress_content = true;
                            self.accumulated.push_str(json_part);
                            if before.is_empty() {
                                return vec![];
                            }
                            choice["delta"] = json!({"content": before});
                            return vec![format!("data: {}", chunk)];
                        }
                    } else {
                        self.accumulated.push_str(piece);
                        return vec![];
                    }
                }
            }
        }

        let finish = choice
            .get("finish_reason")
            .and_then(|f| f.as_str())
            .unwrap_or("");

        if finish == "stop" || finish == "length" {
            if self.suppress_content {
                return self.complete_suppressed(finish);
            }
        }

        vec![normalized]
    }

    /// Emit a final tool-call rewrite when the upstream closes without `finish_reason: stop`.
    pub fn flush_on_stream_end(&mut self) -> Vec<String> {
        if !self.suppress_content || self.accumulated.is_empty() {
            return vec![];
        }
        let synthetic = json!({"choices": [{"delta": {}, "finish_reason": "stop"}]});
        self.process_line(&format!("data: {}", synthetic))
    }

    /// Flush suppressed content and ensure a trailing `data: [DONE]` when upstream omits it.
    /// OpenAI-compatible clients (e.g. OpenCode) hang if the SSE stream never ends with `[DONE]`.
    pub fn finalize_stream(&mut self, done_sent: bool) -> Vec<String> {
        let mut out = self.flush_on_stream_end();
        let already_done =
            done_sent || out.iter().any(|line| line.trim() == "data: [DONE]");
        if !already_done {
            out.push("data: [DONE]".to_string());
        }
        out
    }

    fn complete_suppressed(&mut self, _finish: &str) -> Vec<String> {
        self.suppress_content = false;

        if let Some(tool_calls) = text_content_to_tool_calls(&self.accumulated) {
            self.last_summary = Some(AgentResponseSummary {
                had_structured_tool_calls: true,
                rewrote_text_tool_call: true,
                finish_reason: Some("tool_calls".into()),
                content_preview: Some(truncate_preview(&self.accumulated, 200)),
            });
            self.accumulated.clear();
            return streaming_tool_call_sse_lines(self, &tool_calls);
        }

        self.last_summary = Some(AgentResponseSummary {
            had_structured_tool_calls: false,
            rewrote_text_tool_call: false,
            finish_reason: Some("stop".into()),
            content_preview: Some(truncate_preview(&self.accumulated, 200)),
        });
        let chunk = self.chat_stream_chunk(json!([{
            "index": 0,
            "delta": {"content": null},
            "finish_reason": "stop"
        }]));
        self.accumulated.clear();
        vec![format!("data: {}", chunk)]
    }
}

/// Pick response Content-Type for OpenAI-compatible chat completions.
pub fn chat_response_content_type(stream: bool, upstream: Option<&str>) -> &'static str {
    if stream {
        if upstream
            .map(|ct| ct.contains("text/event-stream"))
            .unwrap_or(false)
        {
            "text/event-stream"
        } else {
            "text/event-stream"
        }
    } else {
        "application/json"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_ollama_predict_applies_default() {
        let mut body = json!({"model": "gemma4:e2b", "messages": []});
        assert!(ensure_ollama_predict_options_with_default(&mut body, Some(8192)));
        assert_eq!(body["options"]["num_predict"], 8192);
    }

    #[test]
    fn ensure_ollama_predict_respects_existing() {
        let mut body = json!({
            "model": "gemma4:e2b",
            "options": {"num_predict": 2048}
        });
        assert!(ensure_ollama_predict_options_with_default(&mut body, Some(8192)));
        assert_eq!(body["options"]["num_predict"], 8192);
    }

    #[test]
    fn ensure_ollama_predict_keeps_high_existing() {
        let mut body = json!({
            "model": "gemma4:e2b",
            "options": {"num_predict": 16384}
        });
        assert!(!ensure_ollama_predict_options_with_default(&mut body, Some(8192)));
        assert_eq!(body["options"]["num_predict"], 16384);
    }

    #[test]
    fn ensure_ollama_predict_preserves_unlimited() {
        let mut body = json!({
            "model": "gemma4:e2b",
            "options": {"num_predict": -1}
        });
        assert!(!ensure_ollama_predict_options_with_default(&mut body, Some(8192)));
        assert_eq!(body["options"]["num_predict"], -1);
    }

    #[test]
    fn ensure_ollama_predict_maps_max_tokens() {
        let mut body = json!({"model": "gemma4:e2b", "max_tokens": 512});
        assert!(ensure_ollama_predict_options_with_default(&mut body, Some(8192)));
        assert_eq!(body["options"]["num_predict"], 512);
    }

    #[test]
    fn ensure_ollama_ctx_applies_default() {
        let mut body = json!({"model": "gemma4:e2b", "messages": []});
        assert!(ensure_ollama_ctx_options_with_default(&mut body, Some(16384)));
        assert_eq!(body["options"]["num_ctx"], 16384);
    }

    #[test]
    fn ensure_ollama_ctx_keeps_high_existing() {
        let mut body = json!({
            "model": "gemma4:e2b",
            "options": {"num_ctx": 32768}
        });
        assert!(!ensure_ollama_ctx_options_with_default(&mut body, Some(16384)));
        assert_eq!(body["options"]["num_ctx"], 32768);
    }

    #[test]
    fn ensure_ollama_ctx_bumps_low_existing() {
        let mut body = json!({
            "model": "gemma4:e2b",
            "options": {"num_ctx": 4096}
        });
        assert!(ensure_ollama_ctx_options_with_default(&mut body, Some(16384)));
        assert_eq!(body["options"]["num_ctx"], 16384);
    }

    #[test]
    fn apply_ollama_body_defaults_sets_predict_and_ctx() {
        let body = apply_ollama_body_defaults_with_default(
            r#"{"model":"gemma4:e2b","messages":[]}"#.to_string(),
            Some(8192),
            Some(16384),
        );
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["options"]["num_predict"], 8192);
        assert_eq!(parsed["options"]["num_ctx"], 16384);
    }

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
    fn strips_responses_only_fields() {
        let body = json!({
            "model": "x",
            "messages": [],
            "store": true,
            "previous_response_id": "resp_123"
        });
        let (out, _) = normalize_chat_request(body);
        assert!(out.get("store").is_none());
        assert!(out.get("previous_response_id").is_none());
    }

    #[test]
    fn rewrites_prose_prefixed_text_tool_call() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "I'll run cargo build in the project and verify the build is successful? | {\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"cargo build\", \"explanation\": \"Builds the project using Cargo.\"}}"
                },
                "finish_reason": "stop"
            }]
        });
        let (out, summary) = rewrite_chat_response(body);
        assert!(summary.rewrote_text_tool_call);
        let tc = &out["choices"][0]["message"]["tool_calls"][0];
        assert_eq!(tc["function"]["name"], "run_in_terminal");
        assert_eq!(out["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn streaming_chunks_include_openai_metadata() {
        let mut state = SseTransformState::default();
        let meta = json!({
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "created": 12345,
            "model": "llama3",
            "choices": [{"delta": {"content": "{\""}, "finish_reason": null}]
        });
        state.process_line(&format!("data: {}", meta));

        let chunk2 = json!({"choices": [{"delta": {"content": "\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]});
        state.process_line(&format!("data: {}", chunk2));

        let out = state.process_line(r#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}"#);
        assert!(out.iter().any(|line| line.contains("chat.completion.chunk")));
        assert!(out.iter().any(|line| line.contains("chatcmpl-test")));
    }

    #[test]
    fn converts_rewritten_chat_completion_to_responses_api() {
        let chat = json!({
            "model": "llama3",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_abc",
                        "type": "function",
                        "function": {
                            "name": "run_in_terminal",
                            "arguments": "{\"command\": \"cargo build\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        });
        let responses = chat_completion_to_responses(chat);
        assert_eq!(responses["output"][0]["type"], "function_call");
        assert_eq!(responses["output"][0]["name"], "run_in_terminal");
        assert_eq!(responses["output"][0]["call_id"], "call_abc");
    }

    #[test]
    fn streaming_rewrites_bare_json_sse_lines() {
        let mut state = SseTransformState::default();
        let chunk1 = r#"{"choices": [{"delta": {"content": "{\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]}"#;
        assert!(state.process_line(chunk1).is_empty());

        let chunk2 = r#"{"choices": [{"delta": {}, "finish_reason": "stop"}]}"#;
        let out = state.process_line(chunk2);
        assert!(out.len() >= 2);
        assert!(out.iter().any(|line| line.contains("run_in_terminal")));
        assert!(state.take_summary().unwrap().rewrote_text_tool_call);
    }

    #[test]
    fn streaming_prose_prefix_flushes_on_done_without_stop() {
        let mut state = SseTransformState::default();
        let chunk1 = json!({"choices": [{"delta": {"content": "run cargo build | {\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]});
        let _ = state.process_line(&format!("data: {}", chunk1));

        let out = state.process_line("data: [DONE]");
        assert!(out.iter().any(|line| line.contains("tool_calls")));
        assert!(state.take_summary().unwrap().rewrote_text_tool_call);
    }

    #[test]
    fn finalize_stream_emits_done_when_upstream_omits_it() {
        let mut state = SseTransformState::default();
        let chunk = json!({"choices": [{"delta": {"content": "hello"}, "finish_reason": null}]});
        let _ = state.process_line(&format!("data: {}", chunk));
        let finish = json!({"choices": [{"delta": {}, "finish_reason": "stop"}]});
        let _ = state.process_line(&format!("data: {}", finish));

        let out = state.finalize_stream(false);
        assert_eq!(out.last().map(String::as_str), Some("data: [DONE]"));
    }

    #[test]
    fn finalize_stream_does_not_duplicate_done() {
        let mut state = SseTransformState::default();
        let out = state.finalize_stream(true);
        assert!(!out.iter().any(|line| line.trim() == "data: [DONE]"));
    }

    #[test]
    fn finalize_stream_after_suppressed_tool_call_without_upstream_done() {
        // Simulates remote WebRTC channel closing after content without [DONE]
        // (the OpenCode hang: partial "build" then silence without stream termination).
        let mut state = SseTransformState::default();
        let chunk1 = json!({"choices": [{"delta": {"content": "run cargo build | {\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]});
        let partial = state.process_line(&format!("data: {}", chunk1));
        assert!(partial.iter().any(|line| line.contains("run cargo build")));

        let out = state.finalize_stream(false);
        assert!(out.iter().any(|line| line.contains("tool_calls")));
        assert_eq!(out.last().map(String::as_str), Some("data: [DONE]"));
        assert!(state.take_summary().unwrap().rewrote_text_tool_call);
    }

    #[test]
    fn streaming_prose_prefix_before_json() {
        let mut state = SseTransformState::default();
        let chunk1 = json!({"choices": [{"delta": {"content": "run cargo build | {"}, "finish_reason": null}]});
        let out1 = state.process_line(&format!("data: {}", chunk1));
        assert_eq!(out1.len(), 1);
        let parsed1: Value =
            serde_json::from_str(out1[0].trim_start_matches("data:").trim()).unwrap();
        assert_eq!(
            parsed1["choices"][0]["delta"]["content"],
            "run cargo build | "
        );

        let chunk2 = json!({"choices": [{"delta": {"content": "\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"ls\"}}"}, "finish_reason": null}]});
        assert!(state.process_line(&format!("data: {}", chunk2)).is_empty());

        let chunk3 = json!({"choices": [{"delta": {}, "finish_reason": "stop"}]});
        let out3 = state.process_line(&format!("data: {}", chunk3));
        assert!(out3.len() >= 2);
        assert!(out3.iter().any(|line| line.contains("run_in_terminal")));
        assert!(out3.iter().any(|line| line.contains("tool_calls")));
        assert!(state.take_summary().unwrap().rewrote_text_tool_call);
    }

    #[test]
    fn rewrites_text_tool_call_in_response() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "{\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"docker-compose up -d\"}}"
                },
                "finish_reason": "stop"
            }]
        });
        let (out, summary) = rewrite_chat_response(body);
        assert!(summary.rewrote_text_tool_call);
        let tc = &out["choices"][0]["message"]["tool_calls"][0];
        assert_eq!(tc["function"]["name"], "run_in_terminal");
        assert_eq!(out["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn preserves_existing_tool_calls() {
        let body = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}}]
                },
                "finish_reason": "tool_calls"
            }]
        });
        let (_, summary) = rewrite_chat_response(body);
        assert!(summary.had_structured_tool_calls);
        assert!(!summary.rewrote_text_tool_call);
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
    fn end_to_end_cursor_style_request_and_text_tool_response() {
        let request = json!({
            "model": "llama3.2",
            "input": [{"role": "user", "content": "start containers"}],
            "stream": false,
            "store": true,
            "tools": [{
                "type": "function",
                "name": "run_in_terminal",
                "description": "Run command",
                "parameters": {
                    "type": "object",
                    "properties": {"command": {"type": "string"}},
                    "required": ["command"],
                    "additionalProperties": false
                }
            }]
        });
        let (normalized, req_summary) = normalize_chat_request(request);
        assert!(req_summary.normalized);
        assert_eq!(normalized["tools"][0]["function"]["name"], "run_in_terminal");
        assert!(normalized["tools"][0]["function"]["parameters"].get("additionalProperties").is_none());

        let upstream_text_response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "{\"name\": \"run_in_terminal\", \"arguments\": {\"command\": \"docker-compose up -d\"}}"
                },
                "finish_reason": "stop"
            }]
        });
        let (final_response, resp_summary) = rewrite_chat_response(upstream_text_response);
        assert!(resp_summary.rewrote_text_tool_call);
        assert_eq!(
            final_response["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "run_in_terminal"
        );
        assert_eq!(final_response["choices"][0]["finish_reason"], "tool_calls");
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
}
