use anyhow::{anyhow, Result};
use reqwest::Client as HttpClient;
use serde_json::json;
use std::io::{self, Write};

pub struct LLMChat {
    base_url: String,
    model_name: String,
    conversation_history: Vec<(String, String)>, // (user_message, assistant_response)
    http_client: HttpClient,
}

impl LLMChat {
    pub async fn new(host: &str, port: u16, model: &str) -> Result<Self> {
        let base_url = format!("http://{}:{}", host, port);

        // Test connection to Ollama
        let http_client = HttpClient::new();
        let test_url = format!("{}/api/tags", base_url);

        // Check if Ollama is reachable
        match http_client.get(&test_url).send().await {
            Ok(response) if response.status().is_success() => {
                // Parse available models
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(models) = body.get("models").and_then(|m| m.as_array()) {
                        let model_names: Vec<String> = models
                            .iter()
                            .filter_map(|m| {
                                m.get("name")
                                    .and_then(|n| n.as_str())
                                    .map(|s| s.to_string())
                            })
                            .collect();

                        if model_names.is_empty() {
                            eprintln!("⚠️  No models available in Ollama");
                            eprintln!("   Run: ollama pull llama3.2:1b");
                            return Err(anyhow!("No models available. Please pull a model first."));
                        }

                        if !model_names.contains(&model.to_string()) {
                            eprintln!("⚠️  Model '{}' not found. Available models:", model);
                            for name in &model_names {
                                eprintln!("   - {}", name);
                            }
                            return Err(anyhow!(
                                "Model '{}' not found. Run: ollama pull {}",
                                model,
                                model
                            ));
                        }
                    }
                }

                println!("✅ Connected to Ollama at {}:{}", host, port);
                println!("📚 Using model: {}", model);
                Ok(LLMChat {
                    base_url,
                    model_name: model.to_string(),
                    conversation_history: Vec::new(),
                    http_client,
                })
            }
            Ok(response) => {
                let status = response.status();
                eprintln!("❌ Ollama returned error status: {}", status);
                eprintln!(
                    "   Make sure Ollama is running and accessible at http://{}:{}",
                    host, port
                );
                Err(anyhow!("Ollama returned status: {}", status))
            }
            Err(e) => {
                eprintln!("❌ Failed to connect to Ollama at http://{}:{}", host, port);
                eprintln!("   Error: {}", e);
                eprintln!("\n📌 To start Ollama:");
                eprintln!("   Local: ollama serve");
                eprintln!("   Docker: docker run -d -p 11434:11434 --name ollama ollama/ollama");
                eprintln!("   Podman:  podman run -d -p 11434:11434 --name ollama ollama/ollama");
                eprintln!("\n📦 Then pull a model:");
                eprintln!("   Docker: docker exec -it ollama ollama pull llama3.2:1b");
                eprintln!("   Podman:  podman exec -it ollama ollama pull llama3.2:1b");
                Err(anyhow!("Failed to connect to Ollama: {}", e))
            }
        }
    }

    pub async fn with_model_selection(host: &str, port: u16) -> Result<Self> {
        let base_url = format!("http://{}:{}", host, port);
        let http_client = HttpClient::new();

        // Fetch available models
        let test_url = format!("{}/api/tags", base_url);
        match http_client.get(&test_url).send().await {
            Ok(response) if response.status().is_success() => {
                let body: serde_json::Value = response.json().await?;
                let models = body
                    .get("models")
                    .and_then(|m| m.as_array())
                    .ok_or_else(|| anyhow!("Invalid response from Ollama"))?;

                let model_names: Vec<String> = models
                    .iter()
                    .filter_map(|m| {
                        m.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();

                if model_names.is_empty() {
                    eprintln!("⚠️  No models available in Ollama");
                    eprintln!("   Run: docker exec -it ollama ollama pull llama3.2:1b");
                    return Err(anyhow!("No models available. Please pull a model first."));
                }

                // Show available models
                println!("\n🤖 Available Models:");
                for (idx, model_name) in model_names.iter().enumerate() {
                    println!("   [{}] {}", idx + 1, model_name);
                }

                // Let user select
                loop {
                    print!("\nSelect model (1-{}): ", model_names.len());
                    io::stdout().flush()?;

                    let mut input = String::new();
                    io::stdin().read_line(&mut input)?;
                    let input = input.trim();

                    if let Ok(idx) = input.parse::<usize>() {
                        if idx > 0 && idx <= model_names.len() {
                            let selected_model = &model_names[idx - 1];
                            println!("✅ Connected to Ollama at {}:{}", host, port);
                            println!("📚 Using model: {}", selected_model);
                            return Ok(LLMChat {
                                base_url,
                                model_name: selected_model.to_string(),
                                conversation_history: Vec::new(),
                                http_client,
                            });
                        }
                    }
                    eprintln!(
                        "❌ Invalid selection. Please enter a number between 1 and {}",
                        model_names.len()
                    );
                }
            }
            Ok(response) => {
                let status = response.status();
                eprintln!("❌ Ollama returned error status: {}", status);
                eprintln!(
                    "   Make sure Ollama is running and accessible at http://{}:{}",
                    host, port
                );
                Err(anyhow!("Ollama returned status: {}", status))
            }
            Err(e) => {
                eprintln!("❌ Failed to connect to Ollama at http://{}:{}", host, port);
                eprintln!("   Error: {}", e);
                eprintln!("\n📌 To start Ollama:");
                eprintln!("   Local: ollama serve");
                eprintln!("   Docker: docker run -d -p 11434:11434 --name ollama ollama/ollama");
                eprintln!("   Podman:  podman run -d -p 11434:11434 --name ollama ollama/ollama");
                eprintln!("\n📦 Then pull a model (e.g., llama3.2:1b):");
                eprintln!("   Docker: docker exec -it ollama ollama pull llama3.2:1b");
                eprintln!("   Podman:  podman exec -it ollama ollama pull llama3.2:1b");
                Err(anyhow!("Failed to connect to Ollama: {}", e))
            }
        }
    }

    pub async fn send_message(&mut self, user_input: &str) -> Result<String> {
        // Build conversation context from history
        let mut messages = Vec::new();

        // Add previous messages to context
        for (user_msg, assistant_msg) in &self.conversation_history {
            messages.push(json!({
                "role": "user",
                "content": user_msg
            }));
            messages.push(json!({
                "role": "assistant",
                "content": assistant_msg
            }));
        }

        // Add current message
        messages.push(json!({
            "role": "user",
            "content": user_input
        }));

        // Try /api/chat endpoint first (Ollama v0.1+)
        let url = format!("{}/api/chat", self.base_url);
        let payload = json!({
            "model": self.model_name,
            "messages": messages,
            "stream": false
        });

        let response = self.http_client.post(&url).json(&payload).send().await?;

        match response.status().as_u16() {
            200 => {
                let body: serde_json::Value = response.json().await?;
                let assistant_response = body
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_str())
                    .ok_or_else(|| anyhow!("Invalid response from Ollama"))?
                    .to_string();

                // Store in history
                self.conversation_history
                    .push((user_input.to_string(), assistant_response.clone()));

                Ok(assistant_response)
            }
            404 => {
                eprintln!("❌ Ollama API endpoint not found (404)");
                eprintln!(
                    "   Model '{}' may not exist or may still be loading",
                    self.model_name
                );
                eprintln!("   Verify with: ollama list");
                eprintln!("   Pull a model: ollama pull {}", self.model_name);
                Err(anyhow!(
                    "Ollama error: Model endpoint returned 404. Check if model is installed."
                ))
            }
            500 => {
                eprintln!("❌ Ollama server error (500)");
                eprintln!("   The server may be overloaded or the model crashed");
                Err(anyhow!("Ollama server error: 500"))
            }
            status => {
                let error_text = response.text().await.unwrap_or_default();
                eprintln!("❌ Ollama error: HTTP {}", status);
                if !error_text.is_empty() {
                    eprintln!("   Details: {}", error_text);
                }
                Err(anyhow!("Ollama error: HTTP {}", status))
            }
        }
    }

    pub async fn start_conversation(&mut self) -> Result<()> {
        println!("\n🤖 AI Chat Mode Active");
        println!("Type 'exit' to return to main menu\n");

        loop {
            print!("You: ");
            io::stdout().flush()?;

            let mut user_input = String::new();
            io::stdin().read_line(&mut user_input)?;
            let user_input = user_input.trim();

            if user_input.eq_ignore_ascii_case("exit") {
                println!("\n👋 Exiting AI Chat mode...\n");
                break;
            }

            if user_input.is_empty() {
                continue;
            }

            match self.send_message(user_input).await {
                Ok(response) => {
                    println!("🤖 Assistant: {}\n", response);
                }
                Err(e) => {
                    println!("❌ Error: {}\n", e);
                }
            }
        }

        Ok(())
    }

    pub fn get_history(&self) -> &[(String, String)] {
        &self.conversation_history
    }

    pub fn clear_history(&mut self) {
        self.conversation_history.clear();
        println!("🗑️  Conversation history cleared");
    }
}
