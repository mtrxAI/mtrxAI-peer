mod llm_chat;
mod webrtc_manager;

use llm_chat::LLMChat;
use std::io::{self, Write};
use webrtc_manager::WebRTCManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════╗");
    println!("║       mtrxAI Client - Mode Selector    ║");
    println!("╚══════════════════════════════════════╝\n");

    // Get device identity name
    print!("Enter your device identity name: ");
    io::stdout().flush()?;
    let mut my_name = String::new();
    io::stdin().read_line(&mut my_name)?;
    let my_name = my_name.trim().to_string();

    if my_name.is_empty() {
        eprintln!("❌ Device name cannot be empty");
        return Ok(());
    }

    loop {
        println!("\n╔══════════════════════════════════════╗");
        println!("║           Select Mode                ║");
        println!("╠══════════════════════════════════════╣");
        println!("║ (1) WebRTC Connection                ║");
        println!("║ (2) AI Chat via Ollama               ║");
        println!("║ (3) Exit                             ║");
        println!("╚══════════════════════════════════════╝");
        print!("\nChoice (1-3): ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            "1" => {
                println!("\n🔗 Starting WebRTC Connection Mode...\n");
                match run_webrtc_mode(&my_name).await {
                    Ok(_) => {
                        println!("\n✅ WebRTC session ended");
                    }
                    Err(e) => {
                        eprintln!("❌ WebRTC error: {}", e);
                    }
                }
            }
            "2" => {
                println!("\n🤖 Starting AI Chat Mode...\n");
                match run_llm_chat_mode().await {
                    Ok(_) => {
                        println!("\n✅ AI Chat session ended");
                    }
                    Err(e) => {
                        eprintln!("❌ AI Chat error: {}", e);
                    }
                }
            }
            "3" => {
                println!("\n👋 Goodbye!");
                break;
            }
            _ => {
                println!("❌ Invalid choice. Please select 1, 2, or 3.");
            }
        }
    }

    Ok(())
}

async fn run_webrtc_mode(my_name: &str) -> anyhow::Result<()> {
    let webrtc_manager = WebRTCManager::new(my_name.to_string()).await?;
    webrtc_manager.run().await
}

async fn run_llm_chat_mode() -> anyhow::Result<()> {
    // Configuration for Ollama (can be customized)
    let host = "127.0.0.1";
    let port = 11434;
    let model = "llama2"; // Default model; can be changed to "mistral", "neural-chat", etc.

    println!("Connecting to Ollama at {}:{}", host, port);
    println!("Using model: {}\n", model);

    match LLMChat::new(host, port, model).await {
        Ok(mut llm_chat) => {
            llm_chat.start_conversation().await?;
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to Ollama: {}", e);
            eprintln!("📌 Make sure Ollama is running locally on port 11434");
            eprintln!("   You can start Ollama with: ollama serve");
        }
    }

    Ok(())
}
