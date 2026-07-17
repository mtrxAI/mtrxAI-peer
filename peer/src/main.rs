#[tokio::main]
async fn main() {
    if let Err(e) = peer::run().await {
        eprintln!("❌ Client error: {}", e);
        std::process::exit(1);
    }
}
