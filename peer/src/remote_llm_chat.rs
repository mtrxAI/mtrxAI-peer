use anyhow::anyhow;
use futures_util::{SinkExt, StreamExt};
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use webrtc::api::APIBuilder;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::llm_chat::LLMChat;
use crate::webrtc_manager::DataChannelMessage;

#[derive(Debug, Clone, Copy)]
pub enum RemoteChatRole {
    Client,
    Server,
}

pub struct RemoteLLMChat {
    my_name: String,
    role: RemoteChatRole,
    peer_state: Arc<Mutex<PeerState>>,
    ws_write: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    ws_read: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    llm_chat: Arc<Mutex<Option<LLMChat>>>,
    selected_model: Option<String>,
    target_server: Option<String>,
}

struct PeerState {
    pc: Arc<RTCPeerConnection>,
    active_data_channel: Option<Arc<webrtc::data_channel::RTCDataChannel>>,
    available_models: Vec<String>,
}

impl RemoteLLMChat {
    pub async fn new(my_name: String, role: RemoteChatRole) -> anyhow::Result<Self> {
        let ws_url = format!("ws://127.0.0.1:8080/ws?name={}", my_name);
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (ws_write, ws_read) = ws_stream.split();
        println!("✅ Connected to signaling server as '{}'", my_name);

        let api = APIBuilder::new().build();
        let config = RTCConfiguration {
            ice_servers: vec![webrtc::ice_transport::ice_server::RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let peer_connection = Arc::new(api.new_peer_connection(config).await?);
        let peer_state = Arc::new(Mutex::new(PeerState {
            pc: peer_connection,
            active_data_channel: None,
            available_models: Vec::new(),
        }));

        Ok(RemoteLLMChat {
            my_name,
            role,
            peer_state,
            ws_write,
            ws_read,
            llm_chat: Arc::new(Mutex::new(None)),
            selected_model: None,
            target_server: None,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let (ui_tx, ui_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            let mut buffer = String::new();
            while io::stdin().read_line(&mut buffer).is_ok() {
                let choice = buffer.trim().to_string();
                if !choice.is_empty() {
                    let _ = ui_tx.send(choice);
                }
                buffer.clear();
            }
        });

        match self.role {
            RemoteChatRole::Client => self.run_client_mode(ui_rx).await,
            RemoteChatRole::Server => self.run_server_mode(ui_rx).await,
        }
    }

    async fn run_client_mode(
        &mut self,
        mut ui_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    ) -> anyhow::Result<()> {
        println!("\n📡 Client Mode: You will initiate the connection to a server");
        println!("   The server must have Ollama running and be in Server mode\n");

        print!("Enter server device name: ");
        io::stdout().flush()?;

        let target_server = match ui_rx.recv().await {
            Some(input) => {
                let name = input.trim().to_string();
                if name.is_empty() {
                    return Err(anyhow!("Server name cannot be empty"));
                }
                name
            }
            None => return Err(anyhow!("UI stream closed unexpectedly")),
        };
        self.target_server = Some(target_server.clone());

        println!("\n🔗 Initiating connection to '{}'...", target_server);

        let pc = self.peer_state.lock().await.pc.clone();
        let data_channel = pc.create_data_channel("remote-llm-chat", None).await?;

        let ps_for_open = Arc::clone(&self.peer_state);
        let dc_ctx_for_open = data_channel.clone();
        data_channel.on_open(Box::new(move || {
            println!("\n🚀 WebRTC connection established!");
            let inner_dc = dc_ctx_for_open.clone();
            let ps_inner = Arc::clone(&ps_for_open);
            Box::pin(async move {
                let mut ps = ps_inner.lock().await;
                ps.active_data_channel = Some(inner_dc);
            })
        }));

        let ps_for_dc = Arc::clone(&self.peer_state);
        data_channel.on_message(Box::new(move |msg| {
            let ps_inner = Arc::clone(&ps_for_dc);
            Box::pin(async move {
                if let Ok(text) = std::str::from_utf8(&msg.data) {
                    if let Ok(dc_msg) = serde_json::from_str::<DataChannelMessage>(&text) {
                        match dc_msg {
                            DataChannelMessage::ModelsAvailable { models } => {
                                println!("\n📚 Available models on server:");
                                for (i, model) in models.iter().enumerate() {
                                    println!("   [{}] {}", i + 1, model);
                                }
                                print!("\nSelect model index or type prompt: ");
                                let _ = io::stdout().flush();

                                let mut ps = ps_inner.lock().await;
                                ps.available_models = models;
                            }
                            DataChannelMessage::ChatResponse { response } => {
                                println!("\n🤖 LLM: {}\n", response);
                                print!("You: ");
                                let _ = io::stdout().flush();
                            }
                            DataChannelMessage::Error { message } => {
                                eprintln!("\n❌ Error from server: {}\n", message);
                                print!("You: ");
                                let _ = io::stdout().flush();
                            }
                            _ => {}
                        }
                    }
                }
            })
        }));

        let offer = pc.create_offer(None).await?;
        let mut gather_complete = pc.gathering_complete_promise().await;
        pc.set_local_description(offer).await?;
        let _ = gather_complete.recv().await;

        let local_desc = pc
            .local_description()
            .await
            .ok_or_else(|| anyhow!("Failed to get local description"))?;
        let route_payload = serde_json::json!({
            "sdp_type": "offer",
            "sdp": local_desc.sdp,
            "chat_mode": true
        });
        let msg = crate::webrtc_manager::ProtocolMessage::Route {
            to: target_server.clone(),
            from: self.my_name.clone(),
            payload: route_payload,
        };
        self.ws_write
            .send(Message::Text(serde_json::to_string(&msg)?))
            .await?;

        println!("⏳ Waiting for server to accept connection...");

        loop {
            tokio::select! {
                Some(user_input) = ui_rx.recv() => {
                    let input = user_input.trim();
                    if input.eq_ignore_ascii_case("exit") {
                        println!("\n👋 Exiting remote LLM chat...");
                        break;
                    }

                    if let Some(dc) = self.get_data_channel().await {
                        if self.selected_model.is_none() {
                            let mut chosen_model = input.to_string();
                            {
                                let ps = self.peer_state.lock().await;
                                if let Ok(idx) = input.parse::<usize>() {
                                    if idx > 0 && idx <= ps.available_models.len() {
                                        chosen_model = ps.available_models[idx - 1].clone();
                                    }
                                }
                            }

                            let msg = DataChannelMessage::ModelSelected { model: chosen_model.clone() };
                            if let Ok(serialized) = serde_json::to_string(&msg) {
                                if let Err(e) = dc.send_text(serialized).await {
                                    eprintln!("❌ Failed to send model selection: {}", e);
                                } else {
                                    self.selected_model = Some(chosen_model.clone());
                                    println!("⏳ Model selection '{}' sent, initializing chat session...", chosen_model);
                                }
                            }
                        } else {
                            let msg = DataChannelMessage::ChatMessage { message: input.to_string() };
                            if let Ok(serialized) = serde_json::to_string(&msg) {
                                if let Err(e) = dc.send_text(serialized).await {
                                    eprintln!("❌ Failed to send chat message: {}", e);
                                }
                            }
                        }
                    } else {
                        println!("⚠️ WebRTC connection not fully ready yet. Please wait.");
                    }
                }

                Some(ws_msg) = self.ws_read.next() => {
                    match ws_msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(proto_msg) = serde_json::from_str::<crate::webrtc_manager::ProtocolMessage>(&text) {
                                match proto_msg {
                                    crate::webrtc_manager::ProtocolMessage::Route { payload, .. } => {
                                        if payload["sdp_type"] == "answer" {
                                            let sdp_str = payload["sdp"].as_str().unwrap_or("");

                                            match RTCSessionDescription::answer(sdp_str.to_string()) {
                                                Ok(answer) => {
                                                    if let Err(e) = pc.set_remote_description(answer).await {
                                                        eprintln!("❌ Failed to set remote description: {}", e);
                                                    } else {
                                                        println!("✅ Connection approved by server. Handshake completed!");
                                                    }
                                                }
                                                Err(e) => eprintln!("❌ Failed to parse SDP answer: {}", e),
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => {
                            println!("❌ Connection to signaling server lost.");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_server_mode(
        &mut self,
        mut ui_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    ) -> anyhow::Result<()> {
        println!("\n🖥️  Server Mode: You will receive connections and run the LLM");
        println!("   Make sure Ollama is running on this machine\n");

        println!("\n⏳ Waiting for client to connect...");

        let pc = self.peer_state.lock().await.pc.clone();
        let ps_for_dc = Arc::clone(&self.peer_state);

        // Clone llm_chat Arc for use in closures
        let llm_chat_clone = Arc::clone(&self.llm_chat);

        pc.on_data_channel(Box::new(move |dc| {
            let ps_inner = Arc::clone(&ps_for_dc);
            let llm_inner = Arc::clone(&llm_chat_clone);
            println!("\n🤝 Client connected to DataChannel: '{}'", dc.label());

            Box::pin(async move {
                let dc_ctx = dc.clone();
                {
                    let mut ps = ps_inner.lock().await;
                    ps.active_data_channel = Some(dc.clone());
                }

                let dc_ctx_for_open = dc_ctx.clone();
                let dc_ctx_for_message = dc_ctx.clone();

                dc.on_open(Box::new(move || {
                    let dc_open_ctx = dc_ctx_for_open.clone();
                    Box::pin(async move {
                        println!("🚀 Server DataChannel is open. Fetching models from Ollama...");
                        
                        // Fetch real models from Ollama
                        let base_url = "http://127.0.0.1:11434";
                        let http_client = reqwest::Client::new();
                        
                        match http_client.get(format!("{}/api/tags", base_url)).send().await {
                            Ok(response) if response.status().is_success() => {
                                if let Ok(body) = response.json::<serde_json::Value>().await {
                                    if let Some(models) = body.get("models").and_then(|m| m.as_array()) {
                                        let model_names: Vec<String> = models
                                            .iter()
                                            .filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                                            .collect();
                                        
                                        let welcome = DataChannelMessage::ModelsAvailable { models: model_names };
                                        if let Ok(serialized) = serde_json::to_string(&welcome) {
                                            let _ = dc_open_ctx.send_text(serialized).await;
                                        }
                                    } else {
                                        let welcome = DataChannelMessage::ModelsAvailable {
                                            models: vec!["llama3".to_string()],
                                        };
                                        if let Ok(serialized) = serde_json::to_string(&welcome) {
                                            let _ = dc_open_ctx.send_text(serialized).await;
                                        }
                                    }
                                }
                            }
                            _ => {
                                eprintln!("⚠️ Failed to fetch models from Ollama, sending default list");
                                let welcome = DataChannelMessage::ModelsAvailable {
                                    models: vec!["llama3".to_string()],
                                };
                                if let Ok(serialized) = serde_json::to_string(&welcome) {
                                    let _ = dc_open_ctx.send_text(serialized).await;
                                }
                            }
                        }
                    })
                }));

                let llm_msg = Arc::clone(&llm_inner);
                dc.on_message(Box::new(move |msg| {
                    let dc_inner = dc_ctx_for_message.clone();
                    let llm_chat_msg = Arc::clone(&llm_msg);
                    Box::pin(async move {
                        if let Ok(text) = std::str::from_utf8(&msg.data) {
                            if let Ok(dc_msg) = serde_json::from_str::<DataChannelMessage>(&text) {
                                match dc_msg {
                                    DataChannelMessage::ModelSelected { model } => {
                                        println!("\n📋 Client selected model: {}", model);
                                        println!("🔄 Initializing LLM with model '{}'...", model);
                                        
                                        match LLMChat::new("127.0.0.1", 11434, &model).await {
                                            Ok(chat) => {
                                                let mut llm_guard = llm_chat_msg.lock().await;
                                                *llm_guard = Some(chat);
                                                println!("✅ LLM initialized successfully!");
                                            }
                                            Err(e) => {
                                                eprintln!("❌ Failed to initialize LLM: {}", e);
                                                let err_msg = DataChannelMessage::Error { 
                                                    message: format!("Failed to initialize LLM: {}", e)
                                                };
                                                let _ = dc_inner.send_text(serde_json::to_string(&err_msg).unwrap()).await;
                                            }
                                        }
                                    }
                                    DataChannelMessage::ChatMessage { message } => {
                                        println!("\n💬 Client: {}", message);
                                        
                                        let mut llm_guard = llm_chat_msg.lock().await;
                                        if let Some(ref mut llm) = *llm_guard {
                                            match llm.send_message(&message).await {
                                                Ok(response) => {
                                                    println!("🤖 Response: {}", response);
                                                    let resp_msg = DataChannelMessage::ChatResponse { response };
                                                    if let Err(e) = dc_inner.send_text(serde_json::to_string(&resp_msg).unwrap()).await {
                                                        eprintln!("❌ Failed to send response: {}", e);
                                                    }
                                                }
                                                Err(e) => {
                                                    eprintln!("❌ LLM error: {}", e);
                                                    let err_msg = DataChannelMessage::Error { 
                                                        message: format!("LLM error: {}", e)
                                                    };
                                                    let _ = dc_inner.send_text(serde_json::to_string(&err_msg).unwrap()).await;
                                                }
                                            }
                                        } else {
                                            drop(llm_guard);
                                            let err_msg = DataChannelMessage::Error { 
                                                message: "LLM not initialized. Please select a model first.".to_string() 
                                            };
                                            let _ = dc_inner.send_text(serde_json::to_string(&err_msg).unwrap()).await;
                                        }
                                    }
                                    _ => {
                                        println!("📩 Received data from client: {}", text);
                                    }
                                }
                            } else {
                                println!("📩 Received data from client: {}", text);
                            }
                        }
                    })
                }));
            })
        }));

        loop {
            tokio::select! {
                Some(user_input) = ui_rx.recv() => {
                    if user_input.trim().eq_ignore_ascii_case("exit") {
                        println!("👋 Shutting down server...");
                        break;
                    }
                }
                Some(ws_msg) = self.ws_read.next() => {
                    match ws_msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(proto_msg) = serde_json::from_str::<crate::webrtc_manager::ProtocolMessage>(&text) {
                                match proto_msg {
                                    crate::webrtc_manager::ProtocolMessage::Route { from, payload, .. } => {
                                        if payload["sdp_type"] == "offer" {
                                            println!("📨 Received WebRTC Offer from '{}'. Processing...", from);
                                            let sdp_str = payload["sdp"].as_str().unwrap_or("");

                                            match RTCSessionDescription::offer(sdp_str.to_string()) {
                                                Ok(offer) => {
                                                    if let Err(e) = pc.set_remote_description(offer).await {
                                                        eprintln!("❌ Server failed to set remote offer: {}", e);
                                                        continue;
                                                    }

                                                    if let Ok(answer) = pc.create_answer(None).await {
                                                        let mut gather_complete = pc.gathering_complete_promise().await;
                                                        let _ = pc.set_local_description(answer).await;
                                                        let _ = gather_complete.recv().await;

                                                        if let Some(local_desc) = pc.local_description().await {
                                                            let reply_payload = serde_json::json!({
                                                                "sdp_type": "answer",
                                                                "sdp": local_desc.sdp
                                                            });
                                                            let reply = crate::webrtc_manager::ProtocolMessage::Route {
                                                                to: from,
                                                                from: self.my_name.clone(),
                                                                payload: reply_payload,
                                                            };
                                                            let _ = self.ws_write.send(Message::Text(serde_json::to_string(&reply).unwrap())).await;
                                                        }
                                                    }
                                                }
                                                Err(e) => eprintln!("❌ Failed to parse remote offer: {}", e),
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_data_channel(&self) -> Option<Arc<webrtc::data_channel::RTCDataChannel>> {
        let ps = self.peer_state.lock().await;
        ps.active_data_channel.clone()
    }
}
