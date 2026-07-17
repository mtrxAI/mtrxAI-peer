//! IPC between blind P2P relay and inference vault/sidecar (Unix socket or TCP).

use crate::p2p_protocol::StreamMessage;
use crate::security::{inference_ipc_bind_host, inference_ipc_host, inference_ipc_socket_path};
use anyhow::{anyhow, Result};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};

const DEFAULT_IPC_PORT: u16 = 12745;

pub fn inference_ipc_port() -> u16 {
    std::env::var("MTRXAI_INFERENCE_IPC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_IPC_PORT)
}

pub fn inference_ipc_tcp_addr() -> String {
    format!("{}:{}", inference_ipc_host(), inference_ipc_port())
}

pub fn inference_ipc_listen_tcp_addr() -> String {
    format!("{}:{}", inference_ipc_bind_host(), inference_ipc_port())
}

enum IpcStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl IpcStream {
    async fn connect() -> Result<Self> {
        if let Some(path) = inference_ipc_socket_path() {
            #[cfg(unix)]
            {
                match UnixStream::connect(&path).await {
                    Ok(s) => return Ok(Self::Unix(s)),
                    Err(e) if inference_ipc_host() != "127.0.0.1" => {
                        eprintln!("Unix IPC connect failed ({e}); trying TCP fallback");
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            #[cfg(not(unix))]
            {
                let _ = path;
            }
        }
        Ok(Self::Tcp(TcpStream::connect(inference_ipc_tcp_addr()).await?))
    }

    async fn write_frame(&mut self, msg: &StreamMessage) -> Result<()> {
        let json = serde_json::to_vec(msg)?;
        let len = (json.len() as u32).to_le_bytes();
        match self {
            Self::Tcp(s) => {
                s.write_all(&len).await?;
                s.write_all(&json).await?;
                s.flush().await?;
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                s.write_all(&len).await?;
                s.write_all(&json).await?;
                s.flush().await?;
            }
        }
        Ok(())
    }

    async fn read_frame(&mut self) -> Result<StreamMessage> {
        let mut len_buf = [0u8; 4];
        match self {
            Self::Tcp(s) => {
                s.read_exact(&mut len_buf).await?;
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                s.read_exact(&mut len_buf).await?;
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 16 * 1024 * 1024 {
            return Err(anyhow!("IPC frame too large"));
        }
        let mut buf = vec![0u8; len];
        match self {
            Self::Tcp(s) => {
                s.read_exact(&mut buf).await?;
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                s.read_exact(&mut buf).await?;
            }
        }
        Ok(serde_json::from_slice(&buf)?)
    }
}

async fn write_frame_tcp(stream: &mut TcpStream, msg: &StreamMessage) -> Result<()> {
    let json = serde_json::to_vec(msg)?;
    let len = (json.len() as u32).to_le_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&json).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame_tcp(stream: &mut TcpStream) -> Result<StreamMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(anyhow!("IPC frame too large"));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

#[cfg(unix)]
async fn write_frame_unix(stream: &mut UnixStream, msg: &StreamMessage) -> Result<()> {
    let json = serde_json::to_vec(msg)?;
    let len = (json.len() as u32).to_le_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&json).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(unix)]
async fn read_frame_unix(stream: &mut UnixStream) -> Result<StreamMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(anyhow!("IPC frame too large"));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

fn prepare_unix_socket(path: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let p = Path::new(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if p.exists() {
            let _ = std::fs::remove_file(p);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

pub struct InferenceIpcClient;

impl InferenceIpcClient {
    pub async fn forward_request(
        _req_id: String,
        _path: String,
        _body: serde_json::Value,
        _room_id: String,
        _consumer_peer_id: String,
        msg: StreamMessage,
    ) -> Result<StreamMessage> {
        let mut stream = IpcStream::connect().await?;
        stream.write_frame(&msg).await?;
        stream.read_frame().await
    }
}

pub struct InferenceIpcServer {
    handler: Arc<
        dyn Fn(StreamMessage) -> std::pin::Pin<Box<dyn std::future::Future<Output = StreamMessage> + Send>>
            + Send
            + Sync,
    >,
}

impl InferenceIpcServer {
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(StreamMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = StreamMessage> + Send + 'static,
    {
        Self {
            handler: Arc::new(move |msg| Box::pin(handler(msg))),
        }
    }

    async fn serve_tcp(self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(&addr).await?;
        println!("🔒 Inference vault IPC listening on tcp://{addr}");
        loop {
            let (mut stream, _) = listener.accept().await?;
            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Ok(req) = read_frame_tcp(&mut stream).await {
                    let resp = handler(req).await;
                    let _ = write_frame_tcp(&mut stream, &resp).await;
                }
            });
        }
    }

    async fn serve_unix(self, path: String) -> Result<()> {
        #[cfg(unix)]
        {
            prepare_unix_socket(&path)?;
            let listener = UnixListener::bind(&path)?;
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
            println!("🔒 Inference vault IPC listening on unix://{path}");
            loop {
                let (mut stream, _) = listener.accept().await?;
                let handler = self.handler.clone();
                tokio::spawn(async move {
                    if let Ok(req) = read_frame_unix(&mut stream).await {
                        let resp = handler(req).await;
                        let _ = write_frame_unix(&mut stream, &resp).await;
                    }
                });
            }
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(anyhow!("Unix IPC is not supported on this platform"))
        }
    }

    pub async fn run(self) -> Result<()> {
        if let Some(path) = inference_ipc_socket_path() {
            #[cfg(unix)]
            {
                let tcp_addr = inference_ipc_listen_tcp_addr();
                let unix_server = self;
                let tcp_server = InferenceIpcServer {
                    handler: unix_server.handler.clone(),
                };
                tokio::select! {
                    r = unix_server.serve_unix(path) => r,
                    r = tcp_server.serve_tcp(tcp_addr) => r,
                }
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                self.serve_tcp(inference_ipc_listen_tcp_addr()).await
            }
        } else {
            self.serve_tcp(inference_ipc_listen_tcp_addr()).await
        }
    }
}

pub type SidecarRequest = (
    String,
    String,
    serde_json::Value,
    String,
    String,
    mpsc::Sender<Result<StreamMessage, String>>,
);

pub struct SidecarBridge {
    pending: Arc<Mutex<Option<SidecarRequest>>>,
}

impl SidecarBridge {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn dispatch(
        &self,
        req_id: String,
        path: String,
        body: serde_json::Value,
        room_id: String,
        consumer_peer_id: String,
        msg: StreamMessage,
    ) -> Result<StreamMessage, String> {
        let (tx, mut rx) = mpsc::channel(1);
        *self.pending.lock().await = Some((
            req_id,
            path,
            body,
            room_id,
            consumer_peer_id,
            tx,
        ));
        if let Ok(mut stream) = IpcStream::connect().await {
            let _ = stream.write_frame(&msg).await;
        }
        rx.recv()
            .await
            .ok_or_else(|| "sidecar did not respond".to_string())?
    }
}
