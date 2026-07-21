use crate::client_config::{swarm_accepts_jobs, SwarmMembership};
use crate::network_catalog::sync_unified_network_models;
use crate::p2p_protocol::{
    catalog_topic, compact_gpu_for_gossip, compact_models_for_gossip, compact_peer_info_for_gossip,
    decode_gossip, encode_gossip, fit_catalog_gossip, model_start_topic, token_namespace,
    CachedPeerRecord, GossipMessage, StreamMessage, PROXY_PROTOCOL, PROXY_PROTOCOL_V2,
};
use crate::p2p_proxy::{
    dispatch_swarm_encrypted_proxy_request, dispatch_swarm_proxy_request, EncryptedProxySlot,
    ReverseProxyTx, RrResponseTx,
};
use crate::proxy_e2ee::encrypt_proxy_request;
use crate::security::{
    e2ee_enabled, load_or_create_libp2p_keypair, peer_id_from_multiaddr, resolve_libp2p_peer_id,
    verify_proxy_auth,
};
use crate::shared::{
    swarm_peer_registry_key, ModelStartAction, ModelStartOfferState, ModelStartRequestState,
    PeerDirection, PeerModerationAction, PeerRegistry, ProxyRequestCommand, SharedState,
    TrackedPeer,
};
use crate::swarm_manager::sync_swarm_peer_connections;
use async_trait::async_trait;
use futures_util::StreamExt;
use libp2p::request_response::ResponseChannel;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{
    gossipsub, identify, noise, rendezvous, request_response, tcp, yamux, Multiaddr, PeerId, Swarm,
    SwarmBuilder,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};

const MAX_CHUNK_BYTES: usize = 3000;
const MAX_PROXY_BODY_BYTES: usize = 512 * 1024;
/// LLM inference can take minutes; libp2p default is 10s.
const PROXY_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(1);

struct PendingIncomingProxy {
    path: String,
    chunks: Vec<(u32, String)>,
}

#[derive(Clone)]
struct EncryptedStreamChunkData {
    seq: u32,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    done: bool,
}

#[derive(Clone)]
struct PendingEncryptedStreamCtx {
    path: String,
    room_id: String,
    consumer_ephemeral_secret: [u8; 32],
    provider_static_public: [u8; 32],
    line_buf: String,
    next_seq: u32,
    reorder: BTreeMap<u32, EncryptedStreamChunkData>,
}

#[derive(Clone)]
struct PendingOutboundProxy {
    req_id: String,
    path: String,
    body: serde_json::Value,
    target_mtrxai_peer: String,
    libp2p_peer: PeerId,
    attempts: u32,
    room_id: Option<String>,
    consumer_ephemeral_secret: Option<[u8; 32]>,
    provider_static_public: Option<[u8; 32]>,
    use_e2ee: bool,
}

const MAX_PROXY_SEND_ATTEMPTS: u32 = 8;

fn p2p_listen_addr_from_env() -> Multiaddr {
    let port = std::env::var("MTRXAI_P2P_LISTEN_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if port > 0 {
        format!("/ip4/0.0.0.0/tcp/{port}")
            .parse()
            .unwrap_or_else(|_| "/ip4/0.0.0.0/tcp/0".parse().expect("valid multiaddr"))
    } else {
        "/ip4/0.0.0.0/tcp/0".parse().expect("valid multiaddr")
    }
}

fn p2p_announce_host_from_env() -> Option<String> {
    std::env::var("MTRXAI_P2P_ANNOUNCE_HOST")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn routable_listen_addr(addr: &Multiaddr) -> Option<Multiaddr> {
    let mut host = None;
    let mut port = None;
    for proto in addr.iter() {
        match proto {
            libp2p::multiaddr::Protocol::Ip4(ip) if !ip.is_unspecified() && !ip.is_loopback() => {
                host = Some(ip.to_string());
            }
            libp2p::multiaddr::Protocol::Tcp(p) => port = Some(p),
            _ => {}
        }
    }
    if let (Some(h), Some(p)) = (host, port) {
        format!("/ip4/{h}/tcp/{p}").parse().ok()
    } else {
        None
    }
}

fn announce_multiaddr(raw: &Multiaddr, announce_host: Option<&str>) -> Option<Multiaddr> {
    let mut port = None;
    for proto in raw.iter() {
        if let libp2p::multiaddr::Protocol::Tcp(p) = proto {
            port = Some(p);
        }
    }
    let port = port?;
    if let Some(host) = announce_host {
        if host.chars().all(|c| c.is_ascii_digit() || c == '.') {
            return format!("/ip4/{host}/tcp/{port}").parse().ok();
        }
        // Keep Docker Compose service names as DNS — resolving to IP here goes stale
        // when container addresses change across restarts.
        return format!("/dns4/{host}/tcp/{port}").parse().ok();
    }
    routable_listen_addr(raw)
}

fn dial_addr_priority(addr: &Multiaddr) -> u8 {
    if addr
        .iter()
        .any(|p| matches!(p, libp2p::multiaddr::Protocol::Dns4(_)))
    {
        0
    } else if addr
        .iter()
        .any(|p| matches!(p, libp2p::multiaddr::Protocol::Dns(_)))
    {
        1
    } else {
        2
    }
}

fn dial_multiaddrs(listen_addrs: &[String], libp2p_peer: PeerId) -> Vec<Multiaddr> {
    let mut addrs: Vec<Multiaddr> = listen_addrs
        .iter()
        .filter_map(|s| s.parse::<Multiaddr>().ok())
        .map(|mut addr| {
            if peer_id_from_multiaddr(&addr).is_none() {
                addr = addr.with(libp2p::multiaddr::Protocol::P2p(libp2p_peer));
            }
            addr
        })
        .collect();
    addrs.sort_by_key(|addr| dial_addr_priority(addr));
    addrs
}

fn bootnode_peer_ids(bootnodes: &[String]) -> HashSet<PeerId> {
    bootnodes
        .iter()
        .filter_map(|s| s.parse::<Multiaddr>().ok())
        .filter_map(|addr| peer_id_from_multiaddr(&addr))
        .collect()
}

fn chunk_utf8(s: &str, max_bytes: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < s.len() {
        let mut end = (start + max_bytes).min(s.len());
        while end > start && !s.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(s[start..end].to_string());
        start = end;
    }
    chunks
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Default)]
struct ProxyCodec;

#[async_trait]
impl request_response::Codec for ProxyCodec {
    type Protocol = String;
    type Request = StreamMessage;
    type Response = StreamMessage;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: futures_util::AsyncRead + Unpin + Send,
    {
        read_json_line(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: futures_util::AsyncRead + Unpin + Send,
    {
        read_json_line(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: futures_util::AsyncWrite + Unpin + Send,
    {
        write_json_line(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: futures_util::AsyncWrite + Unpin + Send,
    {
        write_json_line(io, &resp).await
    }
}

async fn read_json_line<T>(io: &mut T) -> io::Result<StreamMessage>
where
    T: futures_util::AsyncRead + Unpin + Send,
{
    use futures_util::AsyncReadExt;
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = io.read(&mut byte).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed",
            ));
        }
        if byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
    }
    serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

async fn write_json_line<T>(io: &mut T, msg: &StreamMessage) -> io::Result<()>
where
    T: futures_util::AsyncWrite + Unpin + Send,
{
    use futures_util::AsyncWriteExt;
    let mut line =
        serde_json::to_string(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    line.push('\n');
    io.write_all(line.as_bytes()).await?;
    io.flush().await?;
    Ok(())
}

#[derive(NetworkBehaviour)]
struct MtrxaiBehaviour {
    identify: identify::Behaviour,
    gossipsub: gossipsub::Behaviour,
    proxy: request_response::Behaviour<ProxyCodec>,
    rendezvous: rendezvous::client::Behaviour,
}

pub struct P2pManager {
    peer_id: String,
    swarm_id: String,
    p2p_token: String,
    bootnodes: Vec<String>,
    bootnode_peer_ids: HashSet<PeerId>,
    local_peer_id: PeerId,
    listen_addrs: Arc<Mutex<Vec<Multiaddr>>>,
    shared_state: SharedState,
    proxy_state: Arc<crate::llm_proxy::ProxyState>,
    peer_registry: PeerRegistry,
    proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
    model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
    peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
    swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
    swarm_connected: Arc<Mutex<HashMap<String, bool>>>,
    peer_cache: Arc<Mutex<HashMap<String, CachedPeerRecord>>>,
    mtrxai_to_libp2p: Arc<Mutex<HashMap<String, PeerId>>>,
    libp2p_to_mtrxai: Arc<Mutex<HashMap<PeerId, String>>>,
    pending_proxy_by_req_id: Arc<Mutex<HashMap<String, mpsc::Sender<Result<String, String>>>>>,
    pending_proxy_by_outbound_id:
        Arc<Mutex<HashMap<request_response::OutboundRequestId, PendingOutboundProxy>>>,
    pending_outbound_proxies: Arc<Mutex<Vec<PendingOutboundProxy>>>,
    pending_incoming_proxy: Arc<Mutex<HashMap<String, PendingIncomingProxy>>>,
    peer_latency_ms: Arc<Mutex<HashMap<String, u64>>>,
    pending_ping: Arc<Mutex<HashMap<request_response::OutboundRequestId, (String, Instant)>>>,
    rr_response_tx: RrResponseTx,
    rr_response_rx:
        Arc<Mutex<Option<mpsc::Receiver<(ResponseChannel<StreamMessage>, StreamMessage)>>>>,
    reverse_proxy_tx: ReverseProxyTx,
    reverse_proxy_rx: Arc<Mutex<Option<mpsc::Receiver<(PeerId, StreamMessage)>>>>,
    pending_encrypted_streams: Arc<Mutex<HashMap<String, PendingEncryptedStreamCtx>>>,
    pending_encrypted_stream_early:
        Arc<Mutex<HashMap<String, BTreeMap<u32, EncryptedStreamChunkData>>>>,
    pending_reverse_proxies: Arc<Mutex<HashMap<PeerId, Vec<StreamMessage>>>>,
    active_encrypted_proxies: Arc<Mutex<HashMap<PeerId, EncryptedProxySlot>>>,
    outgoing_model_requests: Arc<Mutex<Vec<ModelStartRequestState>>>,
    incoming_model_offers: Arc<Mutex<Vec<ModelStartOfferState>>>,
    handled_swarm_peers: Arc<Mutex<HashSet<PeerId>>>,
    bootnode_dial_backoff_until: Arc<Mutex<Option<Instant>>>,
}

impl P2pManager {
    pub fn new(
        peer_id: String,
        membership: SwarmMembership,
        bootnodes: Vec<String>,
        shared_state: SharedState,
        proxy_cmd_rx: mpsc::Receiver<ProxyRequestCommand>,
        model_start_cmd_rx: mpsc::Receiver<ModelStartAction>,
        peer_moderation_rx: mpsc::Receiver<PeerModerationAction>,
        proxy_state: Arc<crate::llm_proxy::ProxyState>,
        peer_registry: PeerRegistry,
        swarm_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
        cluster_network_models: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
        swarm_connected: Arc<Mutex<HashMap<String, bool>>>,
    ) -> Self {
        let local_peer_id = PeerId::random();
        let bootnode_peer_ids = bootnode_peer_ids(&bootnodes);
        let (rr_response_tx, rr_response_rx) = mpsc::channel(128);
        let (reverse_proxy_tx, reverse_proxy_rx) = mpsc::channel(128);
        Self {
            peer_id,
            swarm_id: membership.swarm_id,
            p2p_token: membership.p2p_token,
            bootnodes,
            bootnode_peer_ids,
            local_peer_id,
            listen_addrs: Arc::new(Mutex::new(Vec::new())),
            shared_state,
            proxy_state,
            peer_registry,
            proxy_cmd_rx,
            model_start_cmd_rx,
            peer_moderation_rx,
            swarm_network_models,
            cluster_network_models,
            swarm_connected,
            peer_cache: Arc::new(Mutex::new(HashMap::new())),
            mtrxai_to_libp2p: Arc::new(Mutex::new(HashMap::new())),
            libp2p_to_mtrxai: Arc::new(Mutex::new(HashMap::new())),
            pending_proxy_by_req_id: Arc::new(Mutex::new(HashMap::new())),
            pending_proxy_by_outbound_id: Arc::new(Mutex::new(HashMap::new())),
            pending_outbound_proxies: Arc::new(Mutex::new(Vec::new())),
            pending_incoming_proxy: Arc::new(Mutex::new(HashMap::new())),
            peer_latency_ms: Arc::new(Mutex::new(HashMap::new())),
            pending_ping: Arc::new(Mutex::new(HashMap::new())),
            rr_response_tx,
            rr_response_rx: Arc::new(Mutex::new(Some(rr_response_rx))),
            reverse_proxy_tx,
            reverse_proxy_rx: Arc::new(Mutex::new(Some(reverse_proxy_rx))),
            pending_encrypted_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_encrypted_stream_early: Arc::new(Mutex::new(HashMap::new())),
            pending_reverse_proxies: Arc::new(Mutex::new(HashMap::new())),
            active_encrypted_proxies: Arc::new(Mutex::new(HashMap::new())),
            outgoing_model_requests: Arc::new(Mutex::new(Vec::new())),
            incoming_model_offers: Arc::new(Mutex::new(Vec::new())),
            handled_swarm_peers: Arc::new(Mutex::new(HashSet::new())),
            bootnode_dial_backoff_until: Arc::new(Mutex::new(None)),
        }
    }

    async fn local_cached_record(&self) -> CachedPeerRecord {
        let state = self.shared_state.lock().await;
        let models = compact_models_for_gossip(&state.local_models_full);
        let peer_info = compact_peer_info_for_gossip(&state.peer_info);
        let gpu_host = compact_gpu_for_gossip(&state.last_gpu_host);
        drop(state);

        let cfg = self.proxy_state.client_config.read().await;
        let accepting = cfg
            .swarms
            .iter()
            .find(|s| s.swarm_id == self.swarm_id)
            .map(swarm_accepts_jobs)
            .unwrap_or(true);
        let provider_static_pk = {
            let e2ee_room =
                crate::proxy_e2ee::e2ee_room_id_for_swarm_membership(&cfg, &self.swarm_id)
                    .unwrap_or_else(|| self.swarm_id.clone());
            crate::proxy_e2ee::provider_static_public_key(
                &self.proxy_state.tx_store,
                &cfg,
                &e2ee_room,
            )
            .ok()
            .map(hex::encode)
        };
        let tee_capable = cfg.gpu_cc_mode.unwrap_or(false);
        drop(cfg);

        CachedPeerRecord {
            peer_id: self.peer_id.clone(),
            models,
            peer_info,
            gpu_host,
            accepting_jobs: accepting,
            listen_addrs: Vec::new(),
            tee_capable,
            trust_level: Some(
                if crate::security::inference_sidecar_enabled() {
                    "host"
                } else {
                    "transport"
                }
                .to_string(),
            ),
            gpu_model: self
                .shared_state
                .lock()
                .await
                .last_gpu_host
                .as_ref()
                .and_then(|g| g.name.clone()),
            provider_static_pk,
        }
    }

    async fn resolve_provider_static_public(
        &self,
        target_mtrxai_peer: &str,
        room_id: &str,
    ) -> anyhow::Result<[u8; 32]> {
        let catalog_pk = self
            .peer_cache
            .lock()
            .await
            .get(target_mtrxai_peer)
            .and_then(|r| r.provider_static_pk.clone());
        let cfg = self.proxy_state.client_config.read().await;
        crate::proxy_e2ee::resolve_provider_static_public(
            &self.proxy_state.tx_store,
            &cfg,
            room_id,
            catalog_pk.as_deref(),
        )
    }

    async fn peers_for_ranking(&self) -> Vec<CachedPeerRecord> {
        let cache = self.peer_cache.lock().await;
        let mut peers: Vec<CachedPeerRecord> = cache.values().cloned().collect();
        peers.push(self.local_cached_record().await);
        peers
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let keypair = load_or_create_libp2p_keypair(&self.proxy_state.tx_store, &self.peer_id)?;
        self.local_peer_id = keypair.public().to_peer_id();
        let local_peer_id = keypair.public().to_peer_id();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .flood_publish(true)
            .mesh_outbound_min(0)
            .mesh_n_low(0)
            .mesh_n(0)
            .mesh_n_high(0)
            .max_transmit_size(256 * 1024)
            .build()
            .map_err(|e| anyhow::anyhow!("gossipsub config: {e}"))?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(|e| anyhow::anyhow!("gossipsub behaviour: {e}"))?;

        let identify = identify::Behaviour::new(identify::Config::new(
            "/mtrxai/1.0.0".to_string(),
            keypair.public(),
        ));
        let proxy = request_response::Behaviour::new(
            [
                (
                    PROXY_PROTOCOL.to_string(),
                    request_response::ProtocolSupport::Full,
                ),
                (
                    PROXY_PROTOCOL_V2.to_string(),
                    request_response::ProtocolSupport::Full,
                ),
            ],
            request_response::Config::default().with_request_timeout(PROXY_REQUEST_TIMEOUT),
        );
        let rendezvous = rendezvous::client::Behaviour::new(keypair.clone());

        let behaviour = MtrxaiBehaviour {
            identify,
            gossipsub,
            proxy,
            rendezvous,
        };

        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_dns()?
            .with_behaviour(|_| behaviour)?
            .build();

        let listen_addr = p2p_listen_addr_from_env();
        swarm.listen_on(listen_addr)?;

        for bootnode in &self.bootnodes {
            if let Ok(addr) = bootnode.parse::<Multiaddr>() {
                let _ = swarm.dial(addr);
            }
        }

        let cat_topic = gossipsub::IdentTopic::new(catalog_topic(&self.p2p_token));
        let ms_topic = gossipsub::IdentTopic::new(model_start_topic(&self.p2p_token));
        swarm.behaviour_mut().gossipsub.subscribe(&cat_topic)?;
        swarm.behaviour_mut().gossipsub.subscribe(&ms_topic)?;

        self.set_connected(true).await;
        if let Err(e) = self.publish_catalog(&mut swarm).await {
            eprintln!("P2P catalog publish warning (swarm {}): {e}", self.swarm_id);
        }

        let mut catalog_interval = tokio::time::interval(Duration::from_secs(30));
        catalog_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        catalog_interval.tick().await;

        let mut rendezvous_interval = tokio::time::interval(Duration::from_secs(60));
        rendezvous_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        rendezvous_interval.tick().await;

        let mut latency_interval = tokio::time::interval(Duration::from_secs(45));
        latency_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        latency_interval.tick().await;

        let mut presence_interval = tokio::time::interval(Duration::from_secs(15));
        presence_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        presence_interval.tick().await;

        self.sync_presence_via_lobby(&mut swarm).await;
        self.ensure_bootnode_connections(&mut swarm).await;

        let mut rr_response_rx = self
            .rr_response_rx
            .lock()
            .await
            .take()
            .expect("rr response rx");
        let mut reverse_proxy_rx = self
            .reverse_proxy_rx
            .lock()
            .await
            .take()
            .expect("reverse proxy rx");

        loop {
            tokio::select! {
                Some(cmd) = self.proxy_cmd_rx.recv() => {
                    if let Err(e) = self.handle_proxy_cmd(&mut swarm, cmd).await {
                        eprintln!("P2P proxy error (swarm {}): {e}", self.swarm_id);
                    }
                }
                Some(action) = self.model_start_cmd_rx.recv() => {
                    if let Err(e) = self.handle_model_start_action(&mut swarm, action).await {
                        eprintln!("P2P model start error (swarm {}): {e}", self.swarm_id);
                    }
                }
                Some(action) = self.peer_moderation_rx.recv() => {
                    self.handle_moderation(&mut swarm, action).await;
                }
                event = swarm.select_next_some() => {
                    if let Err(e) = self.handle_swarm_event(&mut swarm, event).await {
                        eprintln!("P2P swarm event error (swarm {}): {e}", self.swarm_id);
                    }
                }
                _ = catalog_interval.tick() => {
                    let _ = self.publish_catalog(&mut swarm).await;
                }
                _ = rendezvous_interval.tick() => {
                    self.ensure_bootnode_connections(&mut swarm).await;
                    let _ = self.rendezvous_discover(&mut swarm).await;
                }
                _ = presence_interval.tick() => {
                    self.sync_presence_via_lobby(&mut swarm).await;
                    self.ensure_bootnode_connections(&mut swarm).await;
                }
                _ = latency_interval.tick() => {
                    self.probe_all_peer_latencies(&mut swarm).await;
                }
                Some((channel, msg)) = rr_response_rx.recv() => {
                    if !channel.is_open() {
                        eprintln!(
                            "Swarm {} libp2p proxy response channel closed for {:?}",
                            self.swarm_id, msg
                        );
                    } else if let Err(failed_msg) =
                        swarm.behaviour_mut().proxy.send_response(channel, msg)
                    {
                        eprintln!(
                            "Swarm {} failed to send libp2p proxy response: {:?}",
                            self.swarm_id, failed_msg
                        );
                    }
                }
                Some((peer, msg)) = reverse_proxy_rx.recv() => {
                    self.send_or_queue_reverse_proxy(&mut swarm, peer, msg).await;
                }
            }
        }
    }

    async fn probe_all_peer_latencies(&self, swarm: &mut Swarm<MtrxaiBehaviour>) {
        let peers: Vec<(String, PeerId)> = self
            .mtrxai_to_libp2p
            .lock()
            .await
            .iter()
            .map(|(mtrxai, libp2p)| (mtrxai.clone(), *libp2p))
            .collect();
        for (mtrxai, libp2p) in peers {
            if libp2p == self.local_peer_id {
                continue;
            }
            self.probe_peer_latency(swarm, &mtrxai, libp2p).await;
        }
    }

    async fn probe_peer_latency(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        mtrxai_peer: &str,
        libp2p_peer: PeerId,
    ) {
        if !swarm.is_connected(&libp2p_peer) {
            return;
        }
        let outbound = swarm
            .behaviour_mut()
            .proxy
            .send_request(&libp2p_peer, StreamMessage::Ping);
        self.pending_ping
            .lock()
            .await
            .insert(outbound, (mtrxai_peer.to_string(), Instant::now()));
    }

    async fn rendezvous_namespace(&self) -> anyhow::Result<rendezvous::Namespace> {
        rendezvous::Namespace::new(token_namespace(&self.p2p_token))
            .map_err(|e| anyhow::anyhow!("rendezvous namespace: {e}"))
    }

    async fn rendezvous_register_and_discover(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
    ) -> anyhow::Result<()> {
        if self.bootnode_peer_ids.is_empty() {
            return Ok(());
        }
        self.sync_external_addresses(swarm).await;
        let ns = self.rendezvous_namespace().await?;
        for bootnode in &self.bootnode_peer_ids {
            let _ = swarm
                .behaviour_mut()
                .rendezvous
                .register(ns.clone(), *bootnode, None);
            swarm
                .behaviour_mut()
                .rendezvous
                .discover(Some(ns.clone()), None, None, *bootnode);
        }
        Ok(())
    }

    async fn rendezvous_discover(&self, swarm: &mut Swarm<MtrxaiBehaviour>) -> anyhow::Result<()> {
        if self.bootnode_peer_ids.is_empty() {
            return Ok(());
        }
        let ns = self.rendezvous_namespace().await?;
        for bootnode in &self.bootnode_peer_ids {
            swarm
                .behaviour_mut()
                .rendezvous
                .discover(Some(ns.clone()), None, None, *bootnode);
        }
        Ok(())
    }

    async fn dial_mtrxai_peer(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        mtrxai_peer_id: &str,
        listen_addrs: &[String],
    ) {
        let libp2p_peer = resolve_libp2p_peer_id(listen_addrs, mtrxai_peer_id);
        self.mtrxai_to_libp2p
            .lock()
            .await
            .insert(mtrxai_peer_id.to_string(), libp2p_peer);
        self.libp2p_to_mtrxai
            .lock()
            .await
            .insert(libp2p_peer, mtrxai_peer_id.to_string());

        if swarm.is_connected(&libp2p_peer) {
            return;
        }

        let addrs = dial_multiaddrs(listen_addrs, libp2p_peer);
        if addrs.is_empty() {
            return;
        }
        for addr in addrs {
            match swarm.dial(addr.clone()) {
                Ok(_) => {
                    println!(
                        "📞 Swarm {} dialing {} ({mtrxai_peer_id})",
                        self.swarm_id, addr
                    );
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "📞 Swarm {} dial failed for {addr} ({mtrxai_peer_id}): {e}",
                        self.swarm_id
                    );
                }
            }
        }
    }

    async fn advertised_listen_addrs(&self) -> Vec<String> {
        let addrs = self.listen_addrs.lock().await.clone();
        let announce_host = p2p_announce_host_from_env();
        addrs
            .iter()
            .filter_map(|addr| announce_multiaddr(addr, announce_host.as_deref()))
            .map(|addr| format!("{addr}/p2p/{}", self.local_peer_id))
            .collect()
    }

    async fn sync_external_addresses(&self, swarm: &mut Swarm<MtrxaiBehaviour>) {
        let announce_host = p2p_announce_host_from_env();
        let addrs = self.listen_addrs.lock().await.clone();
        for addr in &addrs {
            if let Some(ext) = announce_multiaddr(addr, announce_host.as_deref()) {
                swarm.add_external_address(ext);
            }
        }
    }

    async fn ensure_bootnode_connections(&self, swarm: &mut Swarm<MtrxaiBehaviour>) {
        if self.bootnodes.is_empty() {
            return;
        }
        let has_swarm_peer = {
            let handled = self.handled_swarm_peers.lock().await;
            !handled.is_empty()
        };
        if has_swarm_peer {
            return;
        }
        let now = Instant::now();
        if let Some(until) = *self.bootnode_dial_backoff_until.lock().await {
            if now < until {
                return;
            }
        }
        for bootnode in &self.bootnodes {
            let Ok(addr) = bootnode.parse::<Multiaddr>() else {
                continue;
            };
            let Some(boot_peer) = peer_id_from_multiaddr(&addr) else {
                let _ = swarm.dial(addr);
                continue;
            };
            if !swarm.is_connected(&boot_peer) {
                let _ = swarm.dial(addr);
            }
        }
    }

    async fn sync_presence_via_lobby(&self, swarm: &mut Swarm<MtrxaiBehaviour>) {
        let addrs = self.advertised_listen_addrs().await;
        if addrs.is_empty() {
            return;
        }

        let lobby = &self.proxy_state.lobby_host;
        let client = &self.proxy_state.http_client;
        let register_url = crate::lobby_url::lobby_api_url(lobby, "/api/p2p/swarm/presence");
        let reg_body = serde_json::json!({
            "p2p_token": self.p2p_token,
            "peer_id": self.peer_id,
            "listen_addrs": addrs,
        });
        if let Err(e) = client.post(&register_url).json(&reg_body).send().await {
            eprintln!("Swarm {} presence register failed: {e}", self.swarm_id);
        }

        let peers_url = crate::lobby_url::lobby_api_url(lobby, "/api/public/p2p/swarm/peers");
        let Ok(resp) = client
            .get(&peers_url)
            .query(&[("p2p_token", &self.p2p_token)])
            .send()
            .await
        else {
            return;
        };
        if !resp.status().is_success() {
            eprintln!(
                "Swarm {} presence peers fetch failed: {}",
                self.swarm_id,
                resp.status()
            );
            return;
        }
        #[derive(serde::Deserialize)]
        struct PresencePeersResponse {
            peers: Vec<PresencePeer>,
        }
        #[derive(serde::Deserialize)]
        struct PresencePeer {
            peer_id: String,
            listen_addrs: Vec<String>,
        }
        let Ok(data) = resp.json::<PresencePeersResponse>().await else {
            return;
        };
        let remote_count = data
            .peers
            .iter()
            .filter(|p| p.peer_id != self.peer_id)
            .count();
        if remote_count > 0 {
            println!(
                "🔍 Swarm {} presence: {} remote peer(s) to dial",
                self.swarm_id, remote_count
            );
        }
        for peer in data.peers {
            if peer.peer_id == self.peer_id {
                continue;
            }
            if !peer.listen_addrs.is_empty() {
                let mut cache = self.peer_cache.lock().await;
                if let Some(record) = cache.get_mut(&peer.peer_id) {
                    record.listen_addrs = peer.listen_addrs.clone();
                }
            }
            let libp2p_peer = resolve_libp2p_peer_id(&peer.listen_addrs, &peer.peer_id);
            if swarm.is_connected(&libp2p_peer) {
                continue;
            }
            self.dial_mtrxai_peer(swarm, &peer.peer_id, &peer.listen_addrs)
                .await;
        }
    }

    async fn set_connected(&self, connected: bool) {
        self.swarm_connected
            .lock()
            .await
            .insert(self.swarm_id.clone(), connected);
    }

    async fn refresh_local_catalog(&self) -> anyhow::Result<()> {
        if self.proxy_state.llm_registry.inner.attached_count().await == 0 {
            let mut state = self.shared_state.lock().await;
            state.local_models.clear();
            state.local_models_full.clear();
            state.local_model_collisions.clear();
            return Ok(());
        }
        let snapshot = self
            .proxy_state
            .llm_registry
            .inner
            .rebuild_catalog(crate::ollama_client::gpu_probe_mode())
            .await?;
        let cfg = self.proxy_state.client_config.read().await;
        let (models, names) =
            crate::llm_registry::filter_models_for_cluster_advertisement(&snapshot, &cfg);
        drop(cfg);
        let mut state = self.shared_state.lock().await;
        state.local_models = names;
        state.local_models_full = models;
        if let Some(gpu) = snapshot.gpu_host {
            state.last_gpu_host = Some(gpu);
        }
        state.local_model_collisions = snapshot.collisions;
        Ok(())
    }

    async fn apply_remote_catalog(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        peer_id: String,
        models: Vec<serde_json::Value>,
        peer_info: Option<crate::shared::PeerInfo>,
        gpu_host: Option<crate::shared::GpuHostStatus>,
        accepting_jobs: bool,
        listen_addrs: Vec<String>,
        tee_capable: Option<bool>,
        trust_level: Option<String>,
        gpu_model: Option<String>,
        provider_static_pk: Option<String>,
    ) {
        if peer_id == self.peer_id {
            return;
        }
        let libp2p_peer = resolve_libp2p_peer_id(&listen_addrs, &peer_id);
        self.mtrxai_to_libp2p
            .lock()
            .await
            .insert(peer_id.clone(), libp2p_peer);
        self.libp2p_to_mtrxai
            .lock()
            .await
            .insert(libp2p_peer, peer_id.clone());
        self.dial_mtrxai_peer(swarm, &peer_id, &listen_addrs).await;
        let remote_model_count = models.len();
        self.peer_cache.lock().await.insert(
            peer_id.clone(),
            CachedPeerRecord {
                peer_id: peer_id.clone(),
                models,
                peer_info,
                gpu_host,
                accepting_jobs,
                listen_addrs,
                tee_capable: tee_capable.unwrap_or(false),
                trust_level,
                gpu_model,
                provider_static_pk,
            },
        );
        println!(
            "📥 Swarm {} received catalog from {}: {} model(s)",
            self.swarm_id, peer_id, remote_model_count
        );
        self.rebuild_swarm_catalog().await;
    }

    async fn build_catalog_sync(&self) -> StreamMessage {
        let state = self.shared_state.lock().await;
        let models = compact_models_for_gossip(&state.local_models_full);
        let peer_info = compact_peer_info_for_gossip(&state.peer_info);
        let gpu_host = compact_gpu_for_gossip(&state.last_gpu_host);
        drop(state);

        let cfg = self.proxy_state.client_config.read().await;
        let accepting = cfg
            .swarms
            .iter()
            .find(|s| s.swarm_id == self.swarm_id)
            .map(swarm_accepts_jobs)
            .unwrap_or(true);
        let tee_capable = cfg.gpu_cc_mode.unwrap_or(false)
            || std::env::var("MTRXAI_TEE_MOCK")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
        let gpu_model = self
            .shared_state
            .lock()
            .await
            .last_gpu_host
            .as_ref()
            .and_then(|g| g.name.clone());
        let trust_level = if tee_capable {
            "tee_gpu"
        } else if crate::security::inference_sidecar_enabled() {
            "host"
        } else {
            "transport"
        };
        let provider_static_pk = {
            let e2ee_room =
                crate::proxy_e2ee::e2ee_room_id_for_swarm_membership(&cfg, &self.swarm_id)
                    .unwrap_or_else(|| self.swarm_id.clone());
            crate::proxy_e2ee::provider_static_public_key(
                &self.proxy_state.tx_store,
                &cfg,
                &e2ee_room,
            )
            .ok()
            .map(hex::encode)
        };
        drop(cfg);

        StreamMessage::CatalogSync {
            peer_id: self.peer_id.clone(),
            models,
            peer_info,
            gpu_host,
            accepting_jobs: accepting,
            listen_addrs: self.advertised_listen_addrs().await,
            tee_capable: Some(tee_capable),
            gpu_model,
            attestation_expiry: None,
            trust_level: Some(trust_level.to_string()),
            provider_static_pk,
        }
    }

    async fn push_catalog_to_swarm_peers(&self, swarm: &mut Swarm<MtrxaiBehaviour>) {
        let sync = self.build_catalog_sync().await;
        let peers: Vec<PeerId> = swarm.connected_peers().cloned().collect();
        for peer in peers {
            if self.bootnode_peer_ids.contains(&peer) {
                continue;
            }
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer);
            let _ = swarm
                .behaviour_mut()
                .proxy
                .send_request(&peer, sync.clone());
        }
    }

    async fn publish_catalog(&self, swarm: &mut Swarm<MtrxaiBehaviour>) -> anyhow::Result<()> {
        if let Err(e) = self.refresh_local_catalog().await {
            eprintln!("P2P catalog refresh warning (swarm {}): {e}", self.swarm_id);
        }
        let state = self.shared_state.lock().await;
        let models = compact_models_for_gossip(&state.local_models_full);
        let peer_info = compact_peer_info_for_gossip(&state.peer_info);
        let gpu_host = compact_gpu_for_gossip(&state.last_gpu_host);
        drop(state);

        let cfg = self.proxy_state.client_config.read().await;
        let accepting = cfg
            .swarms
            .iter()
            .find(|s| s.swarm_id == self.swarm_id)
            .map(swarm_accepts_jobs)
            .unwrap_or(true);
        let tee_capable = cfg.gpu_cc_mode.unwrap_or(false)
            || std::env::var("MTRXAI_TEE_MOCK")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
        let gpu_model = self
            .shared_state
            .lock()
            .await
            .last_gpu_host
            .as_ref()
            .and_then(|g| g.name.clone());
        let trust_level = if tee_capable {
            "tee_gpu"
        } else if crate::security::inference_sidecar_enabled() {
            "host"
        } else {
            "transport"
        };
        let provider_static_pk = {
            let e2ee_room =
                crate::proxy_e2ee::e2ee_room_id_for_swarm_membership(&cfg, &self.swarm_id)
                    .unwrap_or_else(|| self.swarm_id.clone());
            crate::proxy_e2ee::provider_static_public_key(
                &self.proxy_state.tx_store,
                &cfg,
                &e2ee_room,
            )
            .ok()
            .map(hex::encode)
        };
        drop(cfg);

        let mut msg = GossipMessage::CatalogUpdate {
            peer_id: self.peer_id.clone(),
            models,
            peer_info,
            gpu_host,
            accepting_jobs: accepting,
            listen_addrs: self.advertised_listen_addrs().await,
            tee_capable: Some(tee_capable),
            gpu_model,
            attestation_expiry: None,
            trust_level: Some(trust_level.to_string()),
            provider_static_pk,
        };
        fit_catalog_gossip(&mut msg);
        let model_count = match &msg {
            GossipMessage::CatalogUpdate { models, .. } => models.len(),
            _ => 0,
        };
        let topic = gossipsub::IdentTopic::new(catalog_topic(&self.p2p_token));
        match swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, encode_gossip(&msg)?)
        {
            Ok(_) => {
                println!(
                    "📤 Swarm {} published catalog: {} model(s) for peer {}",
                    self.swarm_id, model_count, self.peer_id
                );
            }
            Err(gossipsub::PublishError::InsufficientPeers) => {}
            Err(e) => {
                eprintln!(
                    "P2P catalog publish failed (swarm {}): {e} ({} models)",
                    self.swarm_id, model_count
                );
            }
        }
        self.push_catalog_to_swarm_peers(swarm).await;
        self.rebuild_swarm_catalog().await;
        Ok(())
    }

    async fn rebuild_swarm_catalog(&self) {
        let local = self.local_cached_record().await;
        let cache = self.peer_cache.lock().await;
        let mut all_peers: Vec<&CachedPeerRecord> = cache.values().collect();
        all_peers.push(&local);
        let mut by_name: HashMap<
            String,
            (
                serde_json::Value,
                Vec<serde_json::Value>,
                HashSet<String>,
                u64,
            ),
        > = HashMap::new();

        for peer in all_peers {
            if !peer.accepting_jobs {
                continue;
            }
            for model in &peer.models {
                let Some(name) = model.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                let loaded = model
                    .get("_status")
                    .and_then(|s| s.get("loaded"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let entry = by_name
                    .entry(name.to_string())
                    .or_insert_with(|| (model.clone(), Vec::new(), HashSet::new(), 0));
                entry.0 = crate::network_catalog::prefer_richer_template(&entry.0, model);
                if entry.2.insert(peer.peer_id.clone()) {
                    entry
                        .1
                        .push(serde_json::json!({ "id": peer.peer_id, "loaded": loaded }));
                    if loaded {
                        entry.3 += 1;
                    }
                }
            }
        }

        let swarm_models: Vec<serde_json::Value> = by_name
            .into_iter()
            .map(|(name, (template, peers, _, loaded_count))| {
                let representative = peers.first().cloned().unwrap_or(serde_json::json!(null));
                let mut obj = template.as_object().cloned().unwrap_or_default();
                obj.insert("name".to_string(), serde_json::json!(name));
                obj.insert("_swarm_id".to_string(), serde_json::json!(self.swarm_id));
                obj.insert("_peer".to_string(), representative);
                obj.insert("_peers".to_string(), serde_json::json!(peers));
                obj.insert("_peer_count".to_string(), serde_json::json!(peers.len()));
                obj.insert("_loaded_count".to_string(), serde_json::json!(loaded_count));
                obj.insert("_network_scope".to_string(), serde_json::json!("swarm"));
                obj.insert(
                    "_swarms".to_string(),
                    serde_json::json!([{
                        "swarm_id": self.swarm_id,
                        "peer_count": peers.len(),
                        "loaded_count": loaded_count,
                    }]),
                );
                serde_json::Value::Object(obj)
            })
            .collect();

        self.swarm_network_models
            .lock()
            .await
            .insert(self.swarm_id.clone(), swarm_models.clone());

        let peer_count = cache.len() + 1;
        println!(
            "📚 Swarm {} catalog: {} model(s) from {} peer(s) (local + remote)",
            self.swarm_id,
            swarm_models.len(),
            peer_count
        );

        sync_unified_network_models(
            &self.shared_state,
            &self.cluster_network_models,
            &self.swarm_network_models,
        )
        .await;
    }

    async fn handle_swarm_event(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        event: SwarmEvent<MtrxaiBehaviourEvent>,
    ) -> anyhow::Result<()> {
        match event {
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                message,
                ..
            })) => {
                if let Ok(gossip) = decode_gossip(&message.data) {
                    self.handle_gossip(swarm, gossip).await?;
                }
            }
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Rendezvous(
                rendezvous::client::Event::Discovered { registrations, .. },
            )) => {
                for registration in registrations {
                    let record = &registration.record;
                    let peer = record.peer_id();
                    if peer == self.local_peer_id {
                        continue;
                    }
                    for addr in record.addresses() {
                        let _ = swarm.dial(addr.clone());
                    }
                }
            }
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Rendezvous(
                rendezvous::client::Event::Registered { .. },
            )) => {
                let _ = self.rendezvous_discover(swarm).await;
            }
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Rendezvous(
                rendezvous::client::Event::RegisterFailed { error, .. },
            )) => {
                eprintln!(
                    "Swarm {} rendezvous register failed: {:?}",
                    self.swarm_id, error
                );
            }
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Identify(identify::Event::Received {
                ..
            })) => {
                // Dial targets come from catalog/presence (MTRXAI_P2P_ANNOUNCE_HOST).
                // Identify listen addrs are often container-local IPs that go stale.
            }
            SwarmEvent::Behaviour(MtrxaiBehaviourEvent::Proxy(event)) => match event {
                request_response::Event::Message { peer, message, .. } => {
                    self.handle_proxy_rr_message(swarm, peer, message).await?;
                }
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                } => {
                    self.handle_proxy_outbound_failure(swarm, request_id, error)
                        .await;
                }
                request_response::Event::InboundFailure {
                    request_id, error, ..
                } => {
                    eprintln!(
                        "Swarm {} inbound proxy request {:?} failed: {error}",
                        self.swarm_id, request_id
                    );
                }
                request_response::Event::ResponseSent { peer, request_id } => {
                    tracing::debug!(
                        peer = %peer,
                        ?request_id,
                        "Swarm {} proxy response sent",
                        self.swarm_id
                    );
                }
            },
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if peer_id.is_some_and(|p| self.bootnode_peer_ids.contains(&p)) {
                    *self.bootnode_dial_backoff_until.lock().await =
                        Some(Instant::now() + Duration::from_secs(120));
                } else {
                    eprintln!(
                        "Swarm {} outgoing connection failed to {:?}: {error}",
                        self.swarm_id, peer_id
                    );
                }
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if peer_id == self.local_peer_id {
                    return Ok(());
                }
                self.flush_pending_proxies_for_peer(swarm, peer_id).await;
                self.flush_pending_reverse_proxies_for_peer(swarm, peer_id)
                    .await;
                if self.bootnode_peer_ids.contains(&peer_id) {
                    let _ = self.rendezvous_register_and_discover(swarm).await;
                    return Ok(());
                }
                if !self.handled_swarm_peers.lock().await.insert(peer_id) {
                    return Ok(());
                }
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                println!("🤝 Swarm {} P2P connected to {}", self.swarm_id, peer_id);
                self.track_libp2p_peer(peer_id, PeerDirection::Inbound)
                    .await;
                self.map_connected_mtrxai_peer(peer_id).await;
                sync_swarm_peer_connections(
                    &self.shared_state,
                    &self.peer_registry,
                    &self.proxy_state.tx_store,
                    &self.proxy_state.peer_stats,
                )
                .await;
                if let Some(mtrxai) = self.libp2p_to_mtrxai.lock().await.get(&peer_id).cloned() {
                    self.probe_peer_latency(swarm, &mtrxai, peer_id).await;
                }
                let sync = self.build_catalog_sync().await;
                let _ = swarm.behaviour_mut().proxy.send_request(&peer_id, sync);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                if !self.bootnode_peer_ids.contains(&peer_id) && !swarm.is_connected(&peer_id) {
                    self.handled_swarm_peers.lock().await.remove(&peer_id);
                }
                self.untrack_libp2p_peer(peer_id).await;
                sync_swarm_peer_connections(
                    &self.shared_state,
                    &self.peer_registry,
                    &self.proxy_state.tx_store,
                    &self.proxy_state.peer_stats,
                )
                .await;
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                self.listen_addrs.lock().await.push(address.clone());
                self.sync_external_addresses(swarm).await;
                let announced = self.advertised_listen_addrs().await;
                println!(
                    "🌐 Swarm {} listening on {}/p2p/{}",
                    self.swarm_id, address, self.local_peer_id
                );
                if announced.is_empty() {
                    eprintln!(
                        "⚠️ Swarm {} has no routable P2P announce address — set MTRXAI_P2P_ANNOUNCE_HOST (e.g. docker service name)",
                        self.swarm_id
                    );
                } else {
                    println!(
                        "📡 Swarm {} announcing: {}",
                        self.swarm_id,
                        announced.join(", ")
                    );
                }
                let _ = self.publish_catalog(swarm).await;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_proxy_rr_message(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        peer: PeerId,
        message: request_response::Message<StreamMessage, StreamMessage>,
    ) -> anyhow::Result<()> {
        let consumer_mtrxai = self
            .libp2p_to_mtrxai
            .lock()
            .await
            .get(&peer)
            .cloned()
            .unwrap_or_else(|| peer.to_string());

        match message {
            request_response::Message::Request {
                request, channel, ..
            } => match request {
                StreamMessage::Ping => {
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::CatalogSync {
                    peer_id,
                    models,
                    peer_info,
                    gpu_host,
                    accepting_jobs,
                    listen_addrs,
                    tee_capable,
                    trust_level,
                    gpu_model,
                    provider_static_pk,
                    ..
                } => {
                    self.apply_remote_catalog(
                        swarm,
                        peer_id,
                        models,
                        peer_info,
                        gpu_host,
                        accepting_jobs,
                        listen_addrs,
                        tee_capable,
                        trust_level,
                        gpu_model,
                        provider_static_pk,
                    )
                    .await;
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyRequest { req_id, path, body } => {
                    if e2ee_enabled() {
                        let _ = swarm.behaviour_mut().proxy.send_response(
                            channel,
                            StreamMessage::ProxyResponse {
                                req_id,
                                body: String::new(),
                                error: Some(
                                    "Plaintext proxy rejected; upgrade to E2EE v2".to_string(),
                                ),
                            },
                        );
                    } else {
                        dispatch_swarm_proxy_request(
                            req_id,
                            path,
                            body,
                            consumer_mtrxai,
                            self.swarm_id.clone(),
                            self.proxy_state.clone(),
                            self.rr_response_tx.clone(),
                            channel,
                        )
                        .await;
                    }
                }
                StreamMessage::EncryptedProxyRequest {
                    req_id,
                    path,
                    room_id,
                    consumer_ephemeral_pk,
                    nonce,
                    ciphertext,
                    aad_version,
                    auth,
                } => {
                    if auth.is_none() {
                        let _ = swarm.behaviour_mut().proxy.send_response(
                            channel,
                            StreamMessage::EncryptedProxyResponse {
                                req_id,
                                nonce: vec![],
                                ciphertext: vec![],
                                aad_version,
                                error: Some("Proxy auth required".to_string()),
                                stream: None,
                            },
                        );
                        return Ok(());
                    }
                    if let Some(proof) = &auth {
                        if !verify_proxy_auth(proof, &self.p2p_token, &proof.peer_id) {
                            let _ = swarm.behaviour_mut().proxy.send_response(
                                channel,
                                StreamMessage::EncryptedProxyResponse {
                                    req_id,
                                    nonce: vec![],
                                    ciphertext: vec![],
                                    aad_version,
                                    error: Some("Proxy auth failed".to_string()),
                                    stream: None,
                                },
                            );
                            return Ok(());
                        }
                    }
                    println!(
                        "🔐 Swarm {} handling encrypted proxy {} from {}",
                        self.swarm_id, req_id, consumer_mtrxai
                    );
                    self.mtrxai_to_libp2p
                        .lock()
                        .await
                        .insert(consumer_mtrxai.clone(), peer);
                    self.libp2p_to_mtrxai
                        .lock()
                        .await
                        .insert(peer, consumer_mtrxai.clone());
                    dispatch_swarm_encrypted_proxy_request(
                        req_id,
                        path,
                        room_id,
                        consumer_ephemeral_pk,
                        nonce,
                        ciphertext,
                        aad_version,
                        consumer_mtrxai,
                        peer,
                        self.swarm_id.clone(),
                        self.proxy_state.clone(),
                        self.rr_response_tx.clone(),
                        self.reverse_proxy_tx.clone(),
                        self.pending_reverse_proxies.clone(),
                        self.active_encrypted_proxies.clone(),
                        channel,
                    )
                    .await;
                }
                StreamMessage::EncryptedProxyStreamChunk {
                    req_id,
                    seq,
                    nonce,
                    ciphertext,
                    done,
                    ..
                } => {
                    self.handle_encrypted_stream_chunk(req_id, seq, nonce, ciphertext, done)
                        .await;
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyAuth { proof } => {
                    if verify_proxy_auth(&proof, &self.p2p_token, &proof.peer_id) {
                        let _ = swarm
                            .behaviour_mut()
                            .proxy
                            .send_response(channel, StreamMessage::Pong);
                    } else {
                        let _ = swarm.behaviour_mut().proxy.send_response(
                            channel,
                            StreamMessage::ProxyResponseError {
                                req_id: String::new(),
                                error: "Proxy auth failed".to_string(),
                            },
                        );
                    }
                }
                StreamMessage::ProxyRequestStart { req_id, path } => {
                    self.pending_incoming_proxy.lock().await.insert(
                        req_id,
                        PendingIncomingProxy {
                            path,
                            chunks: Vec::new(),
                        },
                    );
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyRequestChunk { req_id, seq, chunk } => {
                    if let Some(pending) = self.pending_incoming_proxy.lock().await.get_mut(&req_id)
                    {
                        pending.chunks.push((seq, chunk));
                    }
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyRequestEnd { req_id } => {
                    let pending = self.pending_incoming_proxy.lock().await.remove(&req_id);
                    if let Some(mut pending) = pending {
                        pending.chunks.sort_by_key(|(seq, _)| *seq);
                        let body_str: String = pending.chunks.into_iter().map(|(_, c)| c).collect();
                        match serde_json::from_str::<serde_json::Value>(&body_str) {
                            Ok(body) => {
                                dispatch_swarm_proxy_request(
                                    req_id,
                                    pending.path,
                                    body,
                                    consumer_mtrxai,
                                    self.swarm_id.clone(),
                                    self.proxy_state.clone(),
                                    self.rr_response_tx.clone(),
                                    channel,
                                )
                                .await;
                            }
                            Err(e) => {
                                let _ = swarm.behaviour_mut().proxy.send_response(
                                    channel,
                                    StreamMessage::ProxyResponse {
                                        req_id,
                                        body: String::new(),
                                        error: Some(format!("Invalid JSON body: {e}")),
                                    },
                                );
                            }
                        }
                    } else {
                        let _ = swarm.behaviour_mut().proxy.send_response(
                            channel,
                            StreamMessage::ProxyResponse {
                                req_id,
                                body: String::new(),
                                error: Some("Unknown proxy request".to_string()),
                            },
                        );
                    }
                }
                StreamMessage::ProxyResponseChunk { req_id, chunk } => {
                    self.proxy_state
                        .stats_stream_progress(&req_id, chunk.len() as u64, 0);
                    let tx = self
                        .pending_proxy_by_req_id
                        .lock()
                        .await
                        .get(&req_id)
                        .cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send(Ok(chunk)).await;
                    }
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyResponseDone { req_id } => {
                    self.pending_proxy_by_req_id.lock().await.remove(&req_id);
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                StreamMessage::ProxyResponseError { req_id, error } => {
                    self.proxy_state.stats_request_error(&req_id);
                    if let Some(tx) = self.pending_proxy_by_req_id.lock().await.remove(&req_id) {
                        let _ = tx.send(Err(error)).await;
                    }
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
                _ => {
                    let _ = swarm
                        .behaviour_mut()
                        .proxy
                        .send_response(channel, StreamMessage::Pong);
                }
            },
            request_response::Message::Response {
                request_id,
                response,
            } => match response {
                StreamMessage::Pong => {
                    if let Some((mtrxai, started)) =
                        self.pending_ping.lock().await.remove(&request_id)
                    {
                        let ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        self.peer_latency_ms.lock().await.insert(mtrxai, ms);
                    }
                }
                StreamMessage::ProxyResponse {
                    req_id,
                    body,
                    error,
                } => {
                    self.pending_proxy_by_outbound_id
                        .lock()
                        .await
                        .remove(&request_id);
                    if let Some(tx) = self.pending_proxy_by_req_id.lock().await.remove(&req_id) {
                        if let Some(err) = error {
                            self.proxy_state.stats_request_error(&req_id);
                            let _ = tx.send(Err(err)).await;
                        } else {
                            let usage = crate::token_usage::parse_usage_from_buffer(&body)
                                .unwrap_or(crate::token_usage::TokenUsage {
                                    prompt_tokens: 0,
                                    completion_tokens: 0,
                                    total_tokens: 0,
                                });
                            self.proxy_state.stats_request_complete(
                                &req_id,
                                usage.total_tokens,
                                body.len() as u64,
                            );
                            if body.is_empty() {
                                let _ = tx.send(Ok(body)).await;
                            } else {
                                for line in body.lines() {
                                    if line.is_empty() {
                                        continue;
                                    }
                                    let _ = tx.send(Ok(format!("{line}\n"))).await;
                                }
                            }
                        }
                    }
                }
                StreamMessage::EncryptedProxyResponse {
                    ref req_id,
                    ref error,
                    stream,
                    ..
                } => {
                    let pending = self
                        .pending_proxy_by_outbound_id
                        .lock()
                        .await
                        .remove(&request_id);
                    if let Some(err) = error {
                        if let Some(tx) = self.pending_proxy_by_req_id.lock().await.remove(req_id) {
                            self.proxy_state.stats_request_error(req_id);
                            let _ = tx.send(Err(err.clone())).await;
                        }
                        return Ok(());
                    }
                    if stream == Some(true) {
                        let Some(pending) = pending else {
                            eprintln!(
                                "Swarm {} encrypted stream ack for {} missing outbound state",
                                self.swarm_id, req_id
                            );
                            return Ok(());
                        };
                        let Some(room_id) = pending.room_id else {
                            if let Some(tx) =
                                self.pending_proxy_by_req_id.lock().await.remove(req_id)
                            {
                                let _ = tx
                                    .send(Err("missing room_id for encrypted stream".into()))
                                    .await;
                            }
                            return Ok(());
                        };
                        let Some(secret) = pending.consumer_ephemeral_secret else {
                            if let Some(tx) =
                                self.pending_proxy_by_req_id.lock().await.remove(req_id)
                            {
                                let _ = tx
                                    .send(Err(
                                        "missing ephemeral secret for encrypted stream".into()
                                    ))
                                    .await;
                            }
                            return Ok(());
                        };
                        let Some(provider_pk) = pending.provider_static_public else {
                            if let Some(tx) =
                                self.pending_proxy_by_req_id.lock().await.remove(req_id)
                            {
                                let _ = tx
                                    .send(Err(
                                        "missing provider static key for encrypted stream".into()
                                    ))
                                    .await;
                            }
                            return Ok(());
                        };
                        self.pending_encrypted_streams.lock().await.insert(
                            req_id.clone(),
                            PendingEncryptedStreamCtx {
                                path: pending.path,
                                room_id,
                                consumer_ephemeral_secret: secret,
                                provider_static_public: provider_pk,
                                line_buf: String::new(),
                                next_seq: 0,
                                reorder: BTreeMap::new(),
                            },
                        );
                        if let Some(early) = self
                            .pending_encrypted_stream_early
                            .lock()
                            .await
                            .remove(req_id)
                        {
                            if let Some(ctx) =
                                self.pending_encrypted_streams.lock().await.get_mut(req_id)
                            {
                                ctx.reorder.extend(early);
                            }
                        }
                        self.drain_encrypted_stream_chunks(req_id).await;
                        println!(
                            "🔓 Swarm {} waiting for encrypted stream chunks for {}",
                            self.swarm_id, req_id
                        );
                        return Ok(());
                    }
                    if let Some(tx) = self.pending_proxy_by_req_id.lock().await.remove(req_id) {
                        let Some(pending) = pending else {
                            let _ = tx.send(Err("missing outbound proxy state".into())).await;
                            return Ok(());
                        };
                        let Some(room_id) = pending.room_id else {
                            let _ = tx.send(Err("missing room_id for decrypt".into())).await;
                            return Ok(());
                        };
                        let Some(secret) = pending.consumer_ephemeral_secret else {
                            let _ = tx
                                .send(Err("missing ephemeral secret for decrypt".into()))
                                .await;
                            return Ok(());
                        };
                        let Some(provider_pk) = pending.provider_static_public else {
                            let _ = tx
                                .send(Err("missing provider static key for decrypt".into()))
                                .await;
                            return Ok(());
                        };
                        let cfg = self.proxy_state.client_config.read().await;
                        match crate::proxy_e2ee::decrypt_proxy_response(
                            &self.proxy_state.tx_store,
                            &cfg,
                            req_id,
                            &pending.path,
                            &room_id,
                            &secret,
                            &provider_pk,
                            &response,
                        )
                        .await
                        {
                            Ok(body) => {
                                let usage = crate::token_usage::parse_usage_from_buffer(&body)
                                    .unwrap_or(crate::token_usage::TokenUsage {
                                        prompt_tokens: 0,
                                        completion_tokens: 0,
                                        total_tokens: 0,
                                    });
                                self.proxy_state.stats_request_complete(
                                    req_id,
                                    usage.total_tokens,
                                    body.len() as u64,
                                );
                                if body.is_empty() {
                                    let _ = tx.send(Ok(body)).await;
                                } else {
                                    for line in body.lines() {
                                        if line.is_empty() {
                                            continue;
                                        }
                                        let _ = tx.send(Ok(format!("{line}\n"))).await;
                                    }
                                }
                                println!(
                                    "✅ Swarm {} decrypted encrypted proxy {} (single shot)",
                                    self.swarm_id, req_id
                                );
                            }
                            Err(e) => {
                                self.proxy_state.stats_request_error(req_id);
                                let _ = tx.send(Err(e.to_string())).await;
                            }
                        }
                    } else {
                        eprintln!(
                            "Swarm {} encrypted proxy response for {} with no pending consumer",
                            self.swarm_id, req_id
                        );
                    }
                }
                other => {
                    if !matches!(other, StreamMessage::Pong) {
                        eprintln!(
                            "Swarm {} unexpected proxy response type: {:?}",
                            self.swarm_id, other
                        );
                    }
                }
            },
        }
        Ok(())
    }

    async fn handle_gossip(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        msg: GossipMessage,
    ) -> anyhow::Result<()> {
        match msg {
            GossipMessage::CatalogUpdate {
                peer_id,
                models,
                peer_info,
                gpu_host,
                accepting_jobs,
                listen_addrs,
                tee_capable,
                trust_level,
                gpu_model,
                provider_static_pk,
                ..
            } => {
                self.apply_remote_catalog(
                    swarm,
                    peer_id,
                    models,
                    peer_info,
                    gpu_host,
                    accepting_jobs,
                    listen_addrs,
                    tee_capable,
                    trust_level,
                    gpu_model,
                    provider_static_pk,
                )
                .await;
            }
            GossipMessage::ModelStartRequest {
                req_id,
                model,
                requested_by,
            } => {
                if requested_by == self.peer_id {
                    return Ok(());
                }
                let cfg = self.proxy_state.client_config.read().await;
                let accepting = cfg
                    .swarms
                    .iter()
                    .find(|s| s.swarm_id == self.swarm_id)
                    .map(swarm_accepts_jobs)
                    .unwrap_or(true);
                drop(cfg);
                if !accepting {
                    return Ok(());
                }
                self.incoming_model_offers
                    .lock()
                    .await
                    .push(ModelStartOfferState {
                        req_id,
                        model,
                        cluster_id: None,
                        swarm_id: Some(self.swarm_id.clone()),
                        requested_by,
                        estimated_vram_mb: 4096,
                        disk_size_mb: None,
                        run_on_requester: None,
                        received_at_unix: unix_now(),
                    });
                self.sync_model_start_state().await;
            }
            GossipMessage::ModelStartOffer { .. }
            | GossipMessage::ModelStartProgress { .. }
            | GossipMessage::ModelStartRespond { .. } => {}
        }
        Ok(())
    }

    async fn sync_model_start_state(&self) {
        let outgoing = self.outgoing_model_requests.lock().await.clone();
        let incoming = self.incoming_model_offers.lock().await.clone();
        let mut state = self.shared_state.lock().await;
        state.outgoing_model_requests = outgoing;
        state.incoming_model_offers = incoming;
    }

    async fn handle_proxy_cmd(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        cmd: ProxyRequestCommand,
    ) -> anyhow::Result<()> {
        if let Some(ref sid) = cmd.swarm_id {
            if sid != &self.swarm_id {
                let _ = cmd
                    .response_tx
                    .send(Err(format!(
                        "Swarm id mismatch (cmd={sid}, local={})",
                        self.swarm_id
                    )))
                    .await;
                return Ok(());
            }
        }

        let target_mtrxai_peer = if let Some(peer) = cmd.target_peer {
            if self.is_peer_blocked(&peer).await {
                let _ = cmd
                    .response_tx
                    .send(Err(format!("Peer {peer} is blocked")))
                    .await;
                return Ok(());
            }
            peer
        } else {
            let peers = self.peers_for_ranking().await;
            let blocked = self.blocked_peers().await;
            let latency = self.peer_latency_ms.lock().await.clone();
            let require_tee = crate::security::require_tee()
                || self
                    .proxy_state
                    .client_config
                    .read()
                    .await
                    .require_tee_for_inference
                    .unwrap_or(false);
            let ranked = crate::peer_ranking::rank_swarm_peers_for_model_with_tee(
                &cmd.model,
                &peers,
                &self.peer_id,
                &blocked,
                &latency,
                require_tee,
            );
            if ranked.is_empty() {
                let _ = cmd
                    .response_tx
                    .send(Err(format!("No swarm peers for model {}", cmd.model)))
                    .await;
                return Ok(());
            }
            ranked[0].clone()
        };

        let libp2p_peer = self.resolve_libp2p_peer(&target_mtrxai_peer).await;
        self.mtrxai_to_libp2p
            .lock()
            .await
            .insert(target_mtrxai_peer.clone(), libp2p_peer);
        self.libp2p_to_mtrxai
            .lock()
            .await
            .insert(libp2p_peer, target_mtrxai_peer.clone());

        if !swarm.is_connected(&libp2p_peer) {
            let listen_addrs = self
                .peer_cache
                .lock()
                .await
                .get(&target_mtrxai_peer)
                .map(|r| r.listen_addrs.clone())
                .unwrap_or_default();
            if !listen_addrs.is_empty() {
                self.dial_mtrxai_peer(swarm, &target_mtrxai_peer, &listen_addrs)
                    .await;
            }
        }

        let body_str = serde_json::to_string(&cmd.body)?;
        if body_str.len() > MAX_PROXY_BODY_BYTES {
            let _ = cmd
                .response_tx
                .send(Err("Request body too large for swarm proxy".to_string()))
                .await;
            return Ok(());
        }

        let req_id = format!("p2p_{}", NEXT_REQ_ID.fetch_add(1, Ordering::SeqCst));
        self.proxy_state.stats_request_start(
            &req_id,
            &target_mtrxai_peer,
            &cmd.model,
            body_str.len() as u64,
        );
        self.pending_proxy_by_req_id
            .lock()
            .await
            .insert(req_id.clone(), cmd.response_tx);
        self.pending_encrypted_streams.lock().await.remove(&req_id);
        self.pending_encrypted_stream_early
            .lock()
            .await
            .remove(&req_id);

        let room_id = {
            let cfg = self.proxy_state.client_config.read().await;
            crate::proxy_e2ee::room_id_for_proxy(
                &cfg,
                cmd.cluster_id.as_deref(),
                cmd.swarm_id.as_deref(),
            )
        };
        let use_e2ee = e2ee_enabled() && room_id.is_some();

        let pending = PendingOutboundProxy {
            req_id,
            path: cmd.path,
            body: cmd.body,
            target_mtrxai_peer: target_mtrxai_peer.clone(),
            libp2p_peer,
            attempts: 0,
            room_id,
            consumer_ephemeral_secret: None,
            provider_static_public: None,
            use_e2ee,
        };

        if !swarm.is_connected(&libp2p_peer) {
            let listen_addrs = self
                .peer_cache
                .lock()
                .await
                .get(&target_mtrxai_peer)
                .map(|r| r.listen_addrs.clone())
                .unwrap_or_default();
            if listen_addrs.is_empty() {
                self.fail_proxy_request(
                    &pending.req_id,
                    format!("Peer {target_mtrxai_peer} has no P2P listen addresses yet"),
                )
                .await;
                return Ok(());
            }
            self.dial_mtrxai_peer(swarm, &target_mtrxai_peer, &listen_addrs)
                .await;
            println!(
                "⏳ Swarm {} queued proxy {} for {} until P2P connect",
                self.swarm_id, pending.req_id, target_mtrxai_peer
            );
            self.pending_outbound_proxies.lock().await.push(pending);
            return Ok(());
        }

        self.send_outbound_proxy(swarm, pending).await;
        Ok(())
    }

    async fn send_outbound_proxy(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        mut pending: PendingOutboundProxy,
    ) {
        pending.libp2p_peer = self.resolve_libp2p_peer(&pending.target_mtrxai_peer).await;
        if !swarm.is_connected(&pending.libp2p_peer) {
            self.pending_outbound_proxies.lock().await.push(pending);
            return;
        }

        let message = if pending.use_e2ee {
            let Some(room_id) = pending.room_id.clone() else {
                self.fail_proxy_request(&pending.req_id, "E2EE enabled but room_id missing".into())
                    .await;
                return;
            };
            self.pending_encrypted_streams
                .lock()
                .await
                .remove(&pending.req_id);
            self.pending_encrypted_stream_early
                .lock()
                .await
                .remove(&pending.req_id);
            let provider_pk = match self
                .resolve_provider_static_public(&pending.target_mtrxai_peer, &room_id)
                .await
            {
                Ok(pk) => pk,
                Err(e) => {
                    self.fail_proxy_request(
                        &pending.req_id,
                        format!("E2EE provider key resolve failed: {e}"),
                    )
                    .await;
                    return;
                }
            };
            pending.provider_static_public = Some(provider_pk);
            let cfg = self.proxy_state.client_config.read().await;
            match encrypt_proxy_request(
                &self.proxy_state.tx_store,
                &cfg,
                &self.peer_id,
                &pending.req_id,
                &pending.path,
                &pending.body,
                &room_id,
                &provider_pk,
            )
            .await
            {
                Ok((msg, secret)) => {
                    pending.consumer_ephemeral_secret = Some(secret);
                    msg
                }
                Err(e) => {
                    self.fail_proxy_request(&pending.req_id, format!("E2EE encrypt failed: {e}"))
                        .await;
                    return;
                }
            }
        } else {
            StreamMessage::ProxyRequest {
                req_id: pending.req_id.clone(),
                path: pending.path.clone(),
                body: pending.body.clone(),
            }
        };

        let outbound_id = swarm
            .behaviour_mut()
            .proxy
            .send_request(&pending.libp2p_peer, message);
        if pending.use_e2ee {
            println!(
                "🔐 Swarm {} sent encrypted proxy {} → {:?}",
                self.swarm_id, pending.req_id, pending.libp2p_peer
            );
        } else {
            println!(
                "📨 Swarm {} sent proxy {} → {:?}",
                self.swarm_id, pending.req_id, pending.libp2p_peer
            );
        }
        self.pending_proxy_by_outbound_id
            .lock()
            .await
            .insert(outbound_id, pending);
    }

    async fn handle_encrypted_stream_chunk(
        &self,
        req_id: String,
        seq: u32,
        nonce: Vec<u8>,
        ciphertext: Vec<u8>,
        done: bool,
    ) {
        if !self
            .pending_proxy_by_req_id
            .lock()
            .await
            .contains_key(&req_id)
        {
            return;
        }

        let chunk = EncryptedStreamChunkData {
            seq,
            nonce,
            ciphertext,
            done,
        };
        if self
            .pending_encrypted_streams
            .lock()
            .await
            .contains_key(&req_id)
        {
            if let Some(ctx) = self.pending_encrypted_streams.lock().await.get_mut(&req_id) {
                ctx.reorder.insert(seq, chunk);
            }
            self.drain_encrypted_stream_chunks(&req_id).await;
        } else {
            self.pending_encrypted_stream_early
                .lock()
                .await
                .entry(req_id)
                .or_default()
                .insert(seq, chunk);
        }
    }

    async fn drain_encrypted_stream_chunks(&self, req_id: &str) {
        let tx = self
            .pending_proxy_by_req_id
            .lock()
            .await
            .get(req_id)
            .cloned();
        let Some(tx) = tx else {
            return;
        };

        loop {
            let next = {
                let streams = self.pending_encrypted_streams.lock().await;
                let Some(ctx) = streams.get(req_id) else {
                    return;
                };
                ctx.next_seq
            };

            let chunk = {
                let mut streams = self.pending_encrypted_streams.lock().await;
                let Some(ctx) = streams.get_mut(req_id) else {
                    return;
                };
                ctx.reorder.remove(&next)
            };

            let Some(chunk) = chunk else {
                break;
            };

            let (path, room_id, secret, provider_pk, mut line_buf) = {
                let streams = self.pending_encrypted_streams.lock().await;
                let Some(ctx) = streams.get(req_id) else {
                    return;
                };
                (
                    ctx.path.clone(),
                    ctx.room_id.clone(),
                    ctx.consumer_ephemeral_secret,
                    ctx.provider_static_public,
                    ctx.line_buf.clone(),
                )
            };

            let cfg = self.proxy_state.client_config.read().await;
            match crate::proxy_e2ee::decrypt_proxy_chunk(
                &self.proxy_state.tx_store,
                &cfg,
                req_id,
                &path,
                chunk.seq,
                &room_id,
                &secret,
                &provider_pk,
                &chunk.nonce,
                &chunk.ciphertext,
            )
            .await
            {
                Ok(body) => {
                    line_buf.push_str(&body);
                    while let Some(pos) = line_buf.find('\n') {
                        let line = line_buf.drain(..=pos).collect::<String>();
                        if line.trim().is_empty() {
                            continue;
                        }
                        let _ = tx.send(Ok(line)).await;
                    }

                    let finished = chunk.done;
                    let mut streams = self.pending_encrypted_streams.lock().await;
                    let Some(ctx) = streams.get_mut(req_id) else {
                        return;
                    };
                    ctx.line_buf = line_buf;
                    ctx.next_seq = chunk.seq + 1;

                    if finished {
                        if !ctx.line_buf.trim().is_empty() {
                            let tail = ctx.line_buf.clone();
                            let _ = tx
                                .send(Ok(if tail.ends_with('\n') {
                                    tail
                                } else {
                                    format!("{tail}\n")
                                }))
                                .await;
                        }
                        let usage = crate::token_usage::parse_usage_from_buffer(&ctx.line_buf)
                            .unwrap_or(crate::token_usage::TokenUsage {
                                prompt_tokens: 0,
                                completion_tokens: 0,
                                total_tokens: 0,
                            });
                        self.proxy_state.stats_request_complete(
                            req_id,
                            usage.total_tokens,
                            ctx.line_buf.len() as u64,
                        );
                        drop(streams);
                        self.pending_proxy_by_req_id.lock().await.remove(req_id);
                        self.pending_encrypted_streams.lock().await.remove(req_id);
                        self.pending_encrypted_stream_early
                            .lock()
                            .await
                            .remove(req_id);
                        println!(
                            "✅ Swarm {} decrypted encrypted proxy {} ({} chunks)",
                            self.swarm_id,
                            req_id,
                            chunk.seq + 1
                        );
                        return;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Swarm {} encrypted stream chunk {} seq {} decrypt failed: {e}",
                        self.swarm_id, req_id, chunk.seq
                    );
                    self.proxy_state.stats_request_error(req_id);
                    self.pending_proxy_by_req_id.lock().await.remove(req_id);
                    self.pending_encrypted_streams.lock().await.remove(req_id);
                    self.pending_encrypted_stream_early
                        .lock()
                        .await
                        .remove(req_id);
                    let _ = tx.send(Err(e.to_string())).await;
                    return;
                }
            }
        }
    }

    async fn send_or_queue_reverse_proxy(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        peer: PeerId,
        msg: StreamMessage,
    ) {
        if swarm.is_connected(&peer) {
            let _ = swarm.behaviour_mut().proxy.send_request(&peer, msg);
            return;
        }

        if let Some(mtrxai) = self.libp2p_to_mtrxai.lock().await.get(&peer).cloned() {
            let listen_addrs = self
                .peer_cache
                .lock()
                .await
                .get(&mtrxai)
                .map(|r| r.listen_addrs.clone())
                .unwrap_or_default();
            if !listen_addrs.is_empty() {
                self.dial_mtrxai_peer(swarm, &mtrxai, &listen_addrs).await;
            }
        }

        let mut queue = self.pending_reverse_proxies.lock().await;
        let entry = queue.entry(peer).or_default();
        let first_in_batch = entry.is_empty();
        entry.push(msg);
        drop(queue);
        if first_in_batch {
            println!(
                "⏳ Swarm {} queued reverse proxy for {:?} (dialing)",
                self.swarm_id, peer
            );
        }
    }

    async fn flush_pending_reverse_proxies_for_peer(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        peer_id: PeerId,
    ) {
        if !swarm.is_connected(&peer_id) {
            return;
        }
        let Some(messages) = self.pending_reverse_proxies.lock().await.remove(&peer_id) else {
            return;
        };
        let count = messages.len();
        for msg in messages {
            let _ = swarm.behaviour_mut().proxy.send_request(&peer_id, msg);
        }
        if count > 0 {
            println!(
                "📤 Swarm {} flushed {count} queued reverse proxy message(s) to {peer_id}",
                self.swarm_id
            );
        }
    }

    async fn flush_pending_proxies_for_peer(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        peer_id: PeerId,
    ) {
        if !swarm.is_connected(&peer_id) {
            return;
        }
        let mut queue = self.pending_outbound_proxies.lock().await;
        let mut i = 0;
        while i < queue.len() {
            let mut pending = queue[i].clone();
            let resolved = self.resolve_libp2p_peer(&pending.target_mtrxai_peer).await;
            pending.libp2p_peer = resolved;
            if pending.libp2p_peer == peer_id && swarm.is_connected(&resolved) {
                queue.remove(i);
                drop(queue);
                self.send_outbound_proxy(swarm, pending).await;
                queue = self.pending_outbound_proxies.lock().await;
            } else {
                queue[i].libp2p_peer = resolved;
                i += 1;
            }
        }
    }

    async fn fail_proxy_request(&self, req_id: &str, message: String) {
        self.proxy_state.stats_request_error(req_id);
        if let Some(tx) = self.pending_proxy_by_req_id.lock().await.remove(req_id) {
            let _ = tx.send(Err(message)).await;
        }
    }

    async fn handle_proxy_outbound_failure(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        request_id: request_response::OutboundRequestId,
        error: request_response::OutboundFailure,
    ) {
        let Some(mut pending) = self
            .pending_proxy_by_outbound_id
            .lock()
            .await
            .remove(&request_id)
        else {
            return;
        };

        let retryable = matches!(
            error,
            request_response::OutboundFailure::DialFailure
                | request_response::OutboundFailure::ConnectionClosed
        );
        if retryable && pending.attempts < MAX_PROXY_SEND_ATTEMPTS {
            pending.attempts += 1;
            if swarm.is_connected(&pending.libp2p_peer) {
                self.send_outbound_proxy(swarm, pending).await;
            } else {
                self.pending_outbound_proxies.lock().await.push(pending);
            }
            return;
        }

        self.fail_proxy_request(
            &pending.req_id,
            format!("Swarm proxy request failed: {error}"),
        )
        .await;
    }

    async fn handle_model_start_action(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        action: ModelStartAction,
    ) -> anyhow::Result<()> {
        match action {
            ModelStartAction::Request {
                req_id,
                model,
                swarm_id,
                ..
            } => {
                if swarm_id.as_deref() != Some(self.swarm_id.as_str()) {
                    return Ok(());
                }
                self.outgoing_model_requests
                    .lock()
                    .await
                    .push(ModelStartRequestState {
                        req_id: req_id.clone(),
                        model: model.clone(),
                        cluster_id: None,
                        swarm_id: Some(self.swarm_id.clone()),
                        status: "pending".to_string(),
                        progress_pct: None,
                        provider_peer: None,
                        peers: None,
                        message: None,
                        updated_at_unix: unix_now(),
                    });
                self.sync_model_start_state().await;
                let gossip = GossipMessage::ModelStartRequest {
                    req_id,
                    model,
                    requested_by: self.peer_id.clone(),
                };
                let topic = gossipsub::IdentTopic::new(model_start_topic(&self.p2p_token));
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic, encode_gossip(&gossip)?)?;
            }
            ModelStartAction::Respond {
                req_id,
                accept,
                swarm_id,
                ..
            } => {
                if swarm_id.as_deref() != Some(self.swarm_id.as_str()) {
                    return Ok(());
                }
                let gossip = GossipMessage::ModelStartRespond {
                    req_id,
                    accept,
                    provider_peer: self.peer_id.clone(),
                };
                let topic = gossipsub::IdentTopic::new(model_start_topic(&self.p2p_token));
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic, encode_gossip(&gossip)?)?;
            }
        }
        Ok(())
    }

    async fn handle_moderation(
        &self,
        swarm: &mut Swarm<MtrxaiBehaviour>,
        action: PeerModerationAction,
    ) {
        if let PeerModerationAction::CloseConnections { peer_id } = action {
            if let Some(libp2p_id) = self.mtrxai_to_libp2p.lock().await.get(&peer_id).copied() {
                let _ = swarm.disconnect_peer_id(libp2p_id);
            }
        }
    }

    async fn is_peer_blocked(&self, peer_id: &str) -> bool {
        self.proxy_state
            .tx_store
            .is_peer_blocked(peer_id)
            .await
            .unwrap_or(false)
    }

    async fn blocked_peers(&self) -> HashSet<String> {
        self.proxy_state
            .tx_store
            .list_blocked_peers()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|b| b.peer_id)
            .collect()
    }

    async fn resolve_libp2p_peer(&self, mtrxai_peer_id: &str) -> PeerId {
        if let Some(cached) = self.mtrxai_to_libp2p.lock().await.get(mtrxai_peer_id) {
            return *cached;
        }
        let listen_addrs = self
            .peer_cache
            .lock()
            .await
            .get(mtrxai_peer_id)
            .map(|r| r.listen_addrs.clone())
            .unwrap_or_default();
        let libp2p_peer = resolve_libp2p_peer_id(&listen_addrs, mtrxai_peer_id);
        self.mtrxai_to_libp2p
            .lock()
            .await
            .insert(mtrxai_peer_id.to_string(), libp2p_peer);
        self.libp2p_to_mtrxai
            .lock()
            .await
            .insert(libp2p_peer, mtrxai_peer_id.to_string());
        libp2p_peer
    }

    async fn map_connected_mtrxai_peer(&self, libp2p_id: PeerId) {
        let cache = self.peer_cache.lock().await;
        for (mtrxai_id, _) in cache.iter() {
            if resolve_libp2p_peer_id(&[], mtrxai_id) == libp2p_id {
                self.mtrxai_to_libp2p
                    .lock()
                    .await
                    .insert(mtrxai_id.clone(), libp2p_id);
                self.libp2p_to_mtrxai
                    .lock()
                    .await
                    .insert(libp2p_id, mtrxai_id.clone());
                break;
            }
        }
    }

    async fn track_libp2p_peer(&self, libp2p_id: PeerId, direction: PeerDirection) {
        let mtrxai_id = self
            .libp2p_to_mtrxai
            .lock()
            .await
            .get(&libp2p_id)
            .cloned()
            .unwrap_or_else(|| libp2p_id.to_string());
        self.mtrxai_to_libp2p
            .lock()
            .await
            .insert(mtrxai_id.clone(), libp2p_id);
        self.libp2p_to_mtrxai
            .lock()
            .await
            .insert(libp2p_id, mtrxai_id.clone());

        let key = swarm_peer_registry_key(&mtrxai_id, &self.swarm_id);
        self.peer_registry.lock().await.insert(
            key,
            TrackedPeer {
                peer_id: mtrxai_id,
                cluster_id: None,
                swarm_id: Some(self.swarm_id.clone()),
                direction,
                connected_at: Instant::now(),
                data_channel_open: true,
                attestation_flags: 0,
            },
        );
    }

    async fn untrack_libp2p_peer(&self, libp2p_id: PeerId) {
        let mtrxai_id = self
            .libp2p_to_mtrxai
            .lock()
            .await
            .remove(&libp2p_id)
            .unwrap_or_else(|| libp2p_id.to_string());
        self.mtrxai_to_libp2p.lock().await.remove(&mtrxai_id);
        let key = swarm_peer_registry_key(&mtrxai_id, &self.swarm_id);
        self.peer_registry.lock().await.remove(&key);
        self.proxy_state.stats_remove_peer(&mtrxai_id);
    }
}
