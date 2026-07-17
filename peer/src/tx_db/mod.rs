mod keys;
mod models;
mod secrets;

use crate::gpu_history::unix_now;
use models::{BlockedPeerRecord, PeerRecord, PeerTransaction};
use native_db::{Builder, Database, Models};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub const TX_DB_PATH: &str = "mtrxai_transactions.db";

/// Local store path — co-located with `MTRXAI_CONFIG_PATH` when set so Docker volumes
/// keep signing keys alongside `client_config.json`.
pub fn tx_db_path() -> String {
    if let Ok(path) = std::env::var("MTRXAI_TX_DB_PATH") {
        if !path.trim().is_empty() {
            return path;
        }
    }

    let config_path = crate::client_config::client_config_path();
    let config = Path::new(&config_path);
    if let Some(parent) = config.parent() {
        if !parent.as_os_str().is_empty() {
            return parent
                .join(TX_DB_PATH)
                .to_string_lossy()
                .into_owned();
        }
    }

    TX_DB_PATH.to_string()
}

static MODELS: Lazy<Models> = Lazy::new(|| {
    let mut models = Models::new();
    models.define::<models::v1::PeerRecord>().unwrap();
    models.define::<models::v1::PeerTransaction>().unwrap();
    models.define::<models::v1::BlockedPeerRecord>().unwrap();
    models.define::<models::v1::LlmServerSecretRecord>().unwrap();
    models.define::<models::v1::LlmServerAdminTokenRecord>().unwrap();
    models.define::<models::v1::Libp2pKeyRecord>().unwrap();
    models.define::<models::v1::RoomKeyRecord>().unwrap();
    models.define::<models::v1::PeerAuthKeyRecord>().unwrap();
    models
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionView {
    pub req_id: String,
    pub consumer_peer_id: Option<uuid::Uuid>,
    pub provider_peer_id: Option<uuid::Uuid>,
    pub model: Option<String>,
    pub total_tokens: Option<i32>,
    pub same_service: Option<bool>,
    pub consumer_credit_delta: Option<i64>,
    pub provider_credit_delta: Option<i64>,
    pub status: String,
    pub settled_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionListItem {
    pub req_id: String,
    pub role: String,
    pub counterparty_peer_id: String,
    pub consumer_peer_id: String,
    pub provider_peer_id: String,
    pub model: String,
    pub total_tokens: u32,
    pub local_credit_delta: i64,
    pub status: String,
    pub reported_at_unix: u64,
    pub settled_at_unix: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreditBucket {
    pub consumed: i64,
    pub earned: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelCreditStats {
    pub model: String,
    pub consumed: i64,
    pub earned: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerCreditStats {
    pub peer_id: String,
    pub consumed: i64,
    pub earned: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionStats {
    pub by_model: Vec<ModelCreditStats>,
    pub by_peer: Vec<PeerCreditStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockedPeerView {
    pub peer_id: String,
    pub blocked_at_unix: u64,
    pub source: String,
    pub reason: String,
}

#[derive(Clone)]
pub struct TxStore {
    db: Arc<Database<'static>>,
}

impl TxStore {
    pub fn open_default() -> anyhow::Result<Self> {
        Self::open(&tx_db_path())
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let db = Builder::new().create_in_memory(&MODELS)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub fn open(path: &str) -> anyhow::Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let db = if Path::new(path).exists() {
            Builder::new().open(&MODELS, path)?
        } else {
            Builder::new().create(&MODELS, path)?
        };
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub async fn record_local_report(
        &self,
        req_id: String,
        role: &str,
        peer_id: &str,
        remote_peer_id: String,
        model: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let role = role.to_string();
        let peer_id = peer_id.to_string();
        tokio::task::spawn_blocking(move || {
            store.record_local_report_sync(
                req_id,
                &role,
                &peer_id,
                remote_peer_id,
                model,
                prompt_tokens,
                completion_tokens,
                total_tokens,
            )
        })
        .await??;
        Ok(())
    }

    fn record_local_report_sync(
        &self,
        req_id: String,
        role: &str,
        peer_id: &str,
        remote_peer_id: String,
        model: String,
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    ) -> anyhow::Result<()> {
        let now = unix_now();
        let (consumer_peer_id, provider_peer_id) = if role == "consumer" {
            (peer_id.to_string(), remote_peer_id.clone())
        } else {
            (remote_peer_id.clone(), peer_id.to_string())
        };

        let mut tx = self.load_transaction(&req_id).unwrap_or(PeerTransaction {
            req_id: req_id.clone(),
            consumer_peer_id,
            provider_peer_id,
            model: model.clone(),
            counterparty_peer_id: remote_peer_id.clone(),
            status: "pending".to_string(),
            reported_at_unix: now,
            role: role.to_string(),
            prompt_tokens,
            completion_tokens,
            total_tokens,
            consumer_credit_delta: 0,
            provider_credit_delta: 0,
            local_credit_delta: 0,
            same_service: false,
            settled_at_unix: 0,
        });

        if tx.reported_at_unix == 0 {
            tx.reported_at_unix = now;
        }
        tx.role = role.to_string();
        tx.counterparty_peer_id = remote_peer_id;
        tx.model = model;
        tx.prompt_tokens = prompt_tokens;
        tx.completion_tokens = completion_tokens;
        tx.total_tokens = total_tokens;
        if tx.status.is_empty() {
            tx.status = "pending".to_string();
        }

        self.upsert_peer(&tx.consumer_peer_id, now)?;
        self.upsert_peer(&tx.provider_peer_id, now)?;
        self.upsert_transaction(tx)?;
        Ok(())
    }

    pub async fn upsert_from_lobby(
        &self,
        view: TransactionView,
        local_peer_id: &str,
        peer_stats: Option<&crate::peer_stats::PeerStatsTrackerHandle>,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let local_peer_id = local_peer_id.to_string();
        let peer_stats = peer_stats.cloned();
        tokio::task::spawn_blocking(move || {
            store.upsert_from_lobby_sync(view, &local_peer_id, peer_stats.as_ref())
        })
        .await??;
        Ok(())
    }

    fn upsert_from_lobby_sync(
        &self,
        view: TransactionView,
        local_peer_id: &str,
        peer_stats: Option<&crate::peer_stats::PeerStatsTrackerHandle>,
    ) -> anyhow::Result<()> {
        let consumer = view
            .consumer_peer_id
            .map(|id| id.to_string())
            .unwrap_or_default();
        let provider = view
            .provider_peer_id
            .map(|id| id.to_string())
            .unwrap_or_default();

        if local_peer_id != consumer && local_peer_id != provider {
            return Ok(());
        }

        let role = if local_peer_id == consumer {
            "consumer"
        } else {
            "provider"
        };
        let counterparty = if local_peer_id == consumer {
            provider.clone()
        } else {
            consumer.clone()
        };

        let consumer_delta = view.consumer_credit_delta.unwrap_or(0);
        let provider_delta = view.provider_credit_delta.unwrap_or(0);
        let local_credit_delta = if local_peer_id == consumer {
            consumer_delta
        } else {
            provider_delta
        };

        let settled_at_unix = view
            .settled_at
            .map(|dt| dt.timestamp().max(0) as u64)
            .unwrap_or(0);

        let now = unix_now();
        let mut tx = self.load_transaction(&view.req_id).unwrap_or(PeerTransaction {
            req_id: view.req_id.clone(),
            consumer_peer_id: consumer.clone(),
            provider_peer_id: provider.clone(),
            model: view.model.clone().unwrap_or_default(),
            counterparty_peer_id: counterparty.clone(),
            status: view.status.clone(),
            reported_at_unix: settled_at_unix.max(now),
            role: role.to_string(),
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: view.total_tokens.unwrap_or(0).max(0) as u32,
            consumer_credit_delta: consumer_delta,
            provider_credit_delta: provider_delta,
            local_credit_delta,
            same_service: view.same_service.unwrap_or(false),
            settled_at_unix,
        });

        if view.status == "mismatched" && tx.status != "mismatched" {
            if let Some(handle) = peer_stats {
                if let Ok(mut guard) = handle.lock() {
                    guard.on_token_mismatch(&counterparty);
                }
            }
        }

        tx.consumer_peer_id = consumer;
        tx.provider_peer_id = provider;
        tx.counterparty_peer_id = counterparty;
        if let Some(model) = view.model {
            tx.model = model;
        }
        if let Some(total) = view.total_tokens {
            tx.total_tokens = total.max(0) as u32;
        }
        tx.status = view.status;
        tx.same_service = view.same_service.unwrap_or(false);
        tx.consumer_credit_delta = consumer_delta;
        tx.provider_credit_delta = provider_delta;
        tx.local_credit_delta = local_credit_delta;
        tx.role = role.to_string();
        tx.settled_at_unix = settled_at_unix;
        if tx.reported_at_unix == 0 {
            tx.reported_at_unix = settled_at_unix.max(now);
        }

        self.upsert_peer(&tx.consumer_peer_id, now)?;
        self.upsert_peer(&tx.provider_peer_id, now)?;
        self.upsert_transaction(tx)?;
        Ok(())
    }

    pub async fn token_totals_by_counterparty(&self) -> anyhow::Result<HashMap<String, u64>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.token_totals_by_counterparty_sync())
            .await?
            .map_err(anyhow::Error::from)
    }

    fn token_totals_by_counterparty_sync(&self) -> anyhow::Result<HashMap<String, u64>> {
        let mut totals: HashMap<String, u64> = HashMap::new();
        for tx in self.all_transactions()? {
            *totals
                .entry(tx.counterparty_peer_id.clone())
                .or_insert(0) += u64::from(tx.total_tokens);
        }
        Ok(totals)
    }

    pub async fn list_transactions(
        &self,
        limit: usize,
        offset: usize,
    ) -> anyhow::Result<Vec<TransactionListItem>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.list_transactions_sync(limit, offset))
            .await?
            .map_err(anyhow::Error::from)
    }

    pub async fn stats(
        &self,
        local_peer_id: &str,
        since_unix: Option<u64>,
    ) -> anyhow::Result<TransactionStats> {
        let store = self.clone();
        let local_peer_id = local_peer_id.to_string();
        tokio::task::spawn_blocking(move || store.stats_sync(&local_peer_id, since_unix))
            .await?
            .map_err(anyhow::Error::from)
    }

    pub async fn block_peer(
        &self,
        peer_id: &str,
        source: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let peer_id = peer_id.to_string();
        let source = source.to_string();
        let reason = reason.unwrap_or("").to_string();
        tokio::task::spawn_blocking(move || store.block_peer_sync(&peer_id, &source, &reason))
            .await??;
        Ok(())
    }

    pub async fn unblock_peer(&self, peer_id: &str) -> anyhow::Result<bool> {
        let store = self.clone();
        let peer_id = peer_id.to_string();
        tokio::task::spawn_blocking(move || store.unblock_peer_sync(&peer_id))
            .await?
            .map_err(anyhow::Error::from)
    }

    pub async fn is_peer_blocked(&self, peer_id: &str) -> anyhow::Result<bool> {
        let store = self.clone();
        let peer_id = peer_id.to_string();
        tokio::task::spawn_blocking(move || Ok(store.is_peer_blocked_sync(&peer_id)))
            .await?
    }

    pub async fn list_blocked_peers(&self) -> anyhow::Result<Vec<BlockedPeerView>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.list_blocked_peers_sync())
            .await?
            .map_err(anyhow::Error::from)
    }

    fn block_peer_sync(&self, peer_id: &str, source: &str, reason: &str) -> anyhow::Result<()> {
        let now = unix_now();
        let record = BlockedPeerRecord {
            peer_id: peer_id.to_string(),
            blocked_at_unix: now,
            source: source.to_string(),
            reason: reason.to_string(),
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(record)?;
        rw.commit()?;
        Ok(())
    }

    fn unblock_peer_sync(&self, peer_id: &str) -> anyhow::Result<bool> {
        let Some(record) = self.load_blocked_peer(peer_id) else {
            return Ok(false);
        };
        let rw = self.db.rw_transaction()?;
        rw.remove(record)?;
        rw.commit()?;
        Ok(true)
    }

    fn is_peer_blocked_sync(&self, peer_id: &str) -> bool {
        self.load_blocked_peer(peer_id).is_some()
    }

    fn list_blocked_peers_sync(&self) -> anyhow::Result<Vec<BlockedPeerView>> {
        let r = self.db.r_transaction()?;
        let scan = r.scan().primary::<BlockedPeerRecord>()?;
        let mut rows = Vec::new();
        for item in scan.all()? {
            let rec = item?;
            rows.push(BlockedPeerView {
                peer_id: rec.peer_id,
                blocked_at_unix: rec.blocked_at_unix,
                source: rec.source,
                reason: rec.reason,
            });
        }
        rows.sort_by(|a, b| b.blocked_at_unix.cmp(&a.blocked_at_unix));
        Ok(rows)
    }

    fn load_blocked_peer(&self, peer_id: &str) -> Option<BlockedPeerRecord> {
        let r = self.db.r_transaction().ok()?;
        r.get()
            .primary::<BlockedPeerRecord>(peer_id.to_string())
            .ok()?
    }

    fn list_transactions_sync(
        &self,
        limit: usize,
        offset: usize,
    ) -> anyhow::Result<Vec<TransactionListItem>> {
        let mut rows = self.all_transactions()?;
        rows.sort_by(|a, b| b.reported_at_unix.cmp(&a.reported_at_unix));
        let items = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|tx| TransactionListItem {
                req_id: tx.req_id,
                role: tx.role,
                counterparty_peer_id: tx.counterparty_peer_id,
                consumer_peer_id: tx.consumer_peer_id,
                provider_peer_id: tx.provider_peer_id,
                model: tx.model,
                total_tokens: tx.total_tokens,
                local_credit_delta: tx.local_credit_delta,
                status: tx.status,
                reported_at_unix: tx.reported_at_unix,
                settled_at_unix: tx.settled_at_unix,
            })
            .collect();
        Ok(items)
    }

    fn stats_sync(
        &self,
        local_peer_id: &str,
        since_unix: Option<u64>,
    ) -> anyhow::Result<TransactionStats> {
        let rows: Vec<_> = self
            .all_transactions()?
            .into_iter()
            .filter(|tx| tx.status == "settled")
            .filter(|tx| {
                tx.consumer_peer_id == local_peer_id || tx.provider_peer_id == local_peer_id
            })
            .filter(|tx| {
                since_unix.is_none_or(|since| {
                    let ts = if tx.settled_at_unix > 0 {
                        tx.settled_at_unix
                    } else {
                        tx.reported_at_unix
                    };
                    ts >= since
                })
            })
            .collect();

        let mut by_model: HashMap<String, CreditBucket> = HashMap::new();
        let mut by_peer: HashMap<String, CreditBucket> = HashMap::new();

        for tx in rows {
            let delta = tx.local_credit_delta;
            if delta == 0 {
                continue;
            }
            let model_bucket = by_model.entry(tx.model.clone()).or_default();
            let peer_bucket = by_peer
                .entry(tx.counterparty_peer_id.clone())
                .or_default();
            if delta < 0 {
                let spent = delta.unsigned_abs() as i64;
                model_bucket.consumed += spent;
                peer_bucket.consumed += spent;
            } else {
                model_bucket.earned += delta;
                peer_bucket.earned += delta;
            }
        }

        let mut by_model: Vec<ModelCreditStats> = by_model
            .into_iter()
            .map(|(model, bucket)| ModelCreditStats {
                model,
                consumed: bucket.consumed,
                earned: bucket.earned,
            })
            .collect();
        by_model.sort_by(|a, b| {
            (b.consumed + b.earned).cmp(&(a.consumed + a.earned))
        });

        let mut by_peer: Vec<PeerCreditStats> = by_peer
            .into_iter()
            .map(|(peer_id, bucket)| PeerCreditStats {
                peer_id,
                consumed: bucket.consumed,
                earned: bucket.earned,
            })
            .collect();
        by_peer.sort_by(|a, b| {
            (b.consumed + b.earned).cmp(&(a.consumed + a.earned))
        });

        Ok(TransactionStats { by_model, by_peer })
    }

    fn upsert_peer(&self, peer_id: &str, ts: u64) -> anyhow::Result<()> {
        if peer_id.is_empty() {
            return Ok(());
        }
        let existing = self.load_peer(peer_id);
        let record = match existing {
            Some(mut rec) => {
                rec.last_seen_unix = ts;
                rec
            }
            None => PeerRecord {
                peer_id: peer_id.to_string(),
                first_seen_unix: ts,
                last_seen_unix: ts,
            },
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(record)?;
        rw.commit()?;
        Ok(())
    }

    fn upsert_transaction(&self, tx: PeerTransaction) -> anyhow::Result<()> {
        let rw = self.db.rw_transaction()?;
        rw.upsert(tx)?;
        rw.commit()?;
        Ok(())
    }

    fn load_transaction(&self, req_id: &str) -> Option<PeerTransaction> {
        let r = self.db.r_transaction().ok()?;
        r.get()
            .primary::<PeerTransaction>(req_id.to_string())
            .ok()?
    }

    fn load_peer(&self, peer_id: &str) -> Option<PeerRecord> {
        let r = self.db.r_transaction().ok()?;
        r.get()
            .primary::<PeerRecord>(peer_id.to_string())
            .ok()?
    }

    fn all_transactions(&self) -> anyhow::Result<Vec<PeerTransaction>> {
        let r = self.db.r_transaction()?;
        let mut rows = Vec::new();
        let scan = r.scan().primary::<PeerTransaction>()?;
        let iter = scan.all()?;
        for item in iter {
            rows.push(item?);
        }
        Ok(rows)
    }
}

impl Default for CreditBucket {
    fn default() -> Self {
        Self {
            consumed: 0,
            earned: 0,
        }
    }
}

pub fn spawn_transaction_sync(proxy_state: Arc<crate::llm_proxy::ProxyState>, tx_store: Arc<TxStore>) {
    tokio::spawn(async move {
        loop {
            if proxy_state.setup_complete.load(std::sync::atomic::Ordering::SeqCst) {
                sync_transactions_once(&proxy_state, &tx_store).await;
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });
}

async fn sync_transactions_once(
    proxy_state: &crate::llm_proxy::ProxyState,
    tx_store: &TxStore,
) {
    let local_peer_id = proxy_state.shared_state.lock().await.peer_id.clone();
    if local_peer_id.is_empty() {
        return;
    }

    let lobby_host = proxy_state.lobby_host.clone();
    let url = crate::lobby_url::lobby_api_url(&lobby_host, "/api/public/transactions?limit=200");
    let Ok(resp) = proxy_state.http_client.get(&url).send().await else {
        return;
    };
    if !resp.status().is_success() {
        return;
    }
    let Ok(rows) = resp.json::<Vec<TransactionView>>().await else {
        return;
    };

    for row in rows {
        if let Err(e) = tx_store
            .upsert_from_lobby(row, &local_peer_id, Some(&proxy_state.peer_stats))
            .await
        {
            eprintln!("tx_db lobby sync error: {}", e);
        }
    }
}
