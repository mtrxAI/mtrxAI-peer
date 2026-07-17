use crate::shared::{PeerConnectionView, PeerTrafficStats};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub type PeerStatsTrackerHandle = Arc<Mutex<PeerStatsTracker>>;

const ROLLING_WINDOW_SECS: u64 = 60;
const TOKEN_SAMPLE_CAP: usize = 128;
const REQUEST_TS_CAP: usize = 128;
const RECENT_OUTCOMES_CAP: usize = 10;

const HIGH_REQUEST_RATE_PER_MIN: f64 = 30.0;
const HIGH_ERROR_RATE: f64 = 0.5;
const MIN_REQUESTS_FOR_ERROR_RATE: u32 = 3;
const LARGE_PAYLOAD_BYTES: u64 = 400 * 1024;
const SLOW_RESPONSE_MS: u64 = 120_000;
const MIN_REQUESTS_FOR_SLOW: u32 = 3;

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug)]
struct ActiveExchange {
    peer_id: String,
    model: String,
    started_at: Instant,
    bytes_sent: u64,
    bytes_received: u64,
    last_partial_tokens: u32,
}

#[derive(Debug, Default)]
struct PeerAccum {
    session_tokens: u64,
    total_requests: u32,
    total_errors: u32,
    active_requests: u32,
    bytes_sent: u64,
    bytes_received: u64,
    last_bytes_sent: u64,
    duration_sum_ms: u64,
    duration_count: u32,
    last_model: Option<String>,
    last_request_at_unix: u64,
    token_mismatch_count: u32,
    token_samples: VecDeque<(u64, u32)>,
    request_timestamps: VecDeque<u64>,
    recent_outcomes: VecDeque<bool>,
}

#[derive(Debug, Default)]
pub struct PeerStatsTracker {
    peers: HashMap<String, PeerAccum>,
    active: HashMap<String, ActiveExchange>,
}

impl PeerStatsTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_request_start(
        &mut self,
        req_id: &str,
        peer_id: &str,
        model: &str,
        bytes_sent: u64,
    ) {
        let now = unix_now();
        let accum = self.peers.entry(peer_id.to_string()).or_default();
        accum.active_requests = accum.active_requests.saturating_add(1);
        accum.bytes_sent = accum.bytes_sent.saturating_add(bytes_sent);
        accum.last_bytes_sent = bytes_sent;
        push_capped(&mut accum.request_timestamps, REQUEST_TS_CAP, now);

        self.active.insert(
            req_id.to_string(),
            ActiveExchange {
                peer_id: peer_id.to_string(),
                model: model.to_string(),
                started_at: Instant::now(),
                bytes_sent,
                bytes_received: 0,
                last_partial_tokens: 0,
            },
        );
    }

    pub fn on_stream_progress(&mut self, req_id: &str, bytes_received: u64, partial_tokens: u32) {
        let Some(active) = self.active.get_mut(req_id) else {
            return;
        };
        active.bytes_received = active.bytes_received.saturating_add(bytes_received);

        if partial_tokens > active.last_partial_tokens {
            let delta = partial_tokens - active.last_partial_tokens;
            active.last_partial_tokens = partial_tokens;
            let peer_id = active.peer_id.clone();
            let now = unix_now();
            let accum = self.peers.entry(peer_id).or_default();
            push_capped(&mut accum.token_samples, TOKEN_SAMPLE_CAP, (now, delta));
        }

        let peer_id = active.peer_id.clone();
        if let Some(accum) = self.peers.get_mut(&peer_id) {
            accum.bytes_received = accum.bytes_received.saturating_add(bytes_received);
        }
    }

    pub fn on_request_complete(&mut self, req_id: &str, tokens: u32, bytes_received: u64) {
        let Some(active) = self.active.remove(req_id) else {
            return;
        };
        let duration_ms = active.started_at.elapsed().as_millis() as u64;
        let peer_id = active.peer_id.clone();
        let bytes_sent = active.bytes_sent;
        let model = active.model;
        let now = unix_now();
        let accum = self.peers.entry(peer_id.clone()).or_default();

        accum.active_requests = accum.active_requests.saturating_sub(1);
        accum.total_requests = accum.total_requests.saturating_add(1);
        accum.session_tokens = accum.session_tokens.saturating_add(u64::from(tokens));
        accum.bytes_sent = accum.bytes_sent.saturating_add(bytes_sent);
        accum.bytes_received = accum.bytes_received.saturating_add(bytes_received);
        accum.last_bytes_sent = bytes_sent;
        accum.duration_sum_ms = accum.duration_sum_ms.saturating_add(duration_ms);
        accum.duration_count = accum.duration_count.saturating_add(1);
        accum.last_model = Some(model);
        accum.last_request_at_unix = now;
        push_capped(&mut accum.token_samples, TOKEN_SAMPLE_CAP, (now, tokens));
        push_capped(&mut accum.recent_outcomes, RECENT_OUTCOMES_CAP, true);
    }

    pub fn on_request_error(&mut self, req_id: &str) {
        let peer_id = self.active.remove(req_id).map(|a| a.peer_id);
        let Some(peer_id) = peer_id else {
            return;
        };
        let accum = self.peers.entry(peer_id).or_default();
        accum.active_requests = accum.active_requests.saturating_sub(1);
        accum.total_errors = accum.total_errors.saturating_add(1);
        push_capped(&mut accum.recent_outcomes, RECENT_OUTCOMES_CAP, false);
    }

    pub fn on_token_mismatch(&mut self, peer_id: &str) {
        let accum = self.peers.entry(peer_id.to_string()).or_default();
        accum.token_mismatch_count = accum.token_mismatch_count.saturating_add(1);
    }

    pub fn remove_peer(&mut self, peer_id: &str) {
        self.peers.remove(peer_id);
        self.active.retain(|_, a| a.peer_id != peer_id);
    }

    pub fn tracked_peer_ids(&self) -> Vec<String> {
        self.peers.keys().cloned().collect()
    }

    pub fn snapshot(&self, peer_id: &str, lifetime_tokens: u64) -> PeerTrafficStats {
        let accum = self.peers.get(peer_id);
        let now = unix_now();
        let cutoff = now.saturating_sub(ROLLING_WINDOW_SECS);

        let (
            session_tokens,
            total_requests,
            active_requests,
            error_count,
            bytes_sent,
            bytes_received,
            avg_duration_ms,
            last_model,
            token_mismatch_count,
            last_bytes_sent,
            recent_outcomes,
        ) = match accum {
            Some(a) => (
                a.session_tokens,
                a.total_requests,
                a.active_requests,
                a.total_errors,
                a.bytes_sent,
                a.bytes_received,
                if a.duration_count > 0 {
                    a.duration_sum_ms / u64::from(a.duration_count)
                } else {
                    0
                },
                a.last_model.clone(),
                a.token_mismatch_count,
                a.last_bytes_sent,
                a.recent_outcomes.clone(),
            ),
            None => (
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                VecDeque::new(),
            ),
        };

        let mut tokens_in_window = 0u64;
        if let Some(a) = accum {
            for (ts, tokens) in &a.token_samples {
                if *ts >= cutoff {
                    tokens_in_window = tokens_in_window.saturating_add(u64::from(*tokens));
                }
            }
            for active in self.active.values() {
                if active.peer_id == peer_id && active.last_partial_tokens > 0 {
                    tokens_in_window =
                        tokens_in_window.saturating_add(u64::from(active.last_partial_tokens));
                }
            }
        }

        let requests_in_window = accum
            .map(|a| a.request_timestamps.iter().filter(|ts| **ts >= cutoff).count() as f64)
            .unwrap_or(0.0);

        let tokens_per_sec = tokens_in_window as f64 / ROLLING_WINDOW_SECS as f64;
        let requests_per_min = requests_in_window * (60.0 / ROLLING_WINDOW_SECS as f64);

        let mut warnings = Vec::new();
        if requests_per_min > HIGH_REQUEST_RATE_PER_MIN {
            warnings.push("high_request_rate".to_string());
        }
        if token_mismatch_count > 0 {
            warnings.push("token_mismatch".to_string());
        }
        if last_bytes_sent > LARGE_PAYLOAD_BYTES {
            warnings.push("large_payload".to_string());
        }
        if total_requests >= MIN_REQUESTS_FOR_SLOW && avg_duration_ms > SLOW_RESPONSE_MS {
            warnings.push("slow_response".to_string());
        }
        if recent_outcomes.len() as u32 >= MIN_REQUESTS_FOR_ERROR_RATE {
            let errors = recent_outcomes.iter().filter(|ok| !**ok).count() as f64;
            if errors / recent_outcomes.len() as f64 >= HIGH_ERROR_RATE {
                warnings.push("high_error_rate".to_string());
            }
        }

        PeerTrafficStats {
            tokens_per_sec,
            requests_per_min,
            session_tokens,
            lifetime_tokens,
            total_requests,
            active_requests,
            error_count,
            bytes_sent,
            bytes_received,
            avg_duration_ms,
            last_model,
            token_mismatch_count,
            warnings,
        }
    }
}

pub fn refresh_live_stats(
    peers: &mut [PeerConnectionView],
    peer_stats: &PeerStatsTrackerHandle,
    lifetime_totals: &HashMap<String, u64>,
) {
    let Ok(guard) = peer_stats.lock() else {
        return;
    };
    for p in peers.iter_mut() {
        let lifetime = lifetime_totals.get(&p.peer_id).copied().unwrap_or(0);
        p.stats = Some(guard.snapshot(&p.peer_id, lifetime));
    }
}

fn push_capped<T>(buf: &mut VecDeque<T>, cap: usize, item: T) {
    if buf.len() >= cap {
        buf.pop_front();
    }
    buf.push_back(item);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_tokens_per_sec() {
        let mut tracker = PeerStatsTracker::new();
        let now = unix_now();
        tracker.on_request_start("r1", "peer-a", "llama3", 100);
        tracker.on_request_complete("r1", 600, 200);

        if let Some(accum) = tracker.peers.get_mut("peer-a") {
            accum.token_samples.clear();
            accum
                .token_samples
                .push_back((now.saturating_sub(30), 300));
            accum
                .token_samples
                .push_back((now.saturating_sub(10), 300));
        }

        let snap = tracker.snapshot("peer-a", 0);
        assert!((snap.tokens_per_sec - 10.0).abs() < 0.01);
    }

    #[test]
    fn high_request_rate_warning() {
        let mut tracker = PeerStatsTracker::new();
        let now = unix_now();
        let accum = tracker.peers.entry("peer-b".to_string()).or_default();
        for i in 0..35 {
            accum
                .request_timestamps
                .push_back(now.saturating_sub(u64::from(i as u32)));
        }
        let snap = tracker.snapshot("peer-b", 0);
        assert!(snap.warnings.contains(&"high_request_rate".to_string()));
    }

    #[test]
    fn active_requests_never_negative() {
        let mut tracker = PeerStatsTracker::new();
        tracker.on_request_start("r1", "peer-c", "m", 50);
        tracker.on_request_error("r1");
        tracker.on_request_error("r1");
        let snap = tracker.snapshot("peer-c", 0);
        assert_eq!(snap.active_requests, 0);
    }

    #[test]
    fn token_mismatch_warning() {
        let mut tracker = PeerStatsTracker::new();
        tracker.on_token_mismatch("peer-d");
        let snap = tracker.snapshot("peer-d", 0);
        assert!(snap.warnings.contains(&"token_mismatch".to_string()));
        assert_eq!(snap.token_mismatch_count, 1);
    }

    #[test]
    fn high_error_rate_warning() {
        let mut tracker = PeerStatsTracker::new();
        tracker.on_request_start("r1", "peer-e", "m", 10);
        tracker.on_request_error("r1");
        tracker.on_request_start("r2", "peer-e", "m", 10);
        tracker.on_request_error("r2");
        tracker.on_request_start("r3", "peer-e", "m", 10);
        tracker.on_request_complete("r3", 10, 10);
        let snap = tracker.snapshot("peer-e", 0);
        assert!(snap.warnings.contains(&"high_error_rate".to_string()));
    }
}
