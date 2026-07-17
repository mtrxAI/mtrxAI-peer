use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionAllowance {
    pub requester_peer_id: String,
    pub model: String,
    pub expires_at: Instant,
}

#[derive(Debug, Default)]
pub struct ConnectionAllowanceStore {
    allowances: HashMap<String, ConnectionAllowance>,
    ttl: Duration,
}

impl ConnectionAllowanceStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            allowances: HashMap::new(),
            ttl,
        }
    }

    pub fn grant(&mut self, req_id: String, requester_peer_id: String, model: String) {
        self.purge_expired();
        self.allowances.insert(
            req_id,
            ConnectionAllowance {
                requester_peer_id,
                model,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }

    pub fn validate_and_consume(
        &mut self,
        req_id: &str,
        requester_peer_id: &str,
    ) -> Option<String> {
        self.purge_expired();
        let allowance = self.allowances.remove(req_id)?;
        if allowance.expires_at <= Instant::now() {
            return None;
        }
        if allowance.requester_peer_id != requester_peer_id {
            return None;
        }
        Some(allowance.model)
    }

    pub fn purge_expired(&mut self) {
        let now = Instant::now();
        self.allowances
            .retain(|_, a| a.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_validate_and_consume() {
        let mut store = ConnectionAllowanceStore::new(Duration::from_secs(60));
        store.grant(
            "req1".to_string(),
            "peer-a".to_string(),
            "llama3".to_string(),
        );
        let model = store.validate_and_consume("req1", "peer-a");
        assert_eq!(model.as_deref(), Some("llama3"));
        assert!(store.validate_and_consume("req1", "peer-a").is_none());
    }

    #[test]
    fn reject_wrong_requester() {
        let mut store = ConnectionAllowanceStore::new(Duration::from_secs(60));
        store.grant(
            "req1".to_string(),
            "peer-a".to_string(),
            "llama3".to_string(),
        );
        assert!(store.validate_and_consume("req1", "peer-b").is_none());
    }

    #[test]
    fn reject_expired() {
        let mut store = ConnectionAllowanceStore::new(Duration::from_millis(1));
        store.grant(
            "req1".to_string(),
            "peer-a".to_string(),
            "llama3".to_string(),
        );
        std::thread::sleep(Duration::from_millis(5));
        assert!(store.validate_and_consume("req1", "peer-a").is_none());
    }
}
