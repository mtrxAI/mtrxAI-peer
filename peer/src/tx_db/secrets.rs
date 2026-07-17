use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use getrandom::getrandom;
use sha2::{Digest, Sha256};

use super::models::{LlmServerAdminTokenRecord, LlmServerSecretRecord};
use super::TxStore;

const NONCE_LEN: usize = 12;

fn machine_secret_key(peer_id: Option<&str>, service_id: Option<&str>) -> [u8; 32] {
    let hostname = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "mtrxai".to_string());
    let mut hasher = Sha256::new();
    hasher.update(b"mtrxai-llm-server-secret-v1");
    hasher.update(hostname.as_bytes());
    hasher.update(peer_id.unwrap_or("").as_bytes());
    hasher.update(service_id.unwrap_or("").as_bytes());
    hasher.finalize().into()
}

fn encrypt_api_key(
    plaintext: &str,
    peer_id: Option<&str>,
    service_id: Option<&str>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = machine_secret_key(peer_id, service_id);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!(e))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom(&mut nonce_bytes).map_err(|e| anyhow!(e))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow!("encrypt failed: {}", e))?;
    Ok((nonce_bytes.to_vec(), ciphertext))
}

fn decrypt_api_key(
    nonce: &[u8],
    ciphertext: &[u8],
    peer_id: Option<&str>,
    service_id: Option<&str>,
) -> Result<String> {
    let key = machine_secret_key(peer_id, service_id);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!(e))?;
    let nonce = Nonce::from_slice(nonce);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow!("decrypt failed: {}", e))?;
    String::from_utf8(plaintext).map_err(|e| anyhow!(e))
}

impl TxStore {
    pub fn put_server_api_key_sync(
        &self,
        server_id: &str,
        api_key: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<()> {
        let (nonce, ciphertext) = encrypt_api_key(api_key, peer_id, service_id)?;
        let record = LlmServerSecretRecord {
            server_id: server_id.to_string(),
            nonce,
            ciphertext,
        };
        let rw = self.db.rw_transaction()?;
        rw.insert(record)?;
        rw.commit()?;
        Ok(())
    }

    pub fn get_server_api_key_sync(
        &self,
        server_id: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<Option<String>> {
        let r = self.db.r_transaction()?;
        let record: Option<LlmServerSecretRecord> = r.get().primary(server_id.to_string())?;
        let Some(record) = record else {
            return Ok(None);
        };
        let plaintext = decrypt_api_key(&record.nonce, &record.ciphertext, peer_id, service_id)?;
        Ok(Some(plaintext))
    }

    pub fn delete_server_api_key_sync(&self, server_id: &str) -> Result<()> {
        let rw = self.db.rw_transaction()?;
        if let Some(record) = rw.get().primary::<LlmServerSecretRecord>(server_id.to_string())? {
            rw.remove(record)?;
        }
        rw.commit()?;
        Ok(())
    }

    pub fn has_server_api_key_sync(&self, server_id: &str) -> Result<bool> {
        let r = self.db.r_transaction()?;
        let record: Option<LlmServerSecretRecord> = r.get().primary(server_id.to_string())?;
        Ok(record.is_some())
    }

    pub async fn put_server_api_key(
        &self,
        server_id: &str,
        api_key: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<()> {
        let store = self.clone();
        let server_id = server_id.to_string();
        let api_key = api_key.to_string();
        let peer_id = peer_id.map(str::to_string);
        let service_id = service_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            store.put_server_api_key_sync(
                &server_id,
                &api_key,
                peer_id.as_deref(),
                service_id.as_deref(),
            )
        })
        .await??;
        Ok(())
    }

    pub async fn get_server_api_key(
        &self,
        server_id: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<Option<String>> {
        let store = self.clone();
        let server_id = server_id.to_string();
        let peer_id = peer_id.map(str::to_string);
        let service_id = service_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            store.get_server_api_key_sync(
                &server_id,
                peer_id.as_deref(),
                service_id.as_deref(),
            )
        })
        .await?
    }

    pub async fn delete_server_api_key(&self, server_id: &str) -> Result<()> {
        let store = self.clone();
        let server_id = server_id.to_string();
        tokio::task::spawn_blocking(move || store.delete_server_api_key_sync(&server_id)).await?
    }

    pub async fn has_server_api_key(&self, server_id: &str) -> Result<bool> {
        let store = self.clone();
        let server_id = server_id.to_string();
        tokio::task::spawn_blocking(move || store.has_server_api_key_sync(&server_id)).await?
    }

    pub fn put_server_admin_token_sync(
        &self,
        server_id: &str,
        admin_token: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<()> {
        let (nonce, ciphertext) = encrypt_api_key(admin_token, peer_id, service_id)?;
        let record = LlmServerAdminTokenRecord {
            server_id: server_id.to_string(),
            nonce,
            ciphertext,
        };
        let rw = self.db.rw_transaction()?;
        rw.insert(record)?;
        rw.commit()?;
        Ok(())
    }

    pub fn get_server_admin_token_sync(
        &self,
        server_id: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<Option<String>> {
        let r = self.db.r_transaction()?;
        let record: Option<LlmServerAdminTokenRecord> = r.get().primary(server_id.to_string())?;
        let Some(record) = record else {
            return Ok(None);
        };
        let plaintext = decrypt_api_key(&record.nonce, &record.ciphertext, peer_id, service_id)?;
        Ok(Some(plaintext))
    }

    pub fn delete_server_admin_token_sync(&self, server_id: &str) -> Result<()> {
        let rw = self.db.rw_transaction()?;
        if let Some(record) = rw.get().primary::<LlmServerAdminTokenRecord>(server_id.to_string())? {
            rw.remove(record)?;
        }
        rw.commit()?;
        Ok(())
    }

    pub fn has_server_admin_token_sync(&self, server_id: &str) -> Result<bool> {
        let r = self.db.r_transaction()?;
        let record: Option<LlmServerAdminTokenRecord> = r.get().primary(server_id.to_string())?;
        Ok(record.is_some())
    }

    pub async fn put_server_admin_token(
        &self,
        server_id: &str,
        admin_token: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<()> {
        let store = self.clone();
        let server_id = server_id.to_string();
        let admin_token = admin_token.to_string();
        let peer_id = peer_id.map(str::to_string);
        let service_id = service_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            store.put_server_admin_token_sync(
                &server_id,
                &admin_token,
                peer_id.as_deref(),
                service_id.as_deref(),
            )
        })
        .await??;
        Ok(())
    }

    pub async fn get_server_admin_token(
        &self,
        server_id: &str,
        peer_id: Option<&str>,
        service_id: Option<&str>,
    ) -> Result<Option<String>> {
        let store = self.clone();
        let server_id = server_id.to_string();
        let peer_id = peer_id.map(str::to_string);
        let service_id = service_id.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            store.get_server_admin_token_sync(
                &server_id,
                peer_id.as_deref(),
                service_id.as_deref(),
            )
        })
        .await?
    }

    pub async fn delete_server_admin_token(&self, server_id: &str) -> Result<()> {
        let store = self.clone();
        let server_id = server_id.to_string();
        tokio::task::spawn_blocking(move || store.delete_server_admin_token_sync(&server_id)).await?
    }

    pub async fn has_server_admin_token(&self, server_id: &str) -> Result<bool> {
        let store = self.clone();
        let server_id = server_id.to_string();
        tokio::task::spawn_blocking(move || store.has_server_admin_token_sync(&server_id)).await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_round_trip() {
        let store = TxStore::open_in_memory().unwrap();
        store
            .put_server_api_key_sync("srv-1", "sk-test-key", Some("peer"), Some("svc"))
            .unwrap();
        assert!(store.has_server_api_key_sync("srv-1").unwrap());
        let key = store
            .get_server_api_key_sync("srv-1", Some("peer"), Some("svc"))
            .unwrap()
            .unwrap();
        assert_eq!(key, "sk-test-key");
        store.delete_server_api_key_sync("srv-1").unwrap();
        assert!(!store.has_server_api_key_sync("srv-1").unwrap());
    }

    #[test]
    fn admin_token_round_trip() {
        let store = TxStore::open_in_memory().unwrap();
        store
            .put_server_admin_token_sync("srv-2", "cell-admin-token", Some("peer"), Some("svc"))
            .unwrap();
        assert!(store.has_server_admin_token_sync("srv-2").unwrap());
        let token = store
            .get_server_admin_token_sync("srv-2", Some("peer"), Some("svc"))
            .unwrap()
            .unwrap();
        assert_eq!(token, "cell-admin-token");
        store.delete_server_admin_token_sync("srv-2").unwrap();
        assert!(!store.has_server_admin_token_sync("srv-2").unwrap());
    }
}
