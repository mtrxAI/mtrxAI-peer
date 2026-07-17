use anyhow::Result;

use super::models::{Libp2pKeyRecord, PeerAuthKeyRecord, RoomKeyRecord};
use super::TxStore;

impl TxStore {
    pub fn put_libp2p_key_sync(&self, mtrxai_peer_id: &str, key_protobuf: &[u8]) -> Result<()> {
        let record = Libp2pKeyRecord {
            mtrxai_peer_id: mtrxai_peer_id.to_string(),
            key_protobuf: key_protobuf.to_vec(),
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(record)?;
        rw.commit()?;
        Ok(())
    }

    pub fn get_libp2p_key_sync(&self, mtrxai_peer_id: &str) -> Result<Option<Vec<u8>>> {
        let r = self.db.r_transaction()?;
        let record: Option<Libp2pKeyRecord> = r.get().primary(mtrxai_peer_id.to_string())?;
        Ok(record.map(|r| r.key_protobuf))
    }

    pub fn put_room_key_sync(&self, room_id: &str, key_blob: &[u8]) -> Result<()> {
        let record = RoomKeyRecord {
            room_id: room_id.to_string(),
            key_blob: key_blob.to_vec(),
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(record)?;
        rw.commit()?;
        Ok(())
    }

    pub fn get_room_key_sync(&self, room_id: &str) -> Result<Option<Vec<u8>>> {
        let r = self.db.r_transaction()?;
        let record: Option<RoomKeyRecord> = r.get().primary(room_id.to_string())?;
        Ok(record.map(|r| r.key_blob))
    }

    pub async fn put_room_key(&self, room_id: &str, key_blob: &[u8]) -> Result<()> {
        let store = self.clone();
        let room_id = room_id.to_string();
        let key_blob = key_blob.to_vec();
        tokio::task::spawn_blocking(move || store.put_room_key_sync(&room_id, &key_blob)).await??;
        Ok(())
    }

    pub async fn get_room_key(&self, room_id: &str) -> Result<Option<Vec<u8>>> {
        let store = self.clone();
        let room_id = room_id.to_string();
        tokio::task::spawn_blocking(move || store.get_room_key_sync(&room_id)).await?
    }

    pub fn put_peer_auth_key_sync(&self, seed_hex: &str) -> Result<()> {
        let record = PeerAuthKeyRecord {
            record_id: "__mtrxai_peer_auth__".to_string(),
            seed_hex: seed_hex.to_string(),
        };
        let rw = self.db.rw_transaction()?;
        rw.upsert(record)?;
        rw.commit()?;
        Ok(())
    }

    pub fn get_peer_auth_key_sync(&self) -> Result<Option<String>> {
        let r = self.db.r_transaction()?;
        let record: Option<PeerAuthKeyRecord> = r.get().primary("__mtrxai_peer_auth__".to_string())?;
        Ok(record.map(|r| r.seed_hex))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_key_round_trip() {
        let store = TxStore::open_in_memory().unwrap();
        store.put_room_key_sync("room-1", b"secret-key-blob").unwrap();
        let got = store.get_room_key_sync("room-1").unwrap().unwrap();
        assert_eq!(got, b"secret-key-blob");
    }
}
