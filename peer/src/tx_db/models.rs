use native_db::{native_db, ToKey};
use native_model::{native_model, Model};
use serde::{Deserialize, Serialize};

pub type PeerRecord = v1::PeerRecord;
pub type PeerTransaction = v1::PeerTransaction;
pub type BlockedPeerRecord = v1::BlockedPeerRecord;
pub type LlmServerSecretRecord = v1::LlmServerSecretRecord;
pub type LlmServerAdminTokenRecord = v1::LlmServerAdminTokenRecord;
pub type Libp2pKeyRecord = v1::Libp2pKeyRecord;
pub type RoomKeyRecord = v1::RoomKeyRecord;
pub type PeerAuthKeyRecord = v1::PeerAuthKeyRecord;

pub mod v1 {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 1, version = 1)]
    #[native_db]
    pub struct PeerRecord {
        #[primary_key]
        pub peer_id: String,
        pub first_seen_unix: u64,
        #[secondary_key]
        pub last_seen_unix: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 2, version = 1)]
    #[native_db]
    pub struct PeerTransaction {
        #[primary_key]
        pub req_id: String,
        #[secondary_key]
        pub consumer_peer_id: String,
        #[secondary_key]
        pub provider_peer_id: String,
        #[secondary_key]
        pub model: String,
        #[secondary_key]
        pub counterparty_peer_id: String,
        #[secondary_key]
        pub status: String,
        #[secondary_key]
        pub reported_at_unix: u64,
        pub role: String,
        pub prompt_tokens: u32,
        pub completion_tokens: u32,
        pub total_tokens: u32,
        pub consumer_credit_delta: i64,
        pub provider_credit_delta: i64,
        pub local_credit_delta: i64,
        pub same_service: bool,
        pub settled_at_unix: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 3, version = 1)]
    #[native_db]
    pub struct BlockedPeerRecord {
        #[primary_key]
        pub peer_id: String,
        pub blocked_at_unix: u64,
        #[secondary_key]
        pub source: String,
        pub reason: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 4, version = 1)]
    #[native_db]
    pub struct LlmServerSecretRecord {
        #[primary_key]
        pub server_id: String,
        pub nonce: Vec<u8>,
        pub ciphertext: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 8, version = 1)]
    #[native_db]
    pub struct LlmServerAdminTokenRecord {
        #[primary_key]
        pub server_id: String,
        pub nonce: Vec<u8>,
        pub ciphertext: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 5, version = 1)]
    #[native_db]
    pub struct Libp2pKeyRecord {
        #[primary_key]
        pub mtrxai_peer_id: String,
        pub key_protobuf: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 6, version = 1)]
    #[native_db]
    pub struct RoomKeyRecord {
        #[primary_key]
        pub room_id: String,
        pub key_blob: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[native_model(id = 7, version = 1)]
    #[native_db]
    pub struct PeerAuthKeyRecord {
        #[primary_key]
        pub record_id: String,
        pub seed_hex: String,
    }
}
