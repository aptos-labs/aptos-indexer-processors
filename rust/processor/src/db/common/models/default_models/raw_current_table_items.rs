use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawCurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

pub trait CurrentTableItemConvertible {
    fn from_raw(raw_item: &RawCurrentTableItem) -> Self;
}
