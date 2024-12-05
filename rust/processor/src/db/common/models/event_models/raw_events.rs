use crate::utils::util::{standardize_address, truncate_str};
use aptos_protos::transaction::v1::{Event as EventPB, EventSizeInfo};
use serde::{Deserialize, Serialize};

/// P99 currently is 303 so using 300 as a safe max length
pub const EVENT_TYPE_MAX_LENGTH: usize = 300;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawEvent {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub block_timestamp: Option<chrono::NaiveDateTime>,
    pub type_tag_bytes: Option<i64>,
    pub total_bytes: Option<i64>,
}

pub trait EventConvertible {
    fn from_raw(raw_item: &RawEvent) -> Self;
}

impl RawEvent {
    pub fn from_raw_event(
        event: &EventPB,
        txn_version: i64,
        txn_block_height: i64,
        event_index: i64,
        size_info: Option<&EventSizeInfo>,
        block_timestamp: Option<chrono::NaiveDateTime>,
    ) -> RawEvent {
        let type_tag_bytes = size_info.map_or(0, |info| info.type_tag_bytes as i64);
        let total_bytes = size_info.map_or(0, |info| info.total_bytes as i64);
        let event_type = event.type_str.to_string();

        RawEvent {
            sequence_number: event.sequence_number as i64,
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            transaction_version: txn_version,
            transaction_block_height: txn_block_height,
            type_: event_type.clone(),
            data: event.data.clone(),
            event_index,
            indexed_type: truncate_str(&event_type, EVENT_TYPE_MAX_LENGTH),
            block_timestamp,
            type_tag_bytes: Some(type_tag_bytes),
            total_bytes: Some(total_bytes),
        }
    }
}
