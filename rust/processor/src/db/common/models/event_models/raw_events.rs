use crate::utils::{
    counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
    util::{parse_timestamp, standardize_address, truncate_str},
};
use aptos_protos::transaction::v1::{
    transaction::TxnData, Event as EventPB, EventSizeInfo, Transaction,
};
use serde::{Deserialize, Serialize};
use tracing::warn;
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

impl RawEvent {
    fn from_event(
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

pub fn parse_events(txn: &Transaction, processor_name: &str) -> Vec<RawEvent> {
    let txn_version = txn.version as i64;
    let block_height = txn.block_height as i64;
    let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
    let size_info = match txn.size_info.as_ref() {
        Some(size_info) => Some(size_info),
        None => {
            warn!(version = txn.version, "Transaction size info not found");
            None
        },
    };
    let txn_data = match txn.txn_data.as_ref() {
        Some(data) => data,
        None => {
            warn!(
                transaction_version = txn_version,
                "Transaction data doesn't exist"
            );
            PROCESSOR_UNKNOWN_TYPE_COUNT
                .with_label_values(&[processor_name])
                .inc();
            return vec![];
        },
    };
    let default = vec![];
    let raw_events = match txn_data {
        TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
        TxnData::Genesis(tx_inner) => &tx_inner.events,
        TxnData::User(tx_inner) => &tx_inner.events,
        TxnData::Validator(tx_inner) => &tx_inner.events,
        _ => &default,
    };

    let event_size_info = size_info.map(|info| info.event_size_info.as_slice());

    raw_events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            // event_size_info will be used for user transactions only, no promises for other transactions.
            // If event_size_info is missing due, it defaults to 0.
            // No need to backfill as event_size_info is primarily for debugging user transactions.
            let size_info = event_size_info.and_then(|infos| infos.get(index));
            RawEvent::from_event(
                event,
                txn_version,
                block_height,
                index as i64,
                size_info,
                Some(block_timestamp),
            )
        })
        .collect::<Vec<RawEvent>>()
}
