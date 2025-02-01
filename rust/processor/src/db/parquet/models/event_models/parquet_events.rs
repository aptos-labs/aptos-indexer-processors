// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]
use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::event_models::raw_events::RawEvent,
};
use allocative_derive::Allocative;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct EventPQ {
    pub txn_version: i64,
    pub account_address: String,
    pub sequence_number: i64,
    pub creation_number: i64,
    pub block_height: i64,
    pub event_type: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for EventPQ {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for EventPQ {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for EventPQ {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl From<RawEvent> for EventPQ {
    fn from(raw_event: RawEvent) -> Self {
        EventPQ {
            txn_version: raw_event.transaction_version,
            account_address: raw_event.account_address,
            sequence_number: raw_event.sequence_number,
            creation_number: raw_event.creation_number,
            block_height: raw_event.transaction_block_height,
            event_type: raw_event.type_,
            data: raw_event.data,
            event_index: raw_event.event_index,
            indexed_type: raw_event.indexed_type,
            type_tag_bytes: raw_event.type_tag_bytes.unwrap_or(0),
            total_bytes: raw_event.total_bytes.unwrap_or(0),
            block_timestamp: raw_event.block_timestamp.unwrap(),
        }
    }
}
