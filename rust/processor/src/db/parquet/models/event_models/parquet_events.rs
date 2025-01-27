// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]
use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::{common::models::event_models::raw_events::RawEvent, parquet::ParquetConvertible},
};
use allocative_derive::Allocative;
use lazy_static::lazy_static;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// This is for future proofing. TODO: change when events v2 comes
const EVENT_VERSION: i8 = 1i8;

lazy_static! {
    pub static ref DEFAULT_ACCOUNT_ADDRESS: String = "NULL_ACCOUNT_ADDRESS".to_string();
    pub static ref DEFAULT_EVENT_TYPE: String = "NULL_EVENT_TYPE".to_string();
    pub static ref DEFAULT_EVENT_DATA: String = "NULL_EVENT_DATA".to_string();
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct Event {
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
    pub event_version: i8,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for Event {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for Event {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for Event {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl ParquetConvertible for RawEvent {
    type ParquetModelType = Event;

    fn to_parquet(&self) -> Self::ParquetModelType {
        Event {
            txn_version: self.transaction_version,
            account_address: self.account_address.clone(),
            sequence_number: self.sequence_number,
            creation_number: self.creation_number,
            block_height: self.transaction_block_height,
            event_type: self.type_.clone(),
            data: self.data.clone(),
            event_index: self.event_index,
            indexed_type: self.indexed_type.clone(),
            type_tag_bytes: self.type_tag_bytes.unwrap_or(0),
            total_bytes: self.total_bytes.unwrap_or(0),
            event_version: EVENT_VERSION,
            block_timestamp: self.block_timestamp.unwrap(),
        }
    }
}

pub type ParquetEventModel = Event;
