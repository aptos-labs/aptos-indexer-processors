// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{EventSizeInfo, WriteOpSizeInfo};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use crate::bq_analytics::generic_parquet_processor::{HasVersion, NamedTable};

#[derive(Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize)]
pub struct MoveModule {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub block_height: i64,
    pub name: String,
    pub address: String,
    pub bytecode: Option<Vec<u8>>,
    pub exposed_functions: Option<String>,
    pub friends: Option<String>,
    pub structs: Option<String>,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub txn_total_bytes: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MoveModuleByteCodeParsed {
    pub address: String,
    pub name: String,
    pub bytecode: Vec<u8>,
    pub exposed_functions: String,
    pub friends: String,
    pub structs: String,
}


#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct EventSize {
    pub txn_version: i64,
    pub event_index: i64,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
}

impl NamedTable for EventSize {
    const TABLE_NAME: &'static str = "event_size";
}

impl HasVersion for EventSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl EventSize {
    pub fn from_event_size_info(info: &EventSizeInfo, txn_version: i64, event_index: i64) -> Self {
        EventSize {
            txn_version,
            event_index,
            type_tag_bytes: info.type_tag_bytes as i64,
            total_bytes: info.total_bytes as i64,
        }
    }
}


#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct WriteSetSize {
    pub txn_version: i64,
    pub change_index: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub total_bytes: i64,
}

impl NamedTable for WriteSetSize {
    const TABLE_NAME: &'static str = "write_set_size";
}

impl HasVersion for WriteSetSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl WriteSetSize {
    pub fn from_transaction_info(info: &WriteOpSizeInfo, txn_version: i64, change_index: i64) -> Self {
        WriteSetSize {
            txn_version,
            change_index,
            key_bytes: info.key_bytes as i64,
            value_bytes: info.value_bytes as i64,
            total_bytes: info.key_bytes as i64 + info.value_bytes as i64,
        }
    }
}
