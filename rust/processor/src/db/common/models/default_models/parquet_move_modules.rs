// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::bq_analytics::generic_parquet_processor::{HasVersion, NamedTable};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{EventSizeInfo, WriteOpSizeInfo};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
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
