// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet::data_type::Decimal;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// PK of current_collections_v2, i.e. collection_id
pub type CurrentCollectionV2PK = String;

#[derive(Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize)]
pub struct CollectionV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: Vec<u8>,
    pub max_supply: Option<Vec<u8>>,
    pub total_minted_v2: Option<Vec<u8>>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}