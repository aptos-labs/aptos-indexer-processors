// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use allocative_derive::Allocative;
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// PK of current_token_ownerships_v2, i.e. token_data_id, property_version_v1, owner_address, storage_id
pub type CurrentTokenOwnershipV2PK = (String, BigDecimal, String, String);

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct TokenOwnershipV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub property_version_v1: u64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub amount: String, // this is a string representation of a bigdecimal
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<String>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}
