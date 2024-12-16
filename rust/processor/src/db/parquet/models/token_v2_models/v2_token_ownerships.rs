// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::{
        common::models::token_v2_models::raw_v2_token_ownerships::{
            CurrentTokenOwnershipV2Convertible, RawCurrentTokenOwnershipV2, RawTokenOwnershipV2,
            TokenOwnershipV2Convertible,
        },
        parquet::models::DEFAULT_NONE,
    },
};
use allocative_derive::Allocative;
use bigdecimal::ToPrimitive;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::error;

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

impl NamedTable for TokenOwnershipV2 {
    const TABLE_NAME: &'static str = "token_ownerships_v2";
}

impl HasVersion for TokenOwnershipV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TokenOwnershipV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl TokenOwnershipV2Convertible for TokenOwnershipV2 {
    fn from_raw(raw_item: RawTokenOwnershipV2) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1.to_u64().unwrap(),
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            amount: raw_item.amount.to_string(),
            table_type_v1: raw_item.table_type_v1,
            token_properties_mutated_v1: raw_item
                .token_properties_mutated_v1
                .map(|v| v.to_string()),
            is_soulbound_v2: raw_item.is_soulbound_v2,
            token_standard: raw_item.token_standard,
            block_timestamp: raw_item.transaction_timestamp,
            non_transferrable_by_owner: raw_item.non_transferrable_by_owner,
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentTokenOwnershipV2 {
    pub token_data_id: String,
    pub property_version_v1: u64, // BigDecimal,
    pub owner_address: String,
    pub storage_id: String,
    pub amount: String, // BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<String>, // Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

impl NamedTable for CurrentTokenOwnershipV2 {
    const TABLE_NAME: &'static str = "current_token_ownerships_v2";
}

impl HasVersion for CurrentTokenOwnershipV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTokenOwnershipV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

// Facilitate tracking when a token is burned
impl CurrentTokenOwnershipV2Convertible for CurrentTokenOwnershipV2 {
    fn from_raw(raw_item: RawCurrentTokenOwnershipV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1.to_u64().unwrap(),
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            amount: raw_item.amount.to_string(),
            table_type_v1: raw_item.table_type_v1,
            token_properties_mutated_v1: raw_item
                .token_properties_mutated_v1
                .and_then(|v| {
                    canonical_json::to_string(&v)
                        .map_err(|e| {
                            error!("Failed to convert token_properties_mutated_v1: {:?}", e);
                            e
                        })
                        .ok()
                })
                .or_else(|| Some(DEFAULT_NONE.to_string())),
            is_soulbound_v2: raw_item.is_soulbound_v2,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            non_transferrable_by_owner: raw_item.non_transferrable_by_owner,
        }
    }
}
