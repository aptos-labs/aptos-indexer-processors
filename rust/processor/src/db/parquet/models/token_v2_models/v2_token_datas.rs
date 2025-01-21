// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::{
        common::models::token_v2_models::raw_v2_token_datas::{
            CurrentTokenDataV2Convertible, RawCurrentTokenDataV2, RawTokenDataV2,
            TokenDataV2Convertible,
        },
        parquet::models::DEFAULT_NONE,
    },
};
use allocative_derive::Allocative;
use anyhow::Context;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct TokenDataV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub largest_property_version_v1: Option<String>, // String format of BigDecimal
    pub token_uri: String,
    pub token_properties: String,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub is_deleted_v2: Option<bool>,
}

impl NamedTable for TokenDataV2 {
    const TABLE_NAME: &'static str = "token_datas_v2";
}

impl HasVersion for TokenDataV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TokenDataV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl TokenDataV2Convertible for TokenDataV2 {
    fn from_raw(raw_item: RawTokenDataV2) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            largest_property_version_v1: raw_item
                .largest_property_version_v1
                .map(|v| v.to_string()),
            token_uri: raw_item.token_uri,
            token_properties: canonical_json::to_string(&raw_item.token_properties.clone())
                .context("Failed to serialize token properties")
                .unwrap(),
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            block_timestamp: raw_item.transaction_timestamp,
            is_deleted_v2: raw_item.is_deleted_v2,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct CurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<String>,                     // BigDecimal
    pub supply: Option<String>,                      // BigDecimal
    pub largest_property_version_v1: Option<String>, // String format of BigDecimal
    pub token_uri: String,
    pub token_properties: String, // serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
}

impl NamedTable for CurrentTokenDataV2 {
    const TABLE_NAME: &'static str = "current_token_datas_v2";
}

impl HasVersion for CurrentTokenDataV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTokenDataV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

impl CurrentTokenDataV2Convertible for CurrentTokenDataV2 {
    fn from_raw(raw_item: RawCurrentTokenDataV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            maximum: raw_item.maximum.map(|v| v.to_string()),
            supply: raw_item.supply.map(|v| v.to_string()),
            largest_property_version_v1: raw_item
                .largest_property_version_v1
                .map(|v| v.to_string()),
            token_uri: raw_item.token_uri,
            token_properties: canonical_json::to_string(&raw_item.token_properties).unwrap_or_else(
                |_| {
                    error!(
                        "Failed to serialize token_properties to JSON: {:?}",
                        raw_item.token_properties
                    );
                    DEFAULT_NONE.to_string()
                },
            ),
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            decimals: raw_item.decimals,
            is_deleted_v2: raw_item.is_deleted_v2,
        }
    }
}
