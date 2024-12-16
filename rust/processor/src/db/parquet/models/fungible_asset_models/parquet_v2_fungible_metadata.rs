// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::fungible_asset_models::raw_v2_fungible_metadata::{
        FungibleAssetMetadataConvertible, RawFungibleAssetMetadataModel,
    },
};
use allocative_derive::Allocative;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct FungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<String>, // it is a string representation of the u128
    pub maximum_v2: Option<String>, // it is a string representation of the u128
}

impl NamedTable for FungibleAssetMetadataModel {
    const TABLE_NAME: &'static str = "fungible_asset_metadata";
}

impl HasVersion for FungibleAssetMetadataModel {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for FungibleAssetMetadataModel {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

impl FungibleAssetMetadataConvertible for FungibleAssetMetadataModel {
    fn from_raw(raw_item: RawFungibleAssetMetadataModel) -> Self {
        Self {
            asset_type: raw_item.asset_type,
            creator_address: raw_item.creator_address,
            name: raw_item.name,
            symbol: raw_item.symbol,
            decimals: raw_item.decimals,
            icon_uri: raw_item.icon_uri,
            project_uri: raw_item.project_uri,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            supply_aggregator_table_handle_v1: raw_item.supply_aggregator_table_handle_v1,
            supply_aggregator_table_key_v1: raw_item.supply_aggregator_table_key_v1,
            token_standard: raw_item.token_standard,
            is_token_v2: raw_item.is_token_v2,
            supply_v2: raw_item.supply_v2.map(|x| x.to_string()),
            maximum_v2: raw_item.maximum_v2.map(|x| x.to_string()),
        }
    }
}
