// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::fungible_asset_models::raw_v2_fungible_asset_balances::{
        CurrentUnifiedFungibleAssetBalanceConvertible, FungibleAssetBalanceConvertible,
        RawCurrentUnifiedFungibleAssetBalance, RawFungibleAssetBalance,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use lazy_static::lazy_static;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

lazy_static! {
    pub static ref DEFAULT_AMOUNT_VALUE: String = "0".to_string();
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct FungibleAssetBalance {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: String, // it is a string representation of the u128
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl NamedTable for FungibleAssetBalance {
    const TABLE_NAME: &'static str = "fungible_asset_balances";
}

impl HasVersion for FungibleAssetBalance {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for FungibleAssetBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl FungibleAssetBalanceConvertible for FungibleAssetBalance {
    fn from_raw(raw_item: RawFungibleAssetBalance) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            storage_id: raw_item.storage_id,
            owner_address: raw_item.owner_address,
            asset_type: raw_item.asset_type,
            is_primary: raw_item.is_primary,
            is_frozen: raw_item.is_frozen,
            amount: raw_item.amount.to_string(),
            block_timestamp: raw_item.transaction_timestamp,
            token_standard: raw_item.token_standard,
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: String, // it is a string representation of the u128
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl NamedTable for CurrentFungibleAssetBalance {
    const TABLE_NAME: &'static str = "current_fungible_asset_balances_legacy";
}

impl HasVersion for CurrentFungibleAssetBalance {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentFungibleAssetBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<String>, // it is a string representation of the u128
    pub amount_v2: Option<String>, // it is a string representation of the u128
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    #[allocative(skip)]
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    #[allocative(skip)]
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

impl NamedTable for CurrentUnifiedFungibleAssetBalance {
    const TABLE_NAME: &'static str = "current_fungible_asset_balances";
}

/// This will be deprecated.
impl HasVersion for CurrentUnifiedFungibleAssetBalance {
    fn version(&self) -> i64 {
        -1
    }
}

/// This will be deprecated.
impl GetTimeStamp for CurrentUnifiedFungibleAssetBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[allow(deprecated)]
        chrono::NaiveDateTime::from_timestamp(0, 0)
    }
}

impl CurrentUnifiedFungibleAssetBalanceConvertible for CurrentUnifiedFungibleAssetBalance {
    fn from_raw(raw_item: RawCurrentUnifiedFungibleAssetBalance) -> Self {
        Self {
            storage_id: raw_item.storage_id,
            owner_address: raw_item.owner_address,
            asset_type_v1: raw_item.asset_type_v1,
            asset_type_v2: raw_item.asset_type_v2,
            is_primary: raw_item.is_primary,
            is_frozen: raw_item.is_frozen,
            amount_v1: raw_item.amount_v1.map(|x| x.to_string()),
            amount_v2: raw_item.amount_v2.map(|x| x.to_string()),
            last_transaction_version_v1: raw_item.last_transaction_version_v1,
            last_transaction_version_v2: raw_item.last_transaction_version_v2,
            last_transaction_timestamp_v1: raw_item.last_transaction_timestamp_v1,
            last_transaction_timestamp_v2: raw_item.last_transaction_timestamp_v2,
        }
    }
}
