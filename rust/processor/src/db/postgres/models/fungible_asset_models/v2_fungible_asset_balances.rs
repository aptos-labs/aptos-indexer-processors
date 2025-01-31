// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::fungible_asset_models::raw_v2_fungible_asset_balances::{
        CurrentUnifiedFungibleAssetBalanceConvertible, FungibleAssetBalanceConvertible,
        RawCurrentUnifiedFungibleAssetBalance, RawFungibleAssetBalance,
    },
    schema::{
        current_fungible_asset_balances, current_fungible_asset_balances_legacy,
        fungible_asset_balances,
    },
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = fungible_asset_balances)]
pub struct FungibleAssetBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl FungibleAssetBalanceConvertible for FungibleAssetBalance {
    fn from_raw(raw_item: RawFungibleAssetBalance) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            storage_id: raw_item.storage_id,
            owner_address: raw_item.owner_address,
            asset_type: raw_item.asset_type,
            is_primary: raw_item.is_primary,
            is_frozen: raw_item.is_frozen,
            amount: raw_item.amount,
            transaction_timestamp: raw_item.transaction_timestamp,
            token_standard: raw_item.token_standard,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances_legacy)]
pub struct CurrentFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Default)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances)]
pub struct CurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
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
            amount_v1: raw_item.amount_v1,
            amount_v2: raw_item.amount_v2,
            last_transaction_version_v1: raw_item.last_transaction_version_v1,
            last_transaction_version_v2: raw_item.last_transaction_version_v2,
            last_transaction_timestamp_v1: raw_item.last_transaction_timestamp_v1,
            last_transaction_timestamp_v2: raw_item.last_transaction_timestamp_v2,
        }
    }
}
