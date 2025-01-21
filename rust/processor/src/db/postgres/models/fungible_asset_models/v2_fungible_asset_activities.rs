// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::{
        common::models::fungible_asset_models::raw_v2_fungible_asset_activities::{
            FungibleAssetActivityConvertible, RawFungibleAssetActivity,
        },
        postgres::models::coin_models::coin_utils::EventGuidResource,
    },
    schema::fungible_asset_activities,
};
use ahash::AHashMap;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub const GAS_FEE_EVENT: &str = "0x1::aptos_coin::GasFeeEvent";
// We will never have a negative number on chain so this will avoid collision in postgres
pub const BURN_GAS_EVENT_CREATION_NUM: i64 = -1;
pub const BURN_GAS_EVENT_INDEX: i64 = -1;

pub type OwnerAddress = String;
pub type CoinType = String;
// Primary key of the current_coin_balances table, i.e. (owner_address, coin_type)
pub type CurrentCoinBalancePK = (OwnerAddress, CoinType);
pub type EventToCoinType = AHashMap<EventGuidResource, CoinType>;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = fungible_asset_activities)]
pub struct FungibleAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<BigDecimal>,
    pub type_: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
}

impl FungibleAssetActivityConvertible for FungibleAssetActivity {
    fn from_raw(raw_item: RawFungibleAssetActivity) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            asset_type: raw_item.asset_type,
            is_frozen: raw_item.is_frozen,
            amount: raw_item.amount,
            type_: raw_item.event_type,
            is_gas_fee: raw_item.is_gas_fee,
            gas_fee_payer_address: raw_item.gas_fee_payer_address,
            is_transaction_success: raw_item.is_transaction_success,
            entry_function_id_str: raw_item.entry_function_id_str,
            block_height: raw_item.block_height,
            token_standard: raw_item.token_standard,
            transaction_timestamp: raw_item.transaction_timestamp,
            storage_refund_amount: raw_item.storage_refund_amount,
        }
    }
}
