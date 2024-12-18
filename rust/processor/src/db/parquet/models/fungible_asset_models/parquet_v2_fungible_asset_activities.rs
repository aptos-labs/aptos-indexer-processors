// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::{
        common::models::fungible_asset_models::raw_v2_fungible_asset_activities::{
            FungibleAssetActivityConvertible, RawFungibleAssetActivity,
        },
        postgres::models::coin_models::coin_utils::EventGuidResource,
    },
    utils::util::bigdecimal_to_u64,
};
use ahash::AHashMap;
use allocative::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
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

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct FungibleAssetActivity {
    pub txn_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<String>, // it is a string representation of the u128
    pub event_type: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub storage_refund_octa: u64,
}

impl NamedTable for FungibleAssetActivity {
    const TABLE_NAME: &'static str = "fungible_asset_activities";
}

impl HasVersion for FungibleAssetActivity {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for FungibleAssetActivity {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl FungibleAssetActivityConvertible for FungibleAssetActivity {
    fn from_raw(raw_item: RawFungibleAssetActivity) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            asset_type: raw_item.asset_type,
            is_frozen: raw_item.is_frozen,
            amount: raw_item.amount.map(|v| v.to_string()),
            event_type: raw_item.event_type,
            is_gas_fee: raw_item.is_gas_fee,
            gas_fee_payer_address: raw_item.gas_fee_payer_address,
            is_transaction_success: raw_item.is_transaction_success,
            entry_function_id_str: raw_item.entry_function_id_str,
            block_height: raw_item.block_height,
            token_standard: raw_item.token_standard,
            block_timestamp: raw_item.transaction_timestamp,
            storage_refund_octa: bigdecimal_to_u64(&raw_item.storage_refund_amount),
        }
    }
}
