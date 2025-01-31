// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use bigdecimal::BigDecimal;
use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{
    coin_supply, current_fungible_asset_balances, fungible_asset_activities,
    fungible_asset_balances, fungible_asset_metadata, fungible_asset_to_coin_mappings,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
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
    pub inserted_at: chrono::NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
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
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances)]
pub struct CurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type_v2: Option<String>,
    pub asset_type_v1: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub amount: BigDecimal,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_version: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp: Option<chrono::NaiveDateTime>,
    pub inserted_at: chrono::NaiveDateTime,
    pub asset_type: String,
    pub token_standard: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(asset_type))]
#[diesel(table_name = fungible_asset_metadata)]
pub struct FungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
    pub inserted_at: chrono::NaiveDateTime,
    pub is_token_v2: Option<bool>,
    pub supply_v2: Option<BigDecimal>,
    pub maximum_v2: Option<BigDecimal>,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, coin_type_hash))]
#[diesel(table_name = coin_supply)]
pub struct CoinSupply {
    pub transaction_version: i64,
    pub coin_type_hash: String,
    pub coin_type: String,
    pub supply: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub transaction_epoch: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(coin_type))]
#[diesel(table_name = fungible_asset_to_coin_mappings)]
pub struct FungibleAssetToCoinMapping {
    pub coin_type: String,
    pub fungible_asset_metadata_address: String,
    pub last_transaction_version: i64,
}
