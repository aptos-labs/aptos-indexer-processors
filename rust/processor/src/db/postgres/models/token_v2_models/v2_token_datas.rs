// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_v2_models::raw_v2_token_datas::{
        CurrentTokenDataV2Convertible, RawCurrentTokenDataV2, RawTokenDataV2,
        TokenDataV2Convertible,
    },
    schema::{current_token_datas_v2, token_datas_v2},
};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// PK of current_token_datas_v2, i.e. token_data_id
pub type CurrentTokenDataV2PK = String;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = token_datas_v2)]
pub struct TokenDataV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    // Here for consistency but we don't need to actually fill it
    // pub is_deleted_v2: Option<bool>,
}

impl TokenDataV2Convertible for TokenDataV2 {
    fn from_raw(raw_item: RawTokenDataV2) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            maximum: raw_item.maximum,
            supply: raw_item.supply,
            largest_property_version_v1: raw_item.largest_property_version_v1,
            token_uri: raw_item.token_uri,
            token_properties: raw_item.token_properties,
            description: raw_item.description,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            transaction_timestamp: raw_item.transaction_timestamp,
            decimals: raw_item.decimals,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id))]
#[diesel(table_name = current_token_datas_v2)]
pub struct CurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
}

impl CurrentTokenDataV2Convertible for CurrentTokenDataV2 {
    fn from_raw(raw_item: RawCurrentTokenDataV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
            token_name: raw_item.token_name,
            maximum: raw_item.maximum,
            supply: raw_item.supply,
            largest_property_version_v1: raw_item.largest_property_version_v1,
            token_uri: raw_item.token_uri,
            token_properties: raw_item.token_properties,
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
