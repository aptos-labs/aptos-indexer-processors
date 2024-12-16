// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_v2_models::raw_v2_token_ownerships::{
        CurrentTokenOwnershipV2Convertible, RawCurrentTokenOwnershipV2, RawTokenOwnershipV2,
        TokenOwnershipV2Convertible,
    },
    schema::{current_token_ownerships_v2, token_ownerships_v2},
};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = token_ownerships_v2)]
pub struct TokenOwnershipV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub amount: BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

impl TokenOwnershipV2Convertible for TokenOwnershipV2 {
    fn from_raw(raw_item: RawTokenOwnershipV2) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1,
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            amount: raw_item.amount,
            table_type_v1: raw_item.table_type_v1,
            token_properties_mutated_v1: raw_item.token_properties_mutated_v1,
            is_soulbound_v2: raw_item.is_soulbound_v2,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            transaction_timestamp: raw_item.transaction_timestamp,
            non_transferrable_by_owner: raw_item.non_transferrable_by_owner,
        }
    }
}
#[derive(
    Clone, Debug, Deserialize, Eq, FieldCount, Identifiable, Insertable, PartialEq, Serialize,
)]
#[diesel(primary_key(token_data_id, property_version_v1, owner_address, storage_id))]
#[diesel(table_name = current_token_ownerships_v2)]
pub struct CurrentTokenOwnershipV2 {
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub owner_address: String,
    pub storage_id: String,
    pub amount: BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

impl Ord for CurrentTokenOwnershipV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id
            .cmp(&other.token_data_id)
            .then(self.property_version_v1.cmp(&other.property_version_v1))
            .then(self.owner_address.cmp(&other.owner_address))
            .then(self.storage_id.cmp(&other.storage_id))
    }
}

impl PartialOrd for CurrentTokenOwnershipV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentTokenOwnershipV2Convertible for CurrentTokenOwnershipV2 {
    fn from_raw(raw_item: RawCurrentTokenOwnershipV2) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1,
            owner_address: raw_item.owner_address,
            storage_id: raw_item.storage_id,
            amount: raw_item.amount,
            table_type_v1: raw_item.table_type_v1,
            token_properties_mutated_v1: raw_item.token_properties_mutated_v1,
            is_soulbound_v2: raw_item.is_soulbound_v2,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            non_transferrable_by_owner: raw_item.non_transferrable_by_owner,
        }
    }
}
