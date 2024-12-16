// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_v2_models::raw_v2_token_activities::{
        RawTokenActivityV2, TokenActivityV2Convertible,
    },
    schema::token_activities_v2,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = token_activities_v2)]
pub struct TokenActivityV2 {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl TokenActivityV2Convertible for TokenActivityV2 {
    fn from_raw(raw_item: RawTokenActivityV2) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            event_account_address: raw_item.event_account_address,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1,
            type_: raw_item.type_,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            token_amount: raw_item.token_amount,
            before_value: raw_item.before_value,
            after_value: raw_item.after_value,
            entry_function_id_str: raw_item.entry_function_id_str,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            transaction_timestamp: raw_item.transaction_timestamp,
        }
    }
}
