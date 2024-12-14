// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::token_v2_models::raw_v2_token_activities::{
        RawTokenActivityV2, TokenActivityV2Convertible,
    },
};
use allocative_derive::Allocative;
use bigdecimal::ToPrimitive;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct TokenActivityV2 {
    pub txn_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: u64, // BigDecimal
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: String, // BigDecimal
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for TokenActivityV2 {
    const TABLE_NAME: &'static str = "token_activities_v2";
}

impl HasVersion for TokenActivityV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TokenActivityV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl TokenActivityV2Convertible for TokenActivityV2 {
    // TODO: consider returning a Result
    fn from_raw(raw_item: RawTokenActivityV2) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            event_account_address: raw_item.event_account_address,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1.to_u64().unwrap(),
            type_: raw_item.type_,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            token_amount: raw_item.token_amount.to_string(),
            before_value: raw_item.before_value,
            after_value: raw_item.after_value,
            entry_function_id_str: raw_item.entry_function_id_str,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            block_timestamp: raw_item.transaction_timestamp,
        }
    }
}
