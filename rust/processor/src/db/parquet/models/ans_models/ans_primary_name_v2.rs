// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::ans_models::raw_ans_primary_name_v2::{
        AnsPrimaryNameV2Convertible, CurrentAnsPrimaryNameV2Convertible, RawAnsPrimaryNameV2,
        RawCurrentAnsPrimaryNameV2,
    },
};
use allocative_derive::Allocative;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct AnsPrimaryNameV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for AnsPrimaryNameV2 {
    const TABLE_NAME: &'static str = "ans_primary_name_v2";
}

impl HasVersion for AnsPrimaryNameV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for AnsPrimaryNameV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl AnsPrimaryNameV2Convertible for AnsPrimaryNameV2 {
    fn from_raw(raw_item: RawAnsPrimaryNameV2) -> Self {
        AnsPrimaryNameV2 {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            block_timestamp: raw_item.transaction_timestamp,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

impl NamedTable for CurrentAnsPrimaryNameV2 {
    const TABLE_NAME: &'static str = "current_ans_primary_name_v2";
}

impl HasVersion for CurrentAnsPrimaryNameV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentAnsPrimaryNameV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

impl CurrentAnsPrimaryNameV2Convertible for CurrentAnsPrimaryNameV2 {
    fn from_raw(raw_item: RawCurrentAnsPrimaryNameV2) -> Self {
        CurrentAnsPrimaryNameV2 {
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            last_transaction_version: raw_item.last_transaction_version,
        }
    }
}
