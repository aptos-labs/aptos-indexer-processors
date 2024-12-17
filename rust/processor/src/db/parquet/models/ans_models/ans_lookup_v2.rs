// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::ans_models::raw_ans_lookup_v2::{
        AnsLookupV2Convertible, CurrentAnsLookupV2Convertible, RawAnsLookupV2,
        RawCurrentAnsLookupV2,
    },
};
use allocative::Allocative;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct AnsLookupV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    #[allocative(skip)]
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl NamedTable for AnsLookupV2 {
    const TABLE_NAME: &'static str = "ans_lookup_v2";
}

impl HasVersion for AnsLookupV2 {
    fn version(&self) -> i64 {
        self.transaction_version
    }
}

impl GetTimeStamp for AnsLookupV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

impl AnsLookupV2Convertible for AnsLookupV2 {
    fn from_raw(raw_item: RawAnsLookupV2) -> Self {
        AnsLookupV2 {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
        }
    }
}

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
}

impl NamedTable for CurrentAnsLookupV2 {
    const TABLE_NAME: &'static str = "current_ans_lookup_v2";
}

impl HasVersion for CurrentAnsLookupV2 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentAnsLookupV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

impl CurrentAnsLookupV2Convertible for CurrentAnsLookupV2 {
    fn from_raw(raw_item: RawCurrentAnsLookupV2) -> Self {
        CurrentAnsLookupV2 {
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_standard: raw_item.token_standard,
            registered_address: raw_item.registered_address,
            last_transaction_version: raw_item.last_transaction_version,
            expiration_timestamp: raw_item.expiration_timestamp,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
            subdomain_expiration_policy: raw_item.subdomain_expiration_policy,
        }
    }
}
