// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::postgres::models::{
        ans_models::{
            ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
            ans_utils::{get_token_name, NameRecordV2, SetReverseLookupEvent, SubdomainExtV2},
        },
        token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::{
        ans_lookup_v2, ans_primary_name_v2, current_ans_lookup_v2, current_ans_primary_name_v2,
    },
    utils::util::standardize_address,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{Event, WriteResource};
use diesel::prelude::*;
use field_count::FieldCount;
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
impl AnsPrimaryNameV2Convertible for AnsPrimaryNameV2 {
    fn from_raw(raw_item: &RawAnsPrimaryNameV2) -> Self {
        AnsPrimaryNameV2 {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            registered_address: raw_item.registered_address.clone(),
            token_standard: raw_item.token_standard.clone(),
            domain: raw_item.domain.clone(),
            subdomain: raw_item.subdomain.clone(),
            token_name: raw_item.token_name.clone(),
            is_deleted: raw_item.is_deleted,
        }
    }
}
