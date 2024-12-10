// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    raw_ans_primary_name_v2::RawAnsPrimaryNameV2, raw_current_ans_lookup_v2::TokenStandardType,
};
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
pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

// impl Ord for RawCurrentAnsPrimaryNameV2 {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.registered_address.cmp(&other.registered_address)
//     }
// }

// impl PartialOrd for CurrentAnsPrimaryNameV2 {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

impl CurrentAnsPrimaryNameV2Convertible for CurrentAnsPrimaryNameV2 {
    fn from_raw(raw_item: &RawCurrentAnsPrimaryNameV2) -> Self {
        CurrentAnsPrimaryNameV2 {
            registered_address: raw_item.registered_address.clone(),
            token_standard: raw_item.token_standard.clone(),
            domain: raw_item.domain.clone(),
            subdomain: raw_item.subdomain.clone(),
            token_name: raw_item.token_name.clone(),
            is_deleted: raw_item.is_deleted,
            last_transaction_version: raw_item.last_transaction_version,
        }
    }
}
