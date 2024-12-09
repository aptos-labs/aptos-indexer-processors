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

// #[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
// #[diesel(primary_key(transaction_version, write_set_change_index))]
// #[diesel(table_name = ans_primary_name_v2)]
// #[diesel(treat_none_as_null = true)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawAnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
}

pub trait AnsPrimaryNameV2Convertible {
    fn from_raw(raw_item: &RawAnsPrimaryNameV2) -> Self;
}
