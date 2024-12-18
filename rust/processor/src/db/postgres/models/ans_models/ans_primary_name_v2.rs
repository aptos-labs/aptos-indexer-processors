// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::ans_models::raw_ans_primary_name_v2::{
        AnsPrimaryNameV2Convertible, CurrentAnsPrimaryNameV2Convertible, RawAnsPrimaryNameV2,
        RawCurrentAnsPrimaryNameV2,
    },
    schema::{ans_primary_name_v2, current_ans_primary_name_v2},
};
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct AnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
}

impl AnsPrimaryNameV2Convertible for AnsPrimaryNameV2 {
    fn from_raw(raw_item: RawAnsPrimaryNameV2) -> Self {
        AnsPrimaryNameV2 {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            registered_address: raw_item.registered_address,
            token_standard: raw_item.token_standard,
            domain: raw_item.domain,
            subdomain: raw_item.subdomain,
            token_name: raw_item.token_name,
            is_deleted: raw_item.is_deleted,
        }
    }
}

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
)]
#[diesel(primary_key(registered_address, token_standard))]
#[diesel(table_name = current_ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
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
