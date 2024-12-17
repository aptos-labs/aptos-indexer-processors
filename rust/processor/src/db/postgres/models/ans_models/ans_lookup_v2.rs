// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::ans_models::raw_ans_lookup_v2::{
        AnsLookupV2Convertible, CurrentAnsLookupV2Convertible, RawAnsLookupV2,
        RawCurrentAnsLookupV2,
    },
    schema::{ans_lookup_v2, current_ans_lookup_v2},
};
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct AnsLookupV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
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
#[diesel(primary_key(domain, subdomain, token_standard))]
#[diesel(table_name = current_ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
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
