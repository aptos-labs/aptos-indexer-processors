// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{
    ans_lookup_v2, ans_primary_name_v2, current_ans_lookup_v2, current_ans_primary_name_v2,
};
use serde::{Deserialize, Serialize};

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
    Queryable,
)]
#[diesel(primary_key(domain, subdomain, token_standard))]
#[diesel(table_name = current_ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub token_name: Option<String>,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
    pub subdomain_expiration_policy: Option<i64>,
}

#[derive(
    Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
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
    pub expiration_timestamp: Option<chrono::NaiveDateTime>,
    pub token_name: String,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
    pub subdomain_expiration_policy: Option<i64>,
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
    Queryable,
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
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(
    Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct AnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_standard: String,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
}
