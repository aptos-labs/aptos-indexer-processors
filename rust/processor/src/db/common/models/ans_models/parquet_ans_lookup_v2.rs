// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type Domain = String;
type Subdomain = String;
type TokenStandardType = String;
type RegisteredAddress = String;
// PK of current_ans_lookup_v2
type CurrentAnsLookupV2PK = (Domain, Subdomain, TokenStandardType);
// PK of current_ans_primary_name
type CurrentAnsPrimaryNameV2PK = (RegisteredAddress, TokenStandardType);

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Serialize)]
pub struct AnsPrimaryNameV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}
