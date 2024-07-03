// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// PK of current_token_datas_v2, i.e. token_data_id
pub type CurrentTokenDataV2PK = String;

#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct TokenDataV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub largest_property_version_v1: Option<u64>,
    pub token_uri: String,
    pub token_properties: String,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub block_timestamp: chrono::NaiveDateTime,
    pub is_deleted_v2: Option<bool>,
}
