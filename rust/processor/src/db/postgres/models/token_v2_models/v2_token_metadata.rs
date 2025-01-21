// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_v2_models::raw_v2_token_metadata::{
        CurrentTokenV2MetadataConvertible, RawCurrentTokenV2Metadata,
    },
    schema::current_token_v2_metadata,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// PK of current_objects, i.e. object_address, resource_type
pub type CurrentTokenV2MetadataPK = (String, String);

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(object_address, resource_type))]
#[diesel(table_name = current_token_v2_metadata)]
pub struct CurrentTokenV2Metadata {
    pub object_address: String,
    pub resource_type: String,
    pub data: Value,
    pub state_key_hash: String,
    pub last_transaction_version: i64,
}

impl CurrentTokenV2MetadataConvertible for CurrentTokenV2Metadata {
    fn from_raw(raw_item: RawCurrentTokenV2Metadata) -> Self {
        Self {
            object_address: raw_item.object_address,
            resource_type: raw_item.resource_type,
            data: raw_item.data,
            state_key_hash: raw_item.state_key_hash,
            last_transaction_version: raw_item.last_transaction_version,
        }
    }
}
