// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::postgres::models::{
        default_models::move_resources::MoveResource,
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        resources::{COIN_ADDR, TOKEN_ADDR, TOKEN_V2_ADDR},
        token_models::token_utils::NAME_LENGTH,
    },
    utils::util::{standardize_address, truncate_str},
};
use anyhow::Context;
use aptos_protos::transaction::v1::WriteResource;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// PK of current_objects, i.e. object_address, resource_type
pub type CurrentTokenV2MetadataPK = (String, String);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RawCurrentTokenV2Metadata {
    pub object_address: String,
    pub resource_type: String,
    pub data: Value,
    pub state_key_hash: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl Ord for RawCurrentTokenV2Metadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.object_address
            .cmp(&other.object_address)
            .then(self.resource_type.cmp(&other.resource_type))
    }
}
impl PartialOrd for RawCurrentTokenV2Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RawCurrentTokenV2Metadata {
    /// Parsing unknown resources with 0x4::token::Token
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        object_metadatas: &ObjectAggregatedDataMapping,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        let object_address = standardize_address(&write_resource.address.to_string());
        if let Some(object_data) = object_metadatas.get(&object_address) {
            // checking if token_v2
            if object_data.token.is_some() {
                let move_tag =
                    MoveResource::convert_move_struct_tag(write_resource.r#type.as_ref().unwrap());
                let resource_type_addr = move_tag.get_address();
                if matches!(
                    resource_type_addr.as_str(),
                    COIN_ADDR | TOKEN_ADDR | TOKEN_V2_ADDR
                ) {
                    return Ok(None);
                }

                let resource = MoveResource::from_write_resource(write_resource, 0, txn_version, 0);

                let state_key_hash = object_data.object.get_state_key_hash();
                if state_key_hash != resource.state_key_hash {
                    return Ok(None);
                }

                let resource_type = truncate_str(&resource.type_, NAME_LENGTH);
                return Ok(Some(RawCurrentTokenV2Metadata {
                    object_address,
                    resource_type,
                    data: resource
                        .data
                        .context("data must be present in write resource")?,
                    state_key_hash: resource.state_key_hash,
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                }));
            }
        }
        Ok(None)
    }
}

pub trait CurrentTokenV2MetadataConvertible {
    fn from_raw(raw_item: RawCurrentTokenV2Metadata) -> Self;
}
