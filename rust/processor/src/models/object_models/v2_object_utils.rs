// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    models::{
        default_models::move_resources::MoveResource,
        fungible_asset_models::v2_fungible_asset_utils::{
            FungibleAssetMetadata, FungibleAssetStore, FungibleAssetSupply,
        },
        token_v2_models::v2_token_utils::{
            AptosCollection, FixedSupply, PropertyMapModel, TokenV2, TransferEvent,
            UnlimitedSupply, V2TokenResource,
        },
    },
    utils::util::{deserialize_from_string, standardize_address},
};
use aptos_protos::transaction::v1::WriteResource;
use bigdecimal::BigDecimal;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// PK of current_objects, i.e. object_address
pub type CurrentObjectPK = String;

/// Tracks all object related metadata in a hashmap for quick access (keyed on address of the object)
pub type ObjectAggregatedDataMapping = HashMap<CurrentObjectPK, ObjectAggregatedData>;

/// Index of the event so that we can write its inverse to the db as primary key (to avoid collisiona)
pub type EventIndex = i64;

/// This contains metadata for the object. This only includes fungible asset and token v2 metadata for now.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectAggregatedData {
    pub object: ObjectWithMetadata,
    pub transfer_event: Option<(EventIndex, TransferEvent)>,
    // Fungible asset structs
    pub fungible_asset_metadata: Option<FungibleAssetMetadata>,
    pub fungible_asset_supply: Option<FungibleAssetSupply>,
    pub fungible_asset_store: Option<FungibleAssetStore>,
    // Token v2 structs
    pub aptos_collection: Option<AptosCollection>,
    pub fixed_supply: Option<FixedSupply>,
    pub property_map: Option<PropertyMapModel>,
    pub token: Option<TokenV2>,
    pub unlimited_supply: Option<UnlimitedSupply>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectCore {
    pub allow_ungated_transfer: bool,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub guid_creation_num: BigDecimal,
    owner: String,
}

impl ObjectCore {
    pub fn get_owner_address(&self) -> String {
        standardize_address(&self.owner)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectWithMetadata {
    pub object_core: ObjectCore,
    pub state_key_hash: String,
}

impl ObjectWithMetadata {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !V2TokenResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        if let V2TokenResource::ObjectCore(inner) = V2TokenResource::from_resource(
            &type_str,
            &serde_json::from_str(write_resource.data.as_str()).unwrap(),
            txn_version,
        )? {
            Ok(Some(Self {
                object_core: inner,
                state_key_hash: standardize_address(
                    hex::encode(write_resource.state_key_hash.as_slice()).as_str(),
                ),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_state_key_hash(&self) -> String {
        standardize_address(&self.state_key_hash)
    }
}
