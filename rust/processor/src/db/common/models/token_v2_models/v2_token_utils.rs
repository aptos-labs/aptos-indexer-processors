// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::{
        common::models::object_models::v2_object_utils::CurrentObjectPK,
        postgres::models::token_models::token_utils::{NAME_LENGTH, URI_LENGTH},
    },
    utils::util::{
        deserialize_from_string, deserialize_token_object_property_map_from_bcs_hexstring,
        standardize_address, truncate_str, Aggregator, AggregatorSnapshot, DerivedStringSnapshot,
    },
};
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{Event, WriteResource};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Formatter},
    str::FromStr,
};

pub const DEFAULT_OWNER_ADDRESS: &str = "unknown";

/// Tracks all token related data in a hashmap for quick access (keyed on address of the object core)
/// Maps address to burn event. If it's an old event previous_owner will be empty
pub type TokenV2Burned = AHashMap<CurrentObjectPK, Burn>;
pub type TokenV2Minted = AHashSet<CurrentObjectPK>;

/// Tracks which token standard a token / collection is built upon
#[derive(Serialize)]
pub enum TokenStandard {
    V1,
    V2,
}

impl fmt::Display for TokenStandard {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let res = match self {
            TokenStandard::V1 => "v1",
            TokenStandard::V2 => "v2",
        };
        write!(f, "{}", res)
    }
}

impl FromStr for TokenStandard {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "v1" => TokenStandard::V1,
            "v2" => TokenStandard::V2,
            _ => return Err(anyhow::anyhow!("Invalid token standard: {}", s)),
        })
    }
}

/* Section on Collection / Token */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Collection {
    creator: String,
    pub description: String,
    // These are set to private because we should never get name or uri directly
    name: String,
    uri: String,
}

impl Collection {
    pub fn get_creator_address(&self) -> String {
        standardize_address(&self.creator)
    }

    pub fn get_uri_trunc(&self) -> String {
        truncate_str(&self.uri, URI_LENGTH)
    }

    pub fn get_name_trunc(&self) -> String {
        truncate_str(&self.name, NAME_LENGTH)
    }
}

impl TryFrom<&WriteResource> for Collection {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AptosCollection {
    pub mutable_description: bool,
    pub mutable_uri: bool,
}

impl TryFrom<&WriteResource> for AptosCollection {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenV2 {
    collection: ResourceReference,
    pub description: String,
    // These are set to private because we should never get name or uri directly
    name: String,
    uri: String,
}

impl TryFrom<&WriteResource> for TokenV2 {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

impl TokenV2 {
    pub fn get_collection_address(&self) -> String {
        self.collection.get_reference_address()
    }

    pub fn get_uri_trunc(&self) -> String {
        truncate_str(&self.uri, URI_LENGTH)
    }

    pub fn get_name_trunc(&self) -> String {
        truncate_str(&self.name, NAME_LENGTH)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceReference {
    inner: String,
}

impl ResourceReference {
    pub fn get_reference_address(&self) -> String {
        standardize_address(&self.inner)
    }
}

/* Section on Supply */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FixedSupply {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub current_supply: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub max_supply: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub total_minted: BigDecimal,
}

impl TryFrom<&WriteResource> for FixedSupply {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnlimitedSupply {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub current_supply: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub total_minted: BigDecimal,
}

impl TryFrom<&WriteResource> for UnlimitedSupply {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConcurrentSupply {
    pub current_supply: Aggregator,
    pub total_minted: Aggregator,
}

impl TryFrom<&WriteResource> for ConcurrentSupply {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

/* Section on Events */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MintEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub index: BigDecimal,
    token: String,
}

impl MintEvent {
    pub fn from_event(event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(V2TokenEvent::MintEvent(inner)) =
            V2TokenEvent::from_event(event.type_str.as_str(), &event.data, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }

    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mint {
    collection: String,
    pub index: AggregatorSnapshot,
    token: String,
}

impl Mint {
    pub fn from_event(event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(V2TokenEvent::Mint(inner)) =
            V2TokenEvent::from_event(event.type_str.as_str(), &event.data, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }

    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenMutationEvent {
    pub mutated_field_name: String,
    pub old_value: String,
    pub new_value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenMutationEventV2 {
    pub token_address: String,
    pub mutated_field_name: String,
    pub old_value: String,
    pub new_value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BurnEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub index: BigDecimal,
    token: String,
}

impl BurnEvent {
    pub fn from_event(event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(V2TokenEvent::BurnEvent(inner)) =
            V2TokenEvent::from_event(event.type_str.as_str(), &event.data, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }

    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Burn {
    collection: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub index: BigDecimal,
    token: String,
    previous_owner: String,
}

impl Burn {
    pub fn new(
        collection: String,
        index: BigDecimal,
        token: String,
        previous_owner: String,
    ) -> Self {
        Burn {
            collection,
            index,
            token,
            previous_owner,
        }
    }

    pub fn from_event(event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(V2TokenEvent::Burn(inner)) =
            V2TokenEvent::from_event(event.type_str.as_str(), &event.data, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }

    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }

    pub fn get_previous_owner_address(&self) -> Option<String> {
        if self.previous_owner.is_empty() {
            None
        } else {
            Some(standardize_address(&self.previous_owner))
        }
    }

    pub fn get_collection_address(&self) -> String {
        standardize_address(&self.collection)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferEvent {
    from: String,
    to: String,
    object: String,
}

impl TransferEvent {
    pub fn from_event(event: &Event, txn_version: i64) -> anyhow::Result<Option<Self>> {
        if let Some(V2TokenEvent::TransferEvent(inner)) =
            V2TokenEvent::from_event(event.type_str.as_str(), &event.data, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }

    pub fn get_from_address(&self) -> String {
        standardize_address(&self.from)
    }

    pub fn get_to_address(&self) -> String {
        standardize_address(&self.to)
    }

    pub fn get_object_address(&self) -> String {
        standardize_address(&self.object)
    }
}

/* Section on Property Maps */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PropertyMapModel {
    #[serde(deserialize_with = "deserialize_token_object_property_map_from_bcs_hexstring")]
    pub inner: serde_json::Value,
}

impl TryFrom<&WriteResource> for PropertyMapModel {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenIdentifiers {
    name: DerivedStringSnapshot,
}

impl TryFrom<&WriteResource> for TokenIdentifiers {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

impl TokenIdentifiers {
    pub fn get_name_trunc(&self) -> String {
        truncate_str(&self.name.value, NAME_LENGTH)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum V2TokenEvent {
    Mint(Mint),
    MintEvent(MintEvent),
    TokenMutationEvent(TokenMutationEvent),
    TokenMutation(TokenMutationEventV2),
    Burn(Burn),
    BurnEvent(BurnEvent),
    TransferEvent(TransferEvent),
}

impl V2TokenEvent {
    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Result<Option<Self>> {
        match data_type {
            "0x4::collection::Mint" => {
                serde_json::from_str(data).map(|inner| Some(Self::Mint(inner)))
            },
            "0x4::collection::MintEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::MintEvent(inner)))
            },
            "0x4::token::MutationEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::TokenMutationEvent(inner)))
            },
            "0x4::token::Mutation" => {
                serde_json::from_str(data).map(|inner| Some(Self::TokenMutation(inner)))
            },
            "0x4::collection::Burn" => {
                serde_json::from_str(data).map(|inner| Some(Self::Burn(inner)))
            },
            "0x4::collection::BurnEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::BurnEvent(inner)))
            },
            "0x1::object::TransferEvent" | "0x1::object::Transfer" => {
                serde_json::from_str(data).map(|inner| Some(Self::TransferEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))
    }
}
