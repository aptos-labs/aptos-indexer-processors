// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::db::{
    common::models::{
        object_models::v2_object_utils::{ObjectCore, Untransferable},
        token_v2_models::v2_token_utils::{
            AptosCollection, Collection, ConcurrentSupply, FixedSupply, PropertyMapModel,
            TokenIdentifiers, TokenV2, UnlimitedSupply,
        },
    },
    postgres::models::{
        default_models::move_resources::MoveResource,
        fungible_asset_models::v2_fungible_asset_utils::{
            ConcurrentFungibleAssetBalance, ConcurrentFungibleAssetSupply, FungibleAssetMetadata,
            FungibleAssetStore, FungibleAssetSupply,
        },
    },
};
use anyhow::Result;
use aptos_protos::transaction::v1::WriteResource;
use const_format::formatcp;

pub const COIN_ADDR: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
pub const TOKEN_ADDR: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
pub const TOKEN_V2_ADDR: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000004";

pub const TYPE_FUNGIBLE_ASSET_SUPPLY: &str = formatcp!("{COIN_ADDR}::fungible_asset::Supply");
pub const TYPE_CONCURRENT_FUNGIBLE_ASSET_SUPPLY: &str =
    formatcp!("{COIN_ADDR}::fungible_asset::ConcurrentSupply");
pub const TYPE_FUNGIBLE_ASSET_METADATA: &str = formatcp!("{COIN_ADDR}::fungible_asset::Metadata");
pub const TYPE_FUNGIBLE_ASSET_STORE: &str = formatcp!("{COIN_ADDR}::fungible_asset::FungibleStore");
pub const TYPE_CONCURRENT_FUNGIBLE_ASSET_BALANCE: &str =
    formatcp!("{COIN_ADDR}::fungible_asset::ConcurrentFungibleBalance");

pub const TYPE_OBJECT_CORE: &str = formatcp!("{COIN_ADDR}::object::ObjectCore");
pub const TYPE_UNTRANSFERABLE: &str = formatcp!("{COIN_ADDR}::object::Untransferable");
pub const TYPE_COLLECTION: &str = formatcp!("{TOKEN_V2_ADDR}::collection::Collection");
pub const TYPE_CONCURRENT_SUPPLY: &str = formatcp!("{TOKEN_V2_ADDR}::collection::ConcurrentSupply");
pub const TYPE_FIXED_SUPPLY: &str = formatcp!("{TOKEN_V2_ADDR}::collection::FixedSupply");
pub const TYPE_UNLIMITED_SUPPLY: &str = formatcp!("{TOKEN_V2_ADDR}::collection::UnlimitedSupply");
pub const TYPE_APOTS_COLLECTION: &str = formatcp!("{TOKEN_V2_ADDR}::aptos_token::AptosCollection");
pub const TYPE_TOKEN_V2: &str = formatcp!("{TOKEN_V2_ADDR}::token::Token");
pub const TYPE_TOKEN_IDENTIFIERS: &str = formatcp!("{TOKEN_V2_ADDR}::token::TokenIdentifiers");
pub const TYPE_PROPERTY_MAP: &str = formatcp!("{TOKEN_V2_ADDR}::property_map::PropertyMap");

pub trait Resource {
    fn type_str() -> &'static str;
}

pub trait FromWriteResource<'a> {
    fn from_write_resource(write_resource: &'a WriteResource) -> Result<Option<Self>>
    where
        Self: Sized;
}

impl<'a, T> FromWriteResource<'a> for T
where
    T: TryFrom<&'a WriteResource, Error = anyhow::Error> + Resource,
{
    fn from_write_resource(write_resource: &'a WriteResource) -> Result<Option<Self>> {
        if MoveResource::get_outer_type_from_write_resource(write_resource) != Self::type_str() {
            return Ok(None);
        }
        Ok(Some(write_resource.try_into()?))
    }
}

pub enum V2FungibleAssetResource {
    ConcurrentFungibleAssetBalance(ConcurrentFungibleAssetBalance),
    ConcurrentFungibleAssetSupply(ConcurrentFungibleAssetSupply),
    FungibleAssetMetadata(FungibleAssetMetadata),
    FungibleAssetStore(FungibleAssetStore),
    FungibleAssetSupply(FungibleAssetSupply),
}

impl Resource for ConcurrentFungibleAssetBalance {
    fn type_str() -> &'static str {
        TYPE_CONCURRENT_FUNGIBLE_ASSET_BALANCE
    }
}

impl Resource for ConcurrentFungibleAssetSupply {
    fn type_str() -> &'static str {
        TYPE_CONCURRENT_FUNGIBLE_ASSET_SUPPLY
    }
}

impl Resource for FungibleAssetMetadata {
    fn type_str() -> &'static str {
        TYPE_FUNGIBLE_ASSET_METADATA
    }
}

impl Resource for FungibleAssetStore {
    fn type_str() -> &'static str {
        TYPE_FUNGIBLE_ASSET_STORE
    }
}

impl Resource for FungibleAssetSupply {
    fn type_str() -> &'static str {
        TYPE_FUNGIBLE_ASSET_SUPPLY
    }
}

impl V2FungibleAssetResource {
    pub fn from_write_resource(write_resource: &WriteResource) -> Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_write_resource(write_resource);
        Ok(Some(match type_str.as_str() {
            TYPE_CONCURRENT_FUNGIBLE_ASSET_BALANCE => {
                Self::ConcurrentFungibleAssetBalance(write_resource.try_into()?)
            },
            TYPE_CONCURRENT_FUNGIBLE_ASSET_SUPPLY => {
                Self::ConcurrentFungibleAssetSupply(write_resource.try_into()?)
            },
            TYPE_FUNGIBLE_ASSET_METADATA => Self::FungibleAssetMetadata(write_resource.try_into()?),
            TYPE_FUNGIBLE_ASSET_STORE => Self::FungibleAssetStore(write_resource.try_into()?),
            TYPE_FUNGIBLE_ASSET_SUPPLY => Self::FungibleAssetSupply(write_resource.try_into()?),
            _ => return Ok(None),
        }))
    }
}

pub enum V2TokenResource {
    AptosCollection(AptosCollection),
    Collection(Collection),
    ConcurrentSupply(ConcurrentSupply),
    FixedSupply(FixedSupply),
    ObjectCore(ObjectCore),
    PropertyMapModel(PropertyMapModel),
    TokenIdentifiers(TokenIdentifiers),
    TokenV2(TokenV2),
    UnlimitedSupply(UnlimitedSupply),
    Untransferable(Untransferable),
}

impl Resource for AptosCollection {
    fn type_str() -> &'static str {
        TYPE_APOTS_COLLECTION
    }
}

impl Resource for Collection {
    fn type_str() -> &'static str {
        TYPE_COLLECTION
    }
}

impl Resource for ConcurrentSupply {
    fn type_str() -> &'static str {
        TYPE_CONCURRENT_SUPPLY
    }
}

impl Resource for FixedSupply {
    fn type_str() -> &'static str {
        TYPE_FIXED_SUPPLY
    }
}

impl Resource for ObjectCore {
    fn type_str() -> &'static str {
        TYPE_OBJECT_CORE
    }
}

impl Resource for PropertyMapModel {
    fn type_str() -> &'static str {
        TYPE_PROPERTY_MAP
    }
}

impl Resource for TokenIdentifiers {
    fn type_str() -> &'static str {
        TYPE_TOKEN_IDENTIFIERS
    }
}

impl Resource for TokenV2 {
    fn type_str() -> &'static str {
        TYPE_TOKEN_V2
    }
}

impl Resource for UnlimitedSupply {
    fn type_str() -> &'static str {
        TYPE_UNLIMITED_SUPPLY
    }
}

impl Resource for Untransferable {
    fn type_str() -> &'static str {
        TYPE_UNTRANSFERABLE
    }
}

impl V2TokenResource {
    pub fn from_write_resource(write_resource: &WriteResource) -> Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_write_resource(write_resource);
        Ok(Some(match type_str.as_str() {
            TYPE_APOTS_COLLECTION => Self::AptosCollection(write_resource.try_into()?),
            TYPE_COLLECTION => Self::Collection(write_resource.try_into()?),
            TYPE_CONCURRENT_SUPPLY => Self::ConcurrentSupply(write_resource.try_into()?),
            TYPE_FIXED_SUPPLY => Self::FixedSupply(write_resource.try_into()?),
            TYPE_OBJECT_CORE => Self::ObjectCore(write_resource.try_into()?),
            TYPE_PROPERTY_MAP => Self::PropertyMapModel(write_resource.try_into()?),
            TYPE_TOKEN_IDENTIFIERS => Self::TokenIdentifiers(write_resource.try_into()?),
            TYPE_TOKEN_V2 => Self::TokenV2(write_resource.try_into()?),
            TYPE_UNLIMITED_SUPPLY => Self::UnlimitedSupply(write_resource.try_into()?),
            TYPE_UNTRANSFERABLE => Self::Untransferable(write_resource.try_into()?),
            _ => return Ok(None),
        }))
    }
}
