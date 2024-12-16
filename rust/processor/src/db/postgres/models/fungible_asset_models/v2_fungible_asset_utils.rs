// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::{
        common::models::token_v2_models::v2_token_utils::ResourceReference,
        postgres::models::token_models::token_utils::URI_LENGTH,
    },
    utils::util::{deserialize_from_string, truncate_str, Aggregator},
};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::WriteResource;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

const FUNGIBLE_ASSET_LENGTH: usize = 32;
const FUNGIBLE_ASSET_SYMBOL: usize = 10;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FeeStatement {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub storage_fee_refund_octas: u64,
}

impl FeeStatement {
    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Option<Self> {
        if data_type == "0x1::transaction_fee::FeeStatement" {
            let fee_statement: FeeStatement = serde_json::from_str(data).unwrap_or_else(|_| {
                tracing::error!(
                    transaction_version = txn_version,
                    data = data,
                    "failed to parse event for fee statement"
                );
                panic!();
            });
            Some(fee_statement)
        } else {
            None
        }
    }
}

/* Section on fungible assets resources */
#[derive(Serialize, Deserialize, Debug, Clone, FieldCount)]
pub struct FungibleAssetMetadata {
    name: String,
    symbol: String,
    pub decimals: i32,
    icon_uri: String,
    project_uri: String,
}

impl TryFrom<&WriteResource> for FungibleAssetMetadata {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

impl FungibleAssetMetadata {
    pub fn get_name(&self) -> String {
        truncate_str(&self.name, FUNGIBLE_ASSET_LENGTH)
    }

    pub fn get_symbol(&self) -> String {
        truncate_str(&self.symbol, FUNGIBLE_ASSET_SYMBOL)
    }

    pub fn get_icon_uri(&self) -> String {
        truncate_str(&self.icon_uri, URI_LENGTH)
    }

    pub fn get_project_uri(&self) -> String {
        truncate_str(&self.project_uri, URI_LENGTH)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FungibleAssetStore {
    pub metadata: ResourceReference,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub balance: BigDecimal,
    pub frozen: bool,
}

impl TryFrom<&WriteResource> for FungibleAssetStore {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FungibleAssetSupply {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub current: BigDecimal,
    pub maximum: OptionalBigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptionalBigDecimal {
    vec: Vec<BigDecimalWrapper>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BigDecimalWrapper(#[serde(deserialize_with = "deserialize_from_string")] pub BigDecimal);

impl TryFrom<&WriteResource> for FungibleAssetSupply {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

impl FungibleAssetSupply {
    pub fn get_maximum(&self) -> Option<BigDecimal> {
        self.maximum.vec.first().map(|x| x.0.clone())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConcurrentFungibleAssetSupply {
    pub current: Aggregator,
}

impl TryFrom<&WriteResource> for ConcurrentFungibleAssetSupply {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConcurrentFungibleAssetBalance {
    pub balance: Aggregator,
}

impl TryFrom<&WriteResource> for ConcurrentFungibleAssetBalance {
    type Error = anyhow::Error;

    fn try_from(write_resource: &WriteResource) -> anyhow::Result<Self> {
        serde_json::from_str(write_resource.data.as_str()).map_err(anyhow::Error::msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WithdrawEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FrozenEvent {
    pub frozen: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositEventV2 {
    pub store: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WithdrawEventV2 {
    pub store: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FrozenEventV2 {
    pub store: String,
    pub frozen: bool,
}

pub enum FungibleAssetEvent {
    DepositEvent(DepositEvent),
    WithdrawEvent(WithdrawEvent),
    FrozenEvent(FrozenEvent),
    DepositEventV2(DepositEventV2),
    WithdrawEventV2(WithdrawEventV2),
    FrozenEventV2(FrozenEventV2),
}

impl FungibleAssetEvent {
    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Result<Option<Self>> {
        match data_type {
            "0x1::fungible_asset::DepositEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::DepositEvent(inner)))
            },
            "0x1::fungible_asset::WithdrawEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::WithdrawEvent(inner)))
            },
            "0x1::fungible_asset::FrozenEvent" => {
                serde_json::from_str(data).map(|inner| Some(Self::FrozenEvent(inner)))
            },
            "0x1::fungible_asset::Deposit" => {
                serde_json::from_str(data).map(|inner| Some(Self::DepositEventV2(inner)))
            },
            "0x1::fungible_asset::Withdraw" => {
                serde_json::from_str(data).map(|inner| Some(Self::WithdrawEventV2(inner)))
            },
            "0x1::fungible_asset::Frozen" => {
                serde_json::from_str(data).map(|inner| Some(Self::FrozenEventV2(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::postgres::models::resources::V2FungibleAssetResource;

    #[test]
    fn test_fungible_asset_supply_null() {
        let test = r#"{"current": "0", "maximum": {"vec": []}}"#;
        let test: serde_json::Value = serde_json::from_str(test).unwrap();
        let supply = serde_json::from_value(test)
            .map(V2FungibleAssetResource::FungibleAssetSupply)
            .unwrap();
        if let V2FungibleAssetResource::FungibleAssetSupply(supply) = supply {
            assert_eq!(supply.current, BigDecimal::from(0));
            assert_eq!(supply.get_maximum(), None);
        } else {
            panic!("Wrong type")
        }
    }

    #[test]
    fn test_fungible_asset_supply_nonnull() {
        let test = r#"{"current": "100", "maximum": {"vec": ["5000"]}}"#;
        let test: serde_json::Value = serde_json::from_str(test).unwrap();
        let supply = serde_json::from_value(test)
            .map(V2FungibleAssetResource::FungibleAssetSupply)
            .unwrap();
        if let V2FungibleAssetResource::FungibleAssetSupply(supply) = supply {
            assert_eq!(supply.current, BigDecimal::from(100));
            assert_eq!(supply.get_maximum(), Some(BigDecimal::from(5000)));
        } else {
            panic!("Wrong type")
        }
    }

    // TODO: Add similar tests for ConcurrentFungibleAssetSupply.
}
