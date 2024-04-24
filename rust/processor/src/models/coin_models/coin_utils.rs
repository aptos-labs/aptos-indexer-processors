// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    models::default_models::move_resources::MoveResource,
    utils::util::{deserialize_from_string, hash_str, standardize_address, truncate_str},
};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{move_type::Content, MoveType, WriteResource};
use bigdecimal::BigDecimal;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::error;

pub const COIN_ADDR: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const COIN_TYPE_HASH_LENGTH: usize = 5000;
const COIN_TYPE_MAX: usize = 1000;

/**
 * This file defines deserialized coin types as defined in our 0x1 contracts.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CoinInfoResource {
    name: String,
    symbol: String,
    pub decimals: i32,
    pub supply: OptionalAggregatorWrapperResource,
}

impl CoinInfoResource {
    pub fn get_name_trunc(&self) -> String {
        truncate_str(&self.name, 32)
    }

    pub fn get_symbol_trunc(&self) -> String {
        truncate_str(&self.symbol, 10)
    }

    /// Getting the table item location of the supply aggregator
    pub fn get_aggregator_metadata(&self) -> Option<AggregatorResource> {
        if let Some(inner) = self.supply.vec.first() {
            inner.aggregator.get_aggregator_metadata()
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptionalAggregatorWrapperResource {
    pub vec: Vec<OptionalAggregatorResource>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptionalAggregatorResource {
    pub aggregator: AggregatorWrapperResource,
    pub integer: IntegerWrapperResource,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggregatorWrapperResource {
    pub vec: Vec<AggregatorResource>,
}

impl AggregatorWrapperResource {
    /// In case we do want to track supply
    pub fn get_aggregator_metadata(&self) -> Option<AggregatorResource> {
        self.vec.first().cloned()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IntegerWrapperResource {
    pub vec: Vec<IntegerResource>,
}

impl IntegerWrapperResource {
    /// In case we do want to track supply
    pub fn get_supply(&self) -> Option<BigDecimal> {
        self.vec.first().map(|inner| inner.value.clone())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggregatorResource {
    pub handle: String,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IntegerResource {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub value: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CoinStoreResource {
    pub coin: Coin,
    pub deposit_events: DepositEventResource,
    pub withdraw_events: WithdrawEventResource,
    pub frozen: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Coin {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub value: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositEventResource {
    pub guid: EventGuidResourceWrapper,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WithdrawEventResource {
    pub guid: EventGuidResourceWrapper,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventGuidResourceWrapper {
    pub id: EventGuidResource,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct EventGuidResource {
    pub addr: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub creation_num: i64,
}

impl EventGuidResource {
    pub fn get_address(&self) -> String {
        standardize_address(&self.addr)
    }

    pub fn get_standardized(&self) -> Self {
        Self {
            addr: self.get_address(),
            creation_num: self.creation_num,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WithdrawCoinEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositCoinEvent {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub amount: BigDecimal,
}

pub struct CoinInfoType {
    coin_type: String,
    creator_address: String,
}

static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(<(.*)>)").unwrap());

static COIN_RESOURCES: Lazy<[String; 2]> = Lazy::new(|| {
    [
        format!("{}::coin::CoinInfo", COIN_ADDR),
        format!("{}::coin::CoinStore", COIN_ADDR),
    ]
});

impl CoinInfoType {
    /// get creator address from move_type, and get coin type from move_type_str
    /// Since move_type_str will contain things we don't need, e.g. 0x1::coin::CoinInfo<T>. We will use
    /// regex to extract T.
    pub fn from_move_type(move_type: &MoveType, move_type_str: &str, txn_version: i64) -> Self {
        if let Content::Struct(struct_tag) = move_type.content.as_ref().unwrap() {
            let matched = RE.captures(move_type_str).unwrap_or_else(|| {
                error!(
                    txn_version = txn_version,
                    move_type_str = move_type_str,
                    "move_type should look like 0x1::coin::CoinInfo<T>"
                );
                panic!();
            });
            let coin_type = matched.get(2).unwrap().as_str();
            Self {
                coin_type: coin_type.to_string(),
                creator_address: struct_tag.address.clone(),
            }
        } else {
            error!(txn_version = txn_version, move_type = ?move_type, "Expected struct tag");
            panic!();
        }
    }

    pub fn get_creator_address(&self) -> String {
        standardize_address(&self.creator_address)
    }

    pub fn to_hash(&self) -> String {
        hash_str(&self.coin_type.to_string())
    }

    /// This function gets the hash of the owner address and the coin type, similar to
    /// how token v2 gets the named object address for the fungible asset store.
    pub fn get_storage_id(coin_type: &str, owner_address: &str) -> String {
        let key = format!("{}::{}", owner_address, coin_type);
        format!("0x{}", hash_str(&key))
    }

    pub fn get_coin_type_trunc(&self) -> String {
        truncate_str(&self.coin_type, COIN_TYPE_HASH_LENGTH)
    }

    pub fn get_coin_type_below_max(&self) -> Option<String> {
        if self.coin_type.len() > COIN_TYPE_MAX {
            None
        } else {
            Some(self.coin_type.clone())
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CoinResource {
    CoinInfoResource(CoinInfoResource),
    CoinStoreResource(CoinStoreResource),
}

impl CoinResource {
    pub fn is_resource_supported(data_type: &str) -> bool {
        COIN_RESOURCES.contains(&data_type.to_string())
    }

    pub fn from_resource(
        data_type: &str,
        data: &serde_json::Value,
        txn_version: i64,
    ) -> Result<CoinResource> {
        match data_type {
            x if x == format!("{}::coin::CoinInfo", COIN_ADDR) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(CoinResource::CoinInfoResource(inner)))
            },
            x if x == format!("{}::coin::CoinStore", COIN_ADDR) => {
                serde_json::from_value(data.clone())
                    .map(|inner| Some(CoinResource::CoinStoreResource(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))?
        .context(format!(
            "Resource unsupported! Call is_resource_supported first. version {} type {}",
            txn_version, data_type
        ))
    }

    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> Result<Option<CoinResource>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !CoinResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );
        Ok(Some(Self::from_resource(
            &type_str,
            resource.data.as_ref().unwrap(),
            txn_version,
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CoinEvent {
    WithdrawCoinEvent(WithdrawCoinEvent),
    DepositCoinEvent(DepositCoinEvent),
}

impl CoinEvent {
    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Result<Option<CoinEvent>> {
        match data_type {
            "0x1::coin::WithdrawEvent" => {
                serde_json::from_str(data).map(|inner| Some(CoinEvent::WithdrawCoinEvent(inner)))
            },
            "0x1::coin::DepositEvent" => {
                serde_json::from_str(data).map(|inner| Some(CoinEvent::DepositCoinEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))
    }
}
