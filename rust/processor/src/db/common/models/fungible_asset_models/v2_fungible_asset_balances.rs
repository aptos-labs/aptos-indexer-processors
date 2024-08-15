// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_fungible_asset_activities::EventToCoinType, v2_fungible_asset_utils::FungibleAssetStore,
};
use crate::{
    db::common::models::{
        coin_models::coin_utils::{CoinInfoType, CoinResource},
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2_models::v2_token_utils::{TokenStandard, V2_STANDARD},
    },
    schema::{
        current_fungible_asset_balances, current_unified_fungible_asset_balances_to_be_renamed,
        fungible_asset_balances,
    },
    utils::util::{
        hex_to_raw_bytes, sha3_256, standardize_address, APTOS_COIN_TYPE_STR,
        APT_METADATA_ADDRESS_HEX, APT_METADATA_ADDRESS_RAW,
    },
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::{BigDecimal, Zero};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

// Storage id
pub type CurrentFungibleAssetBalancePK = String;
pub type CurrentFungibleAssetMapping =
    AHashMap<CurrentFungibleAssetBalancePK, CurrentFungibleAssetBalance>;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = fungible_asset_balances)]
pub struct FungibleAssetBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_fungible_asset_balances)]
pub struct CurrentFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: BigDecimal,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Default)]
#[diesel(primary_key(storage_id))]
#[diesel(table_name = current_unified_fungible_asset_balances_to_be_renamed)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: Option<bool>,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

fn get_paired_metadata_address(coin_type_name: &str) -> String {
    if coin_type_name == APTOS_COIN_TYPE_STR {
        APT_METADATA_ADDRESS_HEX.clone()
    } else {
        let mut preimage = APT_METADATA_ADDRESS_RAW.to_vec();
        preimage.extend(coin_type_name.as_bytes());
        preimage.push(0xFE);
        format!("0x{}", hex::encode(sha3_256(&preimage)))
    }
}

pub fn get_primary_fungible_store_address(
    owner_address: &str,
    metadata_address: &str,
) -> anyhow::Result<String> {
    let mut preimage = hex_to_raw_bytes(owner_address)?;
    preimage.append(&mut hex_to_raw_bytes(metadata_address)?);
    preimage.push(0xFC);
    Ok(standardize_address(&hex::encode(sha3_256(&preimage))))
}

impl From<&CurrentFungibleAssetBalance> for CurrentUnifiedFungibleAssetBalance {
    fn from(cfab: &CurrentFungibleAssetBalance) -> Self {
        if cfab.token_standard.as_str() == V2_STANDARD.borrow().as_str() {
            Self {
                storage_id: cfab.storage_id.clone(),
                owner_address: cfab.owner_address.clone(),
                asset_type_v2: Some(cfab.asset_type.clone()),
                asset_type_v1: None,
                is_primary: Some(cfab.is_primary),
                is_frozen: cfab.is_frozen,
                amount_v1: None,
                amount_v2: Some(cfab.amount.clone()),
                last_transaction_version_v1: None,
                last_transaction_version_v2: Some(cfab.last_transaction_version),
                last_transaction_timestamp_v1: None,
                last_transaction_timestamp_v2: Some(cfab.last_transaction_timestamp),
            }
        } else {
            let metadata_addr = get_paired_metadata_address(&cfab.asset_type);
            let pfs_addr = get_primary_fungible_store_address(&cfab.owner_address, &metadata_addr)
                .expect("calculate pfs_address failed");
            Self {
                storage_id: pfs_addr,
                owner_address: cfab.owner_address.clone(),
                asset_type_v2: None,
                asset_type_v1: Some(cfab.asset_type.clone()),
                is_primary: None,
                is_frozen: cfab.is_frozen,
                amount_v1: Some(cfab.amount.clone()),
                amount_v2: None,
                last_transaction_version_v1: Some(cfab.last_transaction_version),
                last_transaction_version_v2: None,
                last_transaction_timestamp_v1: Some(cfab.last_transaction_timestamp),
                last_transaction_timestamp_v2: None,
            }
        }
    }
}

impl FungibleAssetBalance {
    /// Basically just need to index FA Store, but we'll need to look up FA metadata
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentFungibleAssetBalance)>> {
        if let Some(inner) = &FungibleAssetStore::from_write_resource(write_resource, txn_version)?
        {
            let storage_id = standardize_address(write_resource.address.as_str());
            // Need to get the object of the store
            if let Some(object_data) = object_metadatas.get(&storage_id) {
                let object = &object_data.object.object_core;
                let owner_address = object.get_owner_address();
                let asset_type = inner.metadata.get_reference_address();
                let is_primary = Self::is_primary(&owner_address, &asset_type, &storage_id);

                #[allow(clippy::useless_asref)]
                let concurrent_balance = object_data
                    .concurrent_fungible_asset_balance
                    .as_ref()
                    .map(|concurrent_fungible_asset_balance| {
                        concurrent_fungible_asset_balance.balance.value.clone()
                    });

                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: asset_type.clone(),
                    is_primary,
                    is_frozen: inner.frozen,
                    amount: concurrent_balance
                        .clone()
                        .unwrap_or_else(|| inner.balance.clone()),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V2.to_string(),
                };
                let current_coin_balance = CurrentFungibleAssetBalance {
                    storage_id,
                    owner_address,
                    asset_type: asset_type.clone(),
                    is_primary,
                    is_frozen: inner.frozen,
                    amount: concurrent_balance.unwrap_or_else(|| inner.balance.clone()),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V2.to_string(),
                };
                return Ok(Some((coin_balance, current_coin_balance)));
            }
        }

        Ok(None)
    }

    pub fn get_v1_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentFungibleAssetBalance, EventToCoinType)>> {
        if let Some(CoinResource::CoinStoreDeletion) =
            &CoinResource::from_delete_resource(delete_resource, txn_version)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &delete_resource.r#type.as_ref().unwrap().generic_type_params[0],
                delete_resource.type_str.as_ref(),
                txn_version,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(delete_resource.address.as_str());
                let storage_id =
                    CoinInfoType::get_storage_id(coin_type.as_str(), owner_address.as_str());
                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: false,
                    amount: BigDecimal::zero(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                let current_coin_balance = CurrentFungibleAssetBalance {
                    storage_id,
                    owner_address,
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: false,
                    amount: BigDecimal::zero(),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                return Ok(Some((
                    coin_balance,
                    current_coin_balance,
                    AHashMap::default(),
                )));
            }
        }
        Ok(None)
    }

    /// Getting coin balances from resources for v1
    /// If the fully qualified coin type is too long (currently 1000 length), we exclude from indexing
    pub fn get_v1_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentFungibleAssetBalance, EventToCoinType)>> {
        if let Some(CoinResource::CoinStoreResource(inner)) =
            &CoinResource::from_write_resource(write_resource, txn_version)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                write_resource.type_str.as_ref(),
                txn_version,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(write_resource.address.as_str());
                let storage_id =
                    CoinInfoType::get_storage_id(coin_type.as_str(), owner_address.as_str());
                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: inner.frozen,
                    amount: inner.coin.value.clone(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                let current_coin_balance = CurrentFungibleAssetBalance {
                    storage_id,
                    owner_address,
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: inner.frozen,
                    amount: inner.coin.value.clone(),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V1.to_string(),
                };
                let event_to_coin_mapping: EventToCoinType = AHashMap::from([
                    (
                        inner.withdraw_events.guid.id.get_standardized(),
                        coin_type.clone(),
                    ),
                    (inner.deposit_events.guid.id.get_standardized(), coin_type),
                ]);
                return Ok(Some((
                    coin_balance,
                    current_coin_balance,
                    event_to_coin_mapping,
                )));
            }
        }
        Ok(None)
    }

    /// Primary store address are derived from the owner address and object address in this format: sha3_256([source | object addr | 0xFC]).
    /// This function expects the addresses to have length 66
    pub fn is_primary(
        owner_address: &str,
        metadata_address: &str,
        fungible_store_address: &str,
    ) -> bool {
        fungible_store_address
            == get_primary_fungible_store_address(owner_address, metadata_address).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_primary() {
        let owner_address = "0xfd2984f201abdbf30ccd0ec5c2f2357789222c0bbd3c68999acfebe188fdc09d";
        let metadata_address = "0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309";
        let fungible_store_address =
            "0x5d2c93f23a3964409e8755a179417c4ef842166f6cc41e1416e2c705a02861a6";

        assert!(FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_is_not_primary() {
        let owner_address = "0xfd2984f201abdbf30ccd0ec5c2f2357789222c0bbd3c68999acfebe188fdc09d";
        let metadata_address = "0x5dade62351d0b07340ff41763451e05ca2193de583bb3d762193462161888309";
        let fungible_store_address = "something random";

        assert!(!FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_zero_prefix() {
        let owner_address = "0x049cad43b33c9f907ff80c5f0897ac6bfe6034feea0c9070e37814d1f9efd090";
        let metadata_address = "0x03b0e839106b65826e54fa4c160ca653594b723a5e481a5121c333849bc46f6c";
        let fungible_store_address =
            "0xd4af0c43c6228357d7a09da77bf244cd4a1b97a0eb8ef3df43823ff4a807d0b9";

        assert!(FungibleAssetBalance::is_primary(
            owner_address,
            metadata_address,
            fungible_store_address,
        ));
    }

    #[test]
    fn test_paired_metadata_address() {
        assert_eq!(
            get_paired_metadata_address("0x1::aptos_coin::AptosCoin"),
            *APT_METADATA_ADDRESS_HEX
        );
        assert_eq!(get_paired_metadata_address("0x66c34778730acbb120cefa57a3d98fd21e0c8b3a51e9baee530088b2e444e94c::moon_coin::MoonCoin"), "0xf772c28c069aa7e4417d85d771957eb3c5c11b5bf90b1965cda23b899ebc0384");
    }
}
