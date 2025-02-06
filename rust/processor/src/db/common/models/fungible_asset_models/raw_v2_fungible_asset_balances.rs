// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    raw_v2_fungible_asset_activities::AddressToCoinType,
    raw_v2_fungible_asset_to_coin_mappings::{
        FungibleAssetToCoinMappings, RawFungibleAssetToCoinMapping,
    },
};
use crate::{
    db::{
        common::models::{
            fungible_asset_models::raw_v2_fungible_asset_activities::EventToCoinType,
            object_models::v2_object_utils::ObjectAggregatedDataMapping,
            token_v2_models::v2_token_utils::TokenStandard,
        },
        postgres::models::{
            coin_models::coin_utils::{CoinInfoType, CoinResource},
            fungible_asset_models::v2_fungible_asset_utils::FungibleAssetStore,
            resources::FromWriteResource,
        },
    },
    utils::util::{
        hex_to_raw_bytes, sha3_256, standardize_address, APTOS_COIN_TYPE_STR,
        APT_METADATA_ADDRESS_HEX, APT_METADATA_ADDRESS_RAW,
    },
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::{BigDecimal, Zero};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

// Storage id
pub type CurrentUnifiedFungibleAssetMapping =
    AHashMap<String, RawCurrentUnifiedFungibleAssetBalance>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawFungibleAssetBalance {
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

pub trait FungibleAssetBalanceConvertible {
    fn from_raw(raw_item: RawFungibleAssetBalance) -> Self;
}

/// Note that this used to be called current_unified_fungible_asset_balances_to_be_renamed
/// and was renamed to current_fungible_asset_balances to facilitate migration
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct RawCurrentUnifiedFungibleAssetBalance {
    pub storage_id: String,
    pub owner_address: String,
    // metadata address for (paired) Fungible Asset
    pub asset_type_v1: Option<String>,
    pub asset_type_v2: Option<String>,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount_v1: Option<BigDecimal>,
    pub amount_v2: Option<BigDecimal>,
    pub last_transaction_version_v1: Option<i64>,
    pub last_transaction_version_v2: Option<i64>,
    pub last_transaction_timestamp_v1: Option<chrono::NaiveDateTime>,
    pub last_transaction_timestamp_v2: Option<chrono::NaiveDateTime>,
}

pub trait CurrentUnifiedFungibleAssetBalanceConvertible {
    fn from_raw(raw_item: RawCurrentUnifiedFungibleAssetBalance) -> Self;
}

pub fn get_paired_metadata_address(coin_type_name: &str) -> String {
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

impl RawCurrentUnifiedFungibleAssetBalance {
    pub fn from_fungible_asset_balances(
        fungible_asset_balances: &[RawFungibleAssetBalance],
        fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
    ) -> (
        CurrentUnifiedFungibleAssetMapping,
        CurrentUnifiedFungibleAssetMapping,
    ) {
        // Dedupe fungible asset balances by storage_id, keeping latest version
        let mut v1_balances: CurrentUnifiedFungibleAssetMapping = AHashMap::new();
        let mut v2_balances: CurrentUnifiedFungibleAssetMapping = AHashMap::new();

        for balance in fungible_asset_balances.iter() {
            let unified_balance = Self::from_balance(balance, fa_to_coin_mapping);
            match TokenStandard::from_str(&balance.token_standard).expect("Invalid token standard")
            {
                TokenStandard::V1 => {
                    v1_balances.insert(unified_balance.storage_id.clone(), unified_balance);
                },
                TokenStandard::V2 => {
                    v2_balances.insert(unified_balance.storage_id.clone(), unified_balance);
                },
            }
        }
        (v1_balances, v2_balances)
    }

    pub fn from_balance(
        fab: &RawFungibleAssetBalance,
        fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
    ) -> Self {
        // Determine if this is a V2 token standard
        let is_v2 = matches!(
            TokenStandard::from_str(&fab.token_standard).expect("Invalid token standard"),
            TokenStandard::V2
        );
        // For V2 tokens, asset_type_v2 is the original asset type
        // For V1 tokens, asset_type_v2 is None
        let asset_type_v2 = is_v2.then(|| fab.asset_type.clone());

        // For V2 tokens, look up V1 equivalent in mapping
        // For V1 tokens, use original asset type
        let asset_type_v1 = if is_v2 {
            RawFungibleAssetToCoinMapping::get_asset_type_v1(&fab.asset_type, fa_to_coin_mapping)
        } else {
            Some(fab.asset_type.clone())
        };

        // V1 tokens are always primary, V2 tokens use the stored value
        let is_primary = if is_v2 { fab.is_primary } else { true };

        // Amount and transaction details are stored in v1 or v2 fields based on token standard
        let (amount_v1, amount_v2, version_v1, version_v2, timestamp_v1, timestamp_v2) = if is_v2 {
            (
                None,
                Some(fab.amount.clone()),
                None,
                Some(fab.transaction_version),
                None,
                Some(fab.transaction_timestamp),
            )
        } else {
            (
                Some(fab.amount.clone()),
                None,
                Some(fab.transaction_version),
                None,
                Some(fab.transaction_timestamp),
                None,
            )
        };

        Self {
            storage_id: fab.storage_id.clone(),
            owner_address: fab.owner_address.clone(),
            asset_type_v1,
            asset_type_v2,
            is_primary,
            is_frozen: fab.is_frozen,
            amount_v1,
            amount_v2,
            last_transaction_version_v1: version_v1,
            last_transaction_version_v2: version_v2,
            last_transaction_timestamp_v1: timestamp_v1,
            last_transaction_timestamp_v2: timestamp_v2,
        }
    }
}

impl RawFungibleAssetBalance {
    /// Basically just need to index FA Store, but we'll need to look up FA metadata
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) = &FungibleAssetStore::from_write_resource(write_resource)? {
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
                return Ok(Some(coin_balance));
            }
        }

        Ok(None)
    }

    pub fn get_v1_from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, AddressToCoinType)>> {
        if let Some(CoinResource::CoinStoreDeletion) =
            &CoinResource::from_delete_resource(delete_resource, txn_version)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &delete_resource.r#type.as_ref().unwrap().generic_type_params[0],
                delete_resource.type_str.as_ref(),
                txn_version,
                write_set_change_index,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(delete_resource.address.as_str());
                // Storage id should be derived (for the FA migration)
                let metadata_addr = get_paired_metadata_address(&coin_type);
                let storage_id = get_primary_fungible_store_address(&owner_address, &metadata_addr)
                    .expect("calculate primary fungible store failed");
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
                // Create address to coin type mapping
                let mut address_to_coin_type = AHashMap::new();
                address_to_coin_type.extend([(owner_address.clone(), coin_type.clone())]);
                return Ok(Some((coin_balance, address_to_coin_type)));
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
    ) -> anyhow::Result<Option<(Self, EventToCoinType)>> {
        if let Some(CoinResource::CoinStoreResource(inner)) =
            &CoinResource::from_write_resource(write_resource, txn_version)?
        {
            let coin_info_type = &CoinInfoType::from_move_type(
                &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                write_resource.type_str.as_ref(),
                txn_version,
                write_set_change_index,
            );
            if let Some(coin_type) = coin_info_type.get_coin_type_below_max() {
                let owner_address = standardize_address(write_resource.address.as_str());
                // Storage id should be derived (for the FA migration)
                let metadata_addr = get_paired_metadata_address(&coin_type);
                let storage_id = get_primary_fungible_store_address(&owner_address, &metadata_addr)
                    .expect("calculate primary fungible store failed");
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
                let event_to_coin_mapping: EventToCoinType = AHashMap::from([
                    (
                        inner.withdraw_events.guid.id.get_standardized(),
                        coin_type.clone(),
                    ),
                    (inner.deposit_events.guid.id.get_standardized(), coin_type),
                ]);
                return Ok(Some((coin_balance, event_to_coin_mapping)));
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

        assert!(RawFungibleAssetBalance::is_primary(
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

        assert!(!RawFungibleAssetBalance::is_primary(
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

        assert!(RawFungibleAssetBalance::is_primary(
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
