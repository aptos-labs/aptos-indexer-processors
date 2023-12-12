// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_fungible_asset_activities::EventToCoinType, v2_fungible_asset_utils::FungibleAssetStore,
    v2_fungible_metadata::FungibleAssetMetadataModel,
};
use crate::{
    models::{
        coin_models::coin_utils::{CoinInfoType, CoinResource},
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::{current_fungible_asset_balances, fungible_asset_balances},
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::WriteResource;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use hex::FromHex;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha3::Sha3_256;
use std::collections::HashMap;

// Storage id
pub type CurrentFungibleAssetBalancePK = String;
pub type CurrentFungibleAssetMapping =
    HashMap<CurrentFungibleAssetBalancePK, CurrentFungibleAssetBalance>;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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

impl FungibleAssetBalance {
    /// Basically just need to index FA Store, but we'll need to look up FA metadata
    pub async fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        fungible_asset_metadata: &ObjectAggregatedDataMapping,
        conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<Option<(Self, CurrentFungibleAssetBalance)>> {
        if let Some(inner) = &FungibleAssetStore::from_write_resource(write_resource, txn_version)?
        {
            let storage_id = standardize_address(write_resource.address.as_str());
            // Need to get the object of the store
            if let Some(store_metadata) = fungible_asset_metadata.get(&storage_id) {
                let object = &store_metadata.object.object_core;
                let owner_address = object.get_owner_address();
                let asset_type = inner.metadata.get_reference_address();
                // If it's a fungible token, return early
                if !FungibleAssetMetadataModel::is_address_fungible_asset(
                    conn,
                    &asset_type,
                    fungible_asset_metadata,
                )
                .await
                {
                    return Ok(None);
                }
                let is_primary = Self::is_primary(&owner_address, &asset_type, &storage_id);

                let coin_balance = Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: asset_type.clone(),
                    is_primary,
                    is_frozen: inner.frozen,
                    amount: inner.balance.clone(),
                    transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V2.to_string(),
                };
                let current_coin_balance = CurrentFungibleAssetBalance {
                    storage_id,
                    owner_address,
                    asset_type: asset_type.clone(),
                    is_primary,
                    is_frozen: inner.frozen,
                    amount: inner.balance.clone(),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    token_standard: TokenStandard::V2.to_string(),
                };
                return Ok(Some((coin_balance, current_coin_balance)));
            }
        }

        Ok(None)
    }

    /// Getting coin balances from resources for v1
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
                let event_to_coin_mapping: EventToCoinType = HashMap::from([
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
        let owner_address_bytes = <[u8; 32]>::from_hex(&owner_address[2..]).unwrap();
        let metadata_address_bytes = <[u8; 32]>::from_hex(&metadata_address[2..]).unwrap();

        // construct the expected metadata address
        let mut hasher = Sha3_256::new();
        hasher.update(owner_address_bytes);
        hasher.update(metadata_address_bytes);
        hasher.update([0xFC]);
        let hash_result = hasher.finalize();
        // compare address to actual metadata address
        hex::encode(hash_result) == fungible_store_address[2..]
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
            fungible_store_address
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
            fungible_store_address
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
            fungible_store_address
        ));
    }
}
