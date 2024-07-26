// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        coin_models::coin_utils::{CoinInfoType, CoinResource},
        fungible_asset_models::{
            v2_fungible_asset_activities::EventToCoinType,
            v2_fungible_asset_balances::{
                get_primary_fungible_store_address, CurrentFungibleAssetBalance,
            },
            v2_fungible_asset_utils::FungibleAssetStore,
        },
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2_models::v2_token_utils::TokenStandard,
    },
    utils::util::standardize_address,
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::{BigDecimal, Zero};
use field_count::FieldCount;
use lazy_static::lazy_static;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

lazy_static! {
    pub static ref DEFAULT_AMOUNT_VALUE: String = "0".to_string();
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct FungibleAssetBalance {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub storage_id: String,
    pub owner_address: String,
    pub asset_type: String,
    pub is_primary: bool,
    pub is_frozen: bool,
    pub amount: String, // it is a string representation of the u128
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub token_standard: String,
}

impl NamedTable for FungibleAssetBalance {
    const TABLE_NAME: &'static str = "fungible_asset_balances";
}

impl HasVersion for FungibleAssetBalance {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for FungibleAssetBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl FungibleAssetBalance {
    /// Basically just need to index FA Store, but we'll need to look up FA metadata
    pub async fn get_v2_from_write_resource(
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

                let concurrent_balance = object_data
                    .concurrent_fungible_asset_balance
                    .as_ref()
                    .map(|concurrent_fungible_asset_balance| {
                        concurrent_fungible_asset_balance.balance.value.clone()
                    });

                let coin_balance = Self {
                    txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: asset_type.clone(),
                    is_primary,
                    is_frozen: inner.frozen,
                    amount: concurrent_balance
                        .clone()
                        .unwrap_or_else(|| inner.balance.clone())
                        .to_string(),
                    block_timestamp: txn_timestamp,
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
                    txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: false,
                    amount: DEFAULT_AMOUNT_VALUE.clone(),
                    block_timestamp: txn_timestamp,
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
                    txn_version,
                    write_set_change_index,
                    storage_id: storage_id.clone(),
                    owner_address: owner_address.clone(),
                    asset_type: coin_type.clone(),
                    is_primary: true,
                    is_frozen: inner.frozen,
                    amount: inner.coin.value.clone().to_string(),
                    block_timestamp: txn_timestamp,
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
