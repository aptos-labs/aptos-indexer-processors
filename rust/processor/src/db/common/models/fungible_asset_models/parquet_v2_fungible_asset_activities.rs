// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_fungible_asset_utils::{FeeStatement, FungibleAssetEvent};
use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        coin_models::{
            coin_activities::CoinActivity,
            coin_utils::{CoinEvent, CoinInfoType, EventGuidResource},
        },
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2_models::v2_token_utils::TokenStandard,
    },
    utils::util::{bigdecimal_to_u64, standardize_address},
};
use ahash::AHashMap;
use allocative::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, TransactionInfo, UserTransactionRequest};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

pub const GAS_FEE_EVENT: &str = "0x1::aptos_coin::GasFeeEvent";
// We will never have a negative number on chain so this will avoid collision in postgres
pub const BURN_GAS_EVENT_CREATION_NUM: i64 = -1;
pub const BURN_GAS_EVENT_INDEX: i64 = -1;

pub type OwnerAddress = String;
pub type CoinType = String;
// Primary key of the current_coin_balances table, i.e. (owner_address, coin_type)
pub type CurrentCoinBalancePK = (OwnerAddress, CoinType);
pub type EventToCoinType = AHashMap<EventGuidResource, CoinType>;

/// TODO: This is just a copy of v2_fungible_asset_activities.rs. We should unify the 2 implementations
/// and have parquet as an output.
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct FungibleAssetActivity {
    pub txn_version: i64,
    pub event_index: i64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub asset_type: Option<String>,
    pub is_frozen: Option<bool>,
    pub amount: Option<String>, // it is a string representation of the u128
    pub event_type: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub storage_refund_octa: u64,
}

impl NamedTable for FungibleAssetActivity {
    const TABLE_NAME: &'static str = "fungible_asset_activities";
}

impl HasVersion for FungibleAssetActivity {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for FungibleAssetActivity {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl FungibleAssetActivity {
    pub fn get_v2_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        txn_timestamp: chrono::NaiveDateTime,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        object_aggregated_data_mapping: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.clone();
        if let Some(fa_event) =
            &FungibleAssetEvent::from_event(event_type.as_str(), &event.data, txn_version)?
        {
            let (storage_id, is_frozen, amount) = match fa_event {
                FungibleAssetEvent::WithdrawEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    None,
                    Some(inner.amount.to_string()),
                ),
                FungibleAssetEvent::DepositEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    None,
                    Some(inner.amount.to_string()),
                ),
                FungibleAssetEvent::FrozenEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    Some(inner.frozen),
                    None,
                ),
                FungibleAssetEvent::WithdrawEventV2(inner) => (
                    standardize_address(&inner.store),
                    None,
                    Some(inner.amount.to_string()),
                ),
                FungibleAssetEvent::DepositEventV2(inner) => (
                    standardize_address(&inner.store),
                    None,
                    Some(inner.amount.to_string()),
                ),
                FungibleAssetEvent::FrozenEventV2(inner) => {
                    (standardize_address(&inner.store), Some(inner.frozen), None)
                },
            };

            // The event account address will also help us find fungible store which tells us where to find
            // the metadata
            let maybe_object_metadata = object_aggregated_data_mapping.get(&storage_id);
            // The ObjectCore might not exist in the transaction if the object got deleted
            let maybe_owner_address = maybe_object_metadata
                .map(|metadata| &metadata.object.object_core)
                .map(|object_core| object_core.get_owner_address());
            // The FungibleStore might not exist in the transaction if it's a secondary store that got burnt
            let maybe_asset_type = maybe_object_metadata
                .and_then(|metadata| metadata.fungible_asset_store.as_ref())
                .map(|fa| fa.metadata.get_reference_address());

            return Ok(Some(Self {
                txn_version,
                event_index,
                owner_address: maybe_owner_address,
                storage_id: storage_id.clone(),
                asset_type: maybe_asset_type,
                is_frozen,
                amount,
                event_type: event_type.clone(),
                is_gas_fee: false,
                gas_fee_payer_address: None,
                is_transaction_success: true,
                entry_function_id_str: entry_function_id_str.clone(),
                block_height,
                token_standard: TokenStandard::V2.to_string(),
                block_timestamp: txn_timestamp,
                storage_refund_octa: 0,
            }));
        }
        Ok(None)
    }

    pub fn get_v1_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
        entry_function_id_str: &Option<String>,
        event_to_coin_type: &EventToCoinType,
        event_index: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) =
            CoinEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
        {
            let (owner_address, amount, coin_type_option) = match inner {
                CoinEvent::WithdrawCoinEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    inner.amount.to_string(),
                    None,
                ),
                CoinEvent::DepositCoinEvent(inner) => (
                    standardize_address(&event.key.as_ref().unwrap().account_address),
                    inner.amount.to_string(),
                    None,
                ),
            };
            let coin_type = if let Some(coin_type) = coin_type_option {
                coin_type
            } else {
                let event_key = event.key.as_ref().context("event must have a key")?;
                let event_move_guid = EventGuidResource {
                    addr: standardize_address(event_key.account_address.as_str()),
                    creation_num: event_key.creation_number as i64,
                };
                // Given this mapping only contains coin type < 1000 length, we should not assume that the mapping exists.
                // If it doesn't exist, skip.
                match event_to_coin_type.get(&event_move_guid) {
                    Some(coin_type) => coin_type.clone(),
                    None => {
                        tracing::warn!(
                        "Could not find event in resources (CoinStore), version: {}, event guid: {:?}, mapping: {:?}",
                        txn_version, event_move_guid, event_to_coin_type
                    );
                        return Ok(None);
                    },
                }
            };

            let storage_id =
                CoinInfoType::get_storage_id(coin_type.as_str(), owner_address.as_str());

            Ok(Some(Self {
                txn_version,
                event_index,
                owner_address: Some(owner_address),
                storage_id,
                asset_type: Some(coin_type),
                is_frozen: None,
                amount: Some(amount),
                event_type: event.type_str.clone(),
                is_gas_fee: false,
                gas_fee_payer_address: None,
                is_transaction_success: true,
                entry_function_id_str: entry_function_id_str.clone(),
                block_height,
                token_standard: TokenStandard::V1.to_string(),
                block_timestamp,
                storage_refund_octa: 0,
            }))
        } else {
            Ok(None)
        }
    }

    /// Artificially creates a gas event. If it's a fee payer, still show gas event to the sender
    /// but with an extra field to indicate the fee payer.
    pub fn get_gas_event(
        txn_info: &TransactionInfo,
        user_transaction_request: &UserTransactionRequest,
        entry_function_id_str: &Option<String>,
        txn_version: i64,
        block_timestamp: chrono::NaiveDateTime,
        block_height: i64,
        fee_statement: Option<FeeStatement>,
    ) -> Self {
        let v1_activity = CoinActivity::get_gas_event(
            txn_info,
            user_transaction_request,
            entry_function_id_str,
            txn_version,
            block_timestamp,
            block_height,
            fee_statement,
        );
        let storage_id = CoinInfoType::get_storage_id(
            v1_activity.coin_type.as_str(),
            v1_activity.owner_address.as_str(),
        );
        Self {
            txn_version,
            event_index: v1_activity.event_index.unwrap(),
            owner_address: Some(v1_activity.owner_address),
            storage_id,
            asset_type: Some(v1_activity.coin_type),
            is_frozen: None,
            amount: Some(v1_activity.amount.to_string()),
            event_type: v1_activity.activity_type,
            is_gas_fee: v1_activity.is_gas_fee,
            gas_fee_payer_address: v1_activity.gas_fee_payer_address,
            is_transaction_success: v1_activity.is_transaction_success,
            entry_function_id_str: v1_activity.entry_function_id_str,
            block_height,
            token_standard: TokenStandard::V1.to_string(),
            block_timestamp,
            storage_refund_octa: bigdecimal_to_u64(&v1_activity.storage_refund_amount),
        }
    }
}
