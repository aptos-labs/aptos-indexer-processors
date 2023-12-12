// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_fungible_asset_utils::{FeeStatement, FungibleAssetEvent},
    v2_fungible_metadata::FungibleAssetMetadataModel,
};
use crate::{
    models::{
        coin_models::{
            coin_activities::CoinActivity,
            coin_utils::{CoinEvent, CoinInfoType, EventGuidResource},
        },
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_activities,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, TransactionInfo, UserTransactionRequest};
use bigdecimal::{BigDecimal, Zero};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const GAS_FEE_EVENT: &str = "0x1::aptos_coin::GasFeeEvent";
// We will never have a negative number on chain so this will avoid collision in postgres
pub const BURN_GAS_EVENT_CREATION_NUM: i64 = -1;
pub const BURN_GAS_EVENT_INDEX: i64 = -1;

pub type OwnerAddress = String;
pub type CoinType = String;
// Primary key of the current_coin_balances table, i.e. (owner_address, coin_type)
pub type CurrentCoinBalancePK = (OwnerAddress, CoinType);
pub type EventToCoinType = HashMap<EventGuidResource, CoinType>;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = fungible_asset_activities)]
pub struct FungibleAssetActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub owner_address: String,
    pub storage_id: String,
    pub asset_type: String,
    pub is_frozen: Option<bool>,
    pub amount: Option<BigDecimal>,
    pub type_: String,
    pub is_gas_fee: bool,
    pub gas_fee_payer_address: Option<String>,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub storage_refund_amount: BigDecimal,
}

impl FungibleAssetActivity {
    pub async fn get_v2_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        txn_timestamp: chrono::NaiveDateTime,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        fungible_asset_metadata: &ObjectAggregatedDataMapping,
        conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.clone();
        if let Some(fa_event) =
            &FungibleAssetEvent::from_event(event_type.as_str(), &event.data, txn_version)?
        {
            let storage_id = standardize_address(&event.key.as_ref().unwrap().account_address);

            // The event account address will also help us find fungible store which tells us where to find
            // the metadata
            if let Some(metadata) = fungible_asset_metadata.get(&storage_id) {
                let object_core = &metadata.object.object_core;
                let fungible_asset = metadata.fungible_asset_store.as_ref().unwrap();
                let asset_type = fungible_asset.metadata.get_reference_address();
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

                let (is_frozen, amount) = match fa_event {
                    FungibleAssetEvent::WithdrawEvent(inner) => (None, Some(inner.amount.clone())),
                    FungibleAssetEvent::DepositEvent(inner) => (None, Some(inner.amount.clone())),
                    FungibleAssetEvent::FrozenEvent(inner) => (Some(inner.frozen), None),
                };

                return Ok(Some(Self {
                    transaction_version: txn_version,
                    event_index,
                    owner_address: object_core.get_owner_address(),
                    storage_id: storage_id.clone(),
                    asset_type: asset_type.clone(),
                    is_frozen,
                    amount,
                    type_: event_type.clone(),
                    is_gas_fee: false,
                    gas_fee_payer_address: None,
                    is_transaction_success: true,
                    entry_function_id_str: entry_function_id_str.clone(),
                    block_height,
                    token_standard: TokenStandard::V2.to_string(),
                    transaction_timestamp: txn_timestamp,
                    storage_refund_amount: BigDecimal::zero(),
                }));
            }
        }
        Ok(None)
    }

    pub fn get_v1_from_event(
        event: &Event,
        txn_version: i64,
        block_height: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        entry_function_id_str: &Option<String>,
        event_to_coin_type: &EventToCoinType,
        event_index: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) =
            CoinEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
        {
            let amount = match inner {
                CoinEvent::WithdrawCoinEvent(inner) => inner.amount,
                CoinEvent::DepositCoinEvent(inner) => inner.amount,
            };
            let event_key = event.key.as_ref().context("event must have a key")?;
            let event_move_guid = EventGuidResource {
                addr: standardize_address(event_key.account_address.as_str()),
                creation_num: event_key.creation_number as i64,
            };
            let coin_type =
                    event_to_coin_type
                        .get(&event_move_guid)
                        .unwrap_or_else(|| {
                            panic!(
                                "Could not find event in resources (CoinStore), version: {}, event guid: {:?}, mapping: {:?}",
                                txn_version, event_move_guid, event_to_coin_type
                            )
                        }).clone();
            let storage_id =
                CoinInfoType::get_storage_id(coin_type.as_str(), event_move_guid.addr.as_str());
            Ok(Some(Self {
                transaction_version: txn_version,
                event_index,
                owner_address: event_move_guid.addr,
                storage_id,
                asset_type: coin_type,
                is_frozen: None,
                amount: Some(amount),
                type_: event.type_str.clone(),
                is_gas_fee: false,
                gas_fee_payer_address: None,
                is_transaction_success: true,
                entry_function_id_str: entry_function_id_str.clone(),
                block_height,
                token_standard: TokenStandard::V1.to_string(),
                transaction_timestamp,
                storage_refund_amount: BigDecimal::zero(),
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
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        block_height: i64,
        fee_statement: Option<FeeStatement>,
    ) -> Self {
        let v1_activity = CoinActivity::get_gas_event(
            txn_info,
            user_transaction_request,
            entry_function_id_str,
            transaction_version,
            transaction_timestamp,
            block_height,
            fee_statement,
        );
        let storage_id = CoinInfoType::get_storage_id(
            v1_activity.coin_type.as_str(),
            v1_activity.owner_address.as_str(),
        );
        Self {
            transaction_version,
            event_index: v1_activity.event_index.unwrap(),
            owner_address: v1_activity.owner_address,
            storage_id,
            asset_type: v1_activity.coin_type,
            is_frozen: None,
            amount: Some(v1_activity.amount),
            type_: v1_activity.activity_type,
            is_gas_fee: v1_activity.is_gas_fee,
            gas_fee_payer_address: v1_activity.gas_fee_payer_address,
            is_transaction_success: v1_activity.is_transaction_success,
            entry_function_id_str: v1_activity.entry_function_id_str,
            block_height,
            token_standard: TokenStandard::V1.to_string(),
            transaction_timestamp,
            storage_refund_amount: v1_activity.storage_refund_amount,
        }
    }
}
