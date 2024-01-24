// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    coin_balances::{CoinBalance, CurrentCoinBalance},
    coin_infos::CoinInfo,
    coin_supply::CoinSupply,
    coin_utils::{CoinEvent, EventGuidResource},
};
use crate::{
    models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::{
                CoinType, CurrentCoinBalancePK, EventToCoinType, BURN_GAS_EVENT_CREATION_NUM,
                BURN_GAS_EVENT_INDEX, GAS_FEE_EVENT,
            },
            v2_fungible_asset_utils::FeeStatement,
        },
        user_transactions_models::signatures::Signature,
    },
    processors::coin_processor::APTOS_COIN_TYPE_STR,
    schema::coin_activities,
    utils::util::{get_entry_function_from_user_request, standardize_address, u64_to_bigdecimal},
};
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, Event as EventPB,
    Transaction as TransactionPB, TransactionInfo, UserTransactionRequest,
};
use bigdecimal::{BigDecimal, Zero};
use chrono::NaiveDateTime;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Clone)]
#[diesel(primary_key(
    transaction_version,
    event_account_address,
    event_creation_number,
    event_sequence_number
))]
#[diesel(table_name = coin_activities)]
pub struct CoinActivity {
    pub transaction_version: i64,
    pub event_account_address: String,
    pub event_creation_number: i64,
    pub event_sequence_number: i64,
    pub owner_address: String,
    pub coin_type: String,
    pub amount: BigDecimal,
    pub activity_type: String,
    pub is_gas_fee: bool,
    pub is_transaction_success: bool,
    pub entry_function_id_str: Option<String>,
    pub block_height: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub event_index: Option<i64>,
    pub gas_fee_payer_address: Option<String>,
    pub storage_refund_amount: BigDecimal,
}

impl CoinActivity {
    /// There are different objects containing different information about balances and coins.
    /// Events: Withdraw and Deposit event containing amounts. There is no coin type so we need to get that from Resources. (from event guid)
    /// CoinInfo Resource: Contains name, symbol, decimals and supply. (if supply is aggregator, however, actual supply amount will live in a separate table)
    /// CoinStore Resource: Contains owner address and coin type information used to complete events
    /// Aggregator Table Item: Contains current supply of a coin
    /// Note, we're not currently tracking supply
    pub fn from_transaction(
        transaction: &TransactionPB,
    ) -> (
        Vec<Self>,
        Vec<CoinBalance>,
        HashMap<CoinType, CoinInfo>,
        HashMap<CurrentCoinBalancePK, CurrentCoinBalance>,
        Vec<CoinSupply>,
    ) {
        // All the items we want to track
        let mut coin_activities = Vec::new();
        let mut coin_balances = Vec::new();
        let mut coin_infos: HashMap<CoinType, CoinInfo> = HashMap::new();
        let mut current_coin_balances: HashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
            HashMap::new();
        // This will help us get the coin type when we see coin deposit/withdraw events for coin activities
        let mut all_event_to_coin_type: EventToCoinType = HashMap::new();
        let mut all_coin_supply = Vec::new();

        // Extracts events and user request from genesis and user transactions. Other transactions won't have coin events
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");
        let (events, maybe_user_request): (&Vec<EventPB>, Option<&UserTransactionRequest>) =
            match txn_data {
                TxnData::Genesis(inner) => (&inner.events, None),
                TxnData::User(inner) => (&inner.events, inner.request.as_ref()),
                _ => return Default::default(),
            };

        // The rest are fields common to all transactions
        let txn_version = transaction.version as i64;
        let txn_epoch = transaction.epoch as i64;
        let block_height = transaction.block_height as i64;
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let txn_timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");

        // Handling gas first
        let mut entry_function_id_str = None;
        if let Some(user_request) = maybe_user_request {
            let fee_statement = events.iter().find_map(|event| {
                let event_type = event.type_str.as_str();
                FeeStatement::from_event(event_type, &event.data, txn_version)
            });

            entry_function_id_str = get_entry_function_from_user_request(user_request);
            coin_activities.push(Self::get_gas_event(
                transaction_info,
                user_request,
                &entry_function_id_str,
                txn_version,
                txn_timestamp,
                block_height,
                fee_statement,
            ));
        }

        // Need coin info from move resources
        for wsc in &transaction_info.changes {
            let (maybe_coin_info, maybe_coin_balance_data) =
                if let WriteSetChangeEnum::WriteResource(write_resource) =
                    &wsc.change.as_ref().unwrap()
                {
                    (
                        CoinInfo::from_write_resource(write_resource, txn_version, txn_timestamp)
                            .unwrap(),
                        CoinBalance::from_write_resource(
                            write_resource,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap(),
                    )
                } else {
                    (None, None)
                };

            let maybe_coin_supply = if let WriteSetChangeEnum::WriteTableItem(table_item) =
                wsc.change.as_ref().unwrap()
            {
                CoinSupply::from_write_table_item(table_item, txn_version, txn_timestamp, txn_epoch)
                    .unwrap()
            } else {
                None
            };

            if let Some(coin_info) = maybe_coin_info {
                coin_infos.insert(coin_info.coin_type.clone(), coin_info);
            }
            if let Some((coin_balance, current_coin_balance, event_to_coin_type)) =
                maybe_coin_balance_data
            {
                current_coin_balances.insert(
                    (
                        coin_balance.owner_address.clone(),
                        coin_balance.coin_type.clone(),
                    ),
                    current_coin_balance,
                );
                coin_balances.push(coin_balance);
                all_event_to_coin_type.extend(event_to_coin_type);
            }
            if let Some(coin_supply) = maybe_coin_supply {
                all_coin_supply.push(coin_supply);
            }
        }
        for (index, event) in events.iter().enumerate() {
            let event_type = event.type_str.clone();
            if let Some(parsed_event) =
                CoinEvent::from_event(event_type.as_str(), &event.data, txn_version).unwrap()
            {
                coin_activities.push(Self::from_parsed_event(
                    &event_type,
                    event,
                    &parsed_event,
                    txn_version,
                    &all_event_to_coin_type,
                    block_height,
                    &entry_function_id_str,
                    txn_timestamp,
                    index as i64,
                ));
            };
        }
        (
            coin_activities,
            coin_balances,
            coin_infos,
            current_coin_balances,
            all_coin_supply,
        )
    }

    fn from_parsed_event(
        event_type: &str,
        event: &EventPB,
        coin_event: &CoinEvent,
        txn_version: i64,
        event_to_coin_type: &EventToCoinType,
        block_height: i64,
        entry_function_id_str: &Option<String>,
        transaction_timestamp: chrono::NaiveDateTime,
        event_index: i64,
    ) -> Self {
        let amount = match coin_event {
            CoinEvent::WithdrawCoinEvent(inner) => inner.amount.clone(),
            CoinEvent::DepositCoinEvent(inner) => inner.amount.clone(),
        };
        let event_move_guid = EventGuidResource {
            addr: standardize_address(event.key.as_ref().unwrap().account_address.as_str()),
            creation_num: event.key.as_ref().unwrap().creation_number as i64,
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

        Self {
            transaction_version: txn_version,
            event_account_address: standardize_address(
                &event.key.as_ref().unwrap().account_address,
            ),
            event_creation_number: event.key.as_ref().unwrap().creation_number as i64,
            event_sequence_number: event.sequence_number as i64,
            owner_address: standardize_address(&event.key.as_ref().unwrap().account_address),
            coin_type,
            amount,
            activity_type: event_type.to_string(),
            is_gas_fee: false,
            is_transaction_success: true,
            entry_function_id_str: entry_function_id_str.clone(),
            block_height,
            transaction_timestamp,
            event_index: Some(event_index),
            gas_fee_payer_address: None,
            storage_refund_amount: BigDecimal::zero(),
        }
    }

    pub fn get_gas_event(
        txn_info: &TransactionInfo,
        user_transaction_request: &UserTransactionRequest,
        entry_function_id_str: &Option<String>,
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        block_height: i64,
        fee_statement: Option<FeeStatement>,
    ) -> Self {
        let aptos_coin_burned =
            BigDecimal::from(txn_info.gas_used * user_transaction_request.gas_unit_price);
        let signature = user_transaction_request
            .signature
            .as_ref()
            .unwrap_or_else(|| {
                tracing::error!(
                    transaction_version = transaction_version,
                    "User transaction must have signature"
                );
                panic!("User transaction must have signature")
            });
        let gas_fee_payer_address =
            Signature::get_fee_payer_address(signature, transaction_version);

        Self {
            transaction_version,
            event_account_address: standardize_address(
                &user_transaction_request.sender.to_string(),
            ),
            event_creation_number: BURN_GAS_EVENT_CREATION_NUM,
            event_sequence_number: user_transaction_request.sequence_number as i64,
            owner_address: standardize_address(&user_transaction_request.sender.to_string()),
            coin_type: APTOS_COIN_TYPE_STR.to_string(),
            amount: aptos_coin_burned,
            activity_type: GAS_FEE_EVENT.to_string(),
            is_gas_fee: true,
            is_transaction_success: txn_info.success,
            entry_function_id_str: entry_function_id_str.clone(),
            block_height,
            transaction_timestamp,
            event_index: Some(BURN_GAS_EVENT_INDEX),
            gas_fee_payer_address,
            storage_refund_amount: fee_statement
                .map(|fs| u64_to_bigdecimal(fs.storage_fee_refund_octas))
                .unwrap_or(BigDecimal::zero()),
        }
    }
}
