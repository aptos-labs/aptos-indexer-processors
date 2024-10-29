// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::stake_utils::StakeEvent;
use crate::{
    schema::{current_delegated_staking_pool_allowlist, delegated_staking_pool_allowlist},
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, util::standardize_address},
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(delegator_address, staking_pool_address))]
#[diesel(table_name = current_delegated_staking_pool_allowlist)]
pub struct CurrentDelegatedStakingPoolAllowlist {
    pub staking_pool_address: String,
    pub delegator_address: String,
    pub is_allowed: bool,
    last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, delegator_address, staking_pool_address))]
#[diesel(table_name = delegated_staking_pool_allowlist)]
pub struct DelegatedStakingPoolAllowlist {
    pub staking_pool_address: String,
    pub delegator_address: String,
    pub is_allowed: bool,
    transaction_version: i64,
}

impl CurrentDelegatedStakingPoolAllowlist {
    pub fn from_transaction(
        transaction: &Transaction,
    ) -> anyhow::Result<AHashMap<(String, String), Self>> {
        let mut delegated_staking_pool_allowlist = AHashMap::new();
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["DelegatedStakingPoolAllowlist"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(delegated_staking_pool_allowlist);
            },
        };
        let txn_version = transaction.version as i64;

        if let TxnData::User(user_txn) = txn_data {
            for event in &user_txn.events {
                if let Some(StakeEvent::AllowlistDelegatorEvent(ev)) =
                    StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
                {
                    let current_delegated_staking_pool_allowlist =
                        CurrentDelegatedStakingPoolAllowlist {
                            last_transaction_version: txn_version,
                            staking_pool_address: standardize_address(&ev.pool_address),
                            delegator_address: standardize_address(&ev.delegator_address),
                            is_allowed: ev.enabled,
                        };
                    delegated_staking_pool_allowlist.insert(
                        (
                            current_delegated_staking_pool_allowlist
                                .delegator_address
                                .clone(),
                            current_delegated_staking_pool_allowlist
                                .staking_pool_address
                                .clone(),
                        ),
                        current_delegated_staking_pool_allowlist,
                    );
                }
            }
        }
        Ok(delegated_staking_pool_allowlist)
    }
}

impl DelegatedStakingPoolAllowlist {
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut delegated_staking_pool_allowlist = vec![];
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["DelegatedStakingPoolAllowlist"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(delegated_staking_pool_allowlist);
            },
        };
        let txn_version = transaction.version as i64;

        if let TxnData::User(user_txn) = txn_data {
            for event in &user_txn.events {
                if let Some(StakeEvent::AllowlistDelegatorEvent(ev)) =
                    StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
                {
                    delegated_staking_pool_allowlist.push(Self {
                        transaction_version: txn_version,
                        staking_pool_address: standardize_address(&ev.pool_address),
                        delegator_address: standardize_address(&ev.delegator_address),
                        is_allowed: ev.enabled,
                    });
                }
            }
        }
        Ok(delegated_staking_pool_allowlist)
    }
}
