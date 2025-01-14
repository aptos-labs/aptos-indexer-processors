// Copyright © Aptos Foundation

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::stake_utils::StakeEvent,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{parse_timestamp, standardize_address, u64_to_bigdecimal},
    },
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawDelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
    pub block_timestamp: chrono::NaiveDateTime,
}
pub trait RawDelegatedStakingActivityConvertible {
    fn from_raw(raw: RawDelegatedStakingActivity) -> Self;
}

impl RawDelegatedStakingActivity {
    /// Pretty straightforward parsing from known delegated staking events
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut delegator_activities = vec![];
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["DelegatedStakingActivity"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(delegator_activities);
            },
        };

        let txn_version = transaction.version as i64;
        let events = match txn_data {
            TxnData::User(txn) => &txn.events,
            TxnData::BlockMetadata(txn) => &txn.events,
            TxnData::Validator(txn) => &txn.events,
            _ => return Ok(delegator_activities),
        };
        let block_timestamp = parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version);
        for (index, event) in events.iter().enumerate() {
            let event_index = index as i64;
            if let Some(staking_event) =
                StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
            {
                let activity = match staking_event {
                    StakeEvent::AddStakeEvent(inner) => RawDelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_added),
                        block_timestamp,
                    },
                    StakeEvent::UnlockStakeEvent(inner) => RawDelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_unlocked),
                        block_timestamp,
                    },
                    StakeEvent::WithdrawStakeEvent(inner) => RawDelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_withdrawn),
                        block_timestamp,
                    },
                    StakeEvent::ReactivateStakeEvent(inner) => RawDelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_reactivated),
                        block_timestamp,
                    },
                    StakeEvent::DistributeRewardsEvent(inner) => RawDelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: "".to_string(),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.rewards_amount),
                        block_timestamp,
                    },
                    _ => continue,
                };
                delegator_activities.push(activity);
            }
        }
        Ok(delegator_activities)
    }
}
