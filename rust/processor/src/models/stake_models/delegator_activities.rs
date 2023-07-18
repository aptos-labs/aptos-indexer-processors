// Copyright © Aptos Foundation

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::stake_utils::StakeEvent;
use crate::{
    schema::delegated_staking_activities,
    utils::util::{standardize_address, u64_to_bigdecimal},
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = delegated_staking_activities)]
pub struct DelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
}

impl DelegatedStakingActivity {
    /// Pretty straightforward parsing from known delegated staking events
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut delegator_activities = vec![];
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");
        let txn_version = transaction.version as i64;
        let events = match txn_data {
            TxnData::User(txn) => &txn.events,
            TxnData::BlockMetadata(txn) => &txn.events,
            _ => return Ok(delegator_activities),
        };
        for (index, event) in events.iter().enumerate() {
            let event_index = index as i64;
            if let Some(staking_event) =
                StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
            {
                let activity = match staking_event {
                    StakeEvent::AddStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_added),
                    },
                    StakeEvent::UnlockStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_unlocked),
                    },
                    StakeEvent::WithdrawStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_withdrawn),
                    },
                    StakeEvent::ReactivateStakeEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: standardize_address(&inner.delegator_address),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.amount_reactivated),
                    },
                    StakeEvent::DistributeRewardsEvent(inner) => DelegatedStakingActivity {
                        transaction_version: txn_version,
                        event_index,
                        delegator_address: "".to_string(),
                        pool_address: standardize_address(&inner.pool_address),
                        event_type: event.type_str.clone(),
                        amount: u64_to_bigdecimal(inner.rewards_amount),
                    },
                    _ => continue,
                };
                delegator_activities.push(activity);
            }
        }
        Ok(delegator_activities)
    }
}
