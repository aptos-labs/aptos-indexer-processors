// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::stake_utils::StakeEvent;
use crate::{
    models::should_skip,
    schema::proposal_votes,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{parse_timestamp, standardize_address},
    },
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, proposal_id, voter_address))]
#[diesel(table_name = proposal_votes)]
pub struct ProposalVote {
    pub transaction_version: i64,
    pub proposal_id: i64,
    pub voter_address: String,
    pub staking_pool_address: String,
    pub num_votes: BigDecimal,
    pub should_pass: bool,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl ProposalVote {
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<Vec<Self>> {
        let mut proposal_votes = vec![];
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["ProposalVote"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return Ok(proposal_votes);
            },
        };
        let txn_version = transaction.version as i64;

        if let TxnData::User(user_txn) = txn_data {
            for (index, event) in user_txn.events.iter().enumerate() {
                if let Some(StakeEvent::GovernanceVoteEvent(ev)) =
                    StakeEvent::from_event(event.type_str.as_str(), &event.data, txn_version)?
                {
                    if should_skip(index, event, &user_txn.events) {
                        continue;
                    };
                    proposal_votes.push(Self {
                        transaction_version: txn_version,
                        proposal_id: ev.proposal_id as i64,
                        voter_address: standardize_address(&ev.voter),
                        staking_pool_address: standardize_address(&ev.stake_pool),
                        num_votes: ev.num_votes.clone(),
                        should_pass: ev.should_pass,
                        transaction_timestamp: parse_timestamp(
                            transaction.timestamp.as_ref().unwrap(),
                            txn_version,
                        ),
                    });
                }
            }
        }
        Ok(proposal_votes)
    }
}
