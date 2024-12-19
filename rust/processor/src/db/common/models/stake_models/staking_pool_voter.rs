// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::stake_utils::StakeResource,
    utils::util::{parse_timestamp, standardize_address},
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
type StakingPoolAddress = String;
pub type StakingPoolRawVoterMap = AHashMap<StakingPoolAddress, RawCurrentStakingPoolVoter>;

pub struct RawCurrentStakingPoolVoter {
    pub staking_pool_address: String,
    pub voter_address: String,
    pub last_transaction_version: i64,
    pub operator_address: String,
    pub block_timestamp: chrono::NaiveDateTime,
}

pub trait RawCurrentStakingPoolVoterConvertible {
    fn from_raw(raw: RawCurrentStakingPoolVoter) -> Self;
}

impl RawCurrentStakingPoolVoter {
    pub fn from_transaction(transaction: &Transaction) -> anyhow::Result<StakingPoolRawVoterMap> {
        let mut staking_pool_voters = AHashMap::new();

        let txn_version = transaction.version as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let block_timestamp = parse_timestamp(timestamp, txn_version);
        for wsc in &transaction.info.as_ref().unwrap().changes {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some(StakeResource::StakePool(inner)) =
                    StakeResource::from_write_resource(write_resource, txn_version)?
                {
                    let staking_pool_address =
                        standardize_address(&write_resource.address.to_string());
                    staking_pool_voters.insert(
                        staking_pool_address.clone(),
                        Self {
                            staking_pool_address,
                            voter_address: inner.get_delegated_voter(),
                            last_transaction_version: txn_version,
                            operator_address: inner.get_operator_address(),
                            block_timestamp,
                        },
                    );
                }
            }
        }

        Ok(staking_pool_voters)
    }
}
