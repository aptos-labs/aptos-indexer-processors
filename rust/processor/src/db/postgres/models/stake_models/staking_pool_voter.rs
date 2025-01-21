// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::staking_pool_voter::{
        RawCurrentStakingPoolVoter, RawCurrentStakingPoolVoterConvertible,
    },
    schema::current_staking_pool_voter,
};
use ahash::AHashMap;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type StakingPoolAddress = String;
pub type StakingPoolVoterMap = AHashMap<StakingPoolAddress, CurrentStakingPoolVoter>;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_staking_pool_voter)]
pub struct CurrentStakingPoolVoter {
    pub staking_pool_address: String,
    pub voter_address: String,
    pub last_transaction_version: i64,
    pub operator_address: String,
}

impl RawCurrentStakingPoolVoterConvertible for CurrentStakingPoolVoter {
    fn from_raw(raw: RawCurrentStakingPoolVoter) -> Self {
        Self {
            staking_pool_address: raw.staking_pool_address,
            voter_address: raw.voter_address,
            last_transaction_version: raw.last_transaction_version,
            operator_address: raw.operator_address,
        }
    }
}
