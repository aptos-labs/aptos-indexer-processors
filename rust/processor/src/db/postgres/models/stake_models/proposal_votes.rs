// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::proposal_voters::{
        RawProposalVote, RawProposalVoteConvertible,
    },
    schema::proposal_votes,
};
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

impl RawProposalVoteConvertible for ProposalVote {
    fn from_raw(raw: RawProposalVote) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            proposal_id: raw.proposal_id,
            voter_address: raw.voter_address,
            staking_pool_address: raw.staking_pool_address,
            num_votes: raw.num_votes,
            should_pass: raw.should_pass,
            transaction_timestamp: raw.transaction_timestamp,
        }
    }
}
