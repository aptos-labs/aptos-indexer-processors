// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use bigdecimal::BigDecimal;
use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{
    current_delegated_staking_pool_balances, current_delegated_voter, current_delegator_balances,
    current_staking_pool_voter, delegated_staking_activities, delegated_staking_pool_balances,
    delegated_staking_pools, delegator_balances, proposal_votes,
};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Deserialize,
    Eq,
    FieldCount,
    Identifiable,
    Insertable,
    PartialEq,
    Serialize,
    Queryable,
    Clone,
)]
#[diesel(primary_key(delegator_address, delegation_pool_address))]
#[diesel(table_name = current_delegated_voter)]
pub struct CurrentDelegatedVoter {
    pub delegation_pool_address: String,
    pub delegator_address: String,
    pub table_handle: Option<String>,
    // vote_delegation table handle
    pub voter: Option<String>,
    pub pending_voter: Option<String>,
    // voter to be in the next lockup period
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = delegated_staking_activities)]
pub struct DelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(delegator_address, pool_address, pool_type))]
#[diesel(table_name = current_delegator_balances)]
pub struct CurrentDelegatorBalance {
    pub delegator_address: String,
    pub pool_address: String,
    pub pool_type: String,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
    pub shares: BigDecimal,
    pub parent_table_handle: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = delegator_balances)]
pub struct DelegatorBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub pool_type: String,
    pub table_handle: String,
    pub shares: BigDecimal,
    pub parent_table_handle: String,
    pub inserted_at: chrono::NaiveDateTime,
}

// Pools balances
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, staking_pool_address))]
#[diesel(table_name = delegated_staking_pool_balances)]
pub struct DelegatorPoolBalance {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub inserted_at: chrono::NaiveDateTime,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

// All pools w latest balances (really a more comprehensive version than DelegatorPool)
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_delegated_staking_pool_balances)]
pub struct CurrentDelegatorPoolBalance {
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
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
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_staking_pool_voter)]
pub struct CurrentStakingPoolVoter {
    pub staking_pool_address: String,
    pub voter_address: String,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
    pub operator_address: String,
}

// All pools
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = delegated_staking_pools)]
pub struct DelegatorPool {
    pub staking_pool_address: String,
    pub first_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}
