// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::delegator_pools::{
        DelegatorPool, RawCurrentDelegatorPoolBalance, RawCurrentDelegatorPoolBalanceConvertible,
        RawDelegatorPoolBalance, RawDelegatorPoolBalanceConvertible,
        RawDelegatorPoolBalanceMetadata, RawDelegatorPoolBalanceMetadataConvertible,
        RawPoolBalanceMetadata, RawPoolBalanceMetadataConvertible,
    },
    schema::{current_delegated_staking_pool_balances, delegated_staking_pool_balances},
};
use ahash::AHashMap;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
type StakingPoolAddress = String;
pub type DelegatorPoolMap = AHashMap<StakingPoolAddress, DelegatorPool>;
pub type DelegatorPoolBalanceMap = AHashMap<StakingPoolAddress, RawCurrentDelegatorPoolBalance>;

// Metadata to fill pool balances and delegator balance
#[derive(Debug, Deserialize, Serialize)]
pub struct DelegatorPoolBalanceMetadata {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub active_share_table_handle: String,
    pub inactive_share_table_handle: String,
}

impl RawDelegatorPoolBalanceMetadataConvertible for DelegatorPoolBalanceMetadata {
    fn from_raw(raw: RawDelegatorPoolBalanceMetadata) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            staking_pool_address: raw.staking_pool_address,
            total_coins: raw.total_coins,
            total_shares: raw.total_shares,
            scaling_factor: raw.scaling_factor,
            operator_commission_percentage: raw.operator_commission_percentage,
            active_share_table_handle: raw.active_share_table_handle,
            inactive_share_table_handle: raw.inactive_share_table_handle,
        }
    }
}

// Similar metadata but specifically for 0x1::pool_u64_unbound::Pool
#[derive(Debug, Deserialize, Serialize)]
pub struct PoolBalanceMetadata {
    pub transaction_version: i64,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub scaling_factor: BigDecimal,
    pub shares_table_handle: String,
    pub parent_table_handle: String,
}

impl RawPoolBalanceMetadataConvertible for PoolBalanceMetadata {
    fn from_raw(raw: RawPoolBalanceMetadata) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            total_coins: raw.total_coins,
            total_shares: raw.total_shares,
            scaling_factor: raw.scaling_factor,
            shares_table_handle: raw.shares_table_handle,
            parent_table_handle: raw.parent_table_handle,
        }
    }
}

// Pools balances
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, staking_pool_address))]
#[diesel(table_name = delegated_staking_pool_balances)]
pub struct DelegatorPoolBalance {
    pub transaction_version: i64,
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

impl RawDelegatorPoolBalanceConvertible for DelegatorPoolBalance {
    fn from_raw(raw: RawDelegatorPoolBalance) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            staking_pool_address: raw.staking_pool_address,
            total_coins: raw.total_coins,
            total_shares: raw.total_shares,
            operator_commission_percentage: raw.operator_commission_percentage,
            inactive_table_handle: raw.inactive_table_handle,
            active_table_handle: raw.active_table_handle,
        }
    }
}

// All pools w latest balances (really a more comprehensive version than DelegatorPool)
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(staking_pool_address))]
#[diesel(table_name = current_delegated_staking_pool_balances)]
pub struct CurrentDelegatorPoolBalance {
    pub staking_pool_address: String,
    pub total_coins: BigDecimal,
    pub total_shares: BigDecimal,
    pub last_transaction_version: i64,
    pub operator_commission_percentage: BigDecimal,
    pub inactive_table_handle: String,
    pub active_table_handle: String,
}

impl RawCurrentDelegatorPoolBalanceConvertible for CurrentDelegatorPoolBalance {
    fn from_raw(raw: RawCurrentDelegatorPoolBalance) -> Self {
        Self {
            staking_pool_address: raw.staking_pool_address,
            total_coins: raw.total_coins,
            total_shares: raw.total_shares,
            last_transaction_version: raw.last_transaction_version,
            operator_commission_percentage: raw.operator_commission_percentage,
            inactive_table_handle: raw.inactive_table_handle,
            active_table_handle: raw.active_table_handle,
        }
    }
}
