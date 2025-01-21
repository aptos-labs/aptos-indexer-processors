// Copyright © Aptos Foundation

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
use crate::{
    db::common::models::stake_models::{
        delegator_balances::{
            RawCurrentDelegatorBalance, RawCurrentDelegatorBalanceConvertible, RawDelegatorBalance,
            RawDelegatorBalanceConvertible,
        },
        delegator_pools::{RawDelegatorPoolBalanceMetadata, RawPoolBalanceMetadata},
    },
    schema::{current_delegator_balances, delegator_balances},
};
use ahash::AHashMap;
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub type TableHandle = String;
pub type Address = String;
pub type ShareToStakingPoolMapping = AHashMap<TableHandle, RawDelegatorPoolBalanceMetadata>;
pub type ShareToPoolMapping = AHashMap<TableHandle, RawPoolBalanceMetadata>;
pub type CurrentDelegatorBalancePK = (Address, Address, String);
pub type CurrentDelegatorBalanceMap = AHashMap<CurrentDelegatorBalancePK, CurrentDelegatorBalance>;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(delegator_address, pool_address, pool_type))]
#[diesel(table_name = current_delegator_balances)]
pub struct CurrentDelegatorBalance {
    pub delegator_address: String,
    pub pool_address: String,
    pub pool_type: String,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub shares: BigDecimal,
    pub parent_table_handle: String,
}

impl RawCurrentDelegatorBalanceConvertible for CurrentDelegatorBalance {
    fn from_raw(raw: RawCurrentDelegatorBalance) -> Self {
        Self {
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            pool_type: raw.pool_type,
            table_handle: raw.table_handle,
            last_transaction_version: raw.last_transaction_version,
            shares: raw.shares,
            parent_table_handle: raw.parent_table_handle,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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
}

impl RawDelegatorBalanceConvertible for DelegatorBalance {
    fn from_raw(raw: RawDelegatorBalance) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            write_set_change_index: raw.write_set_change_index,
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            pool_type: raw.pool_type,
            table_handle: raw.table_handle,
            shares: raw.shares,
            parent_table_handle: raw.parent_table_handle,
        }
    }
}
