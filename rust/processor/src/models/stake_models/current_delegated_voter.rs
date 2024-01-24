// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::{
    delegator_balances::{CurrentDelegatorBalance, ShareToStakingPoolMapping},
    stake_utils::VoteDelegationTableItem,
};
use crate::{
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema::current_delegated_voter,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::v1::WriteTableItem;
use diesel::{prelude::*, ExpressionMethods};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Identifiable, Queryable)]
#[diesel(primary_key(delegator_address, delegation_pool_address))]
#[diesel(table_name = current_delegated_voter)]
pub struct CurrentDelegatedVoterQuery {
    pub delegation_pool_address: String,
    pub delegator_address: String,
    pub table_handle: Option<String>, // vote_delegation table handle
    pub voter: Option<String>,
    pub pending_voter: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(
    Debug, Deserialize, Eq, FieldCount, Identifiable, Insertable, PartialEq, Serialize, Clone,
)]
#[diesel(primary_key(delegator_address, delegation_pool_address))]
#[diesel(table_name = current_delegated_voter)]
pub struct CurrentDelegatedVoter {
    pub delegation_pool_address: String,
    pub delegator_address: String,
    pub table_handle: Option<String>, // vote_delegation table handle
    pub voter: Option<String>,
    pub pending_voter: Option<String>, // voter to be in the next lockup period
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

// (delegation_pool_address, delegator_address)
type CurrentDelegatedVoterPK = (String, String);
type CurrentDelegatedVoterMap = HashMap<CurrentDelegatedVoterPK, CurrentDelegatedVoter>;
// table handle to delegation pool address mapping
type VoteDelegationTableHandleToPool = HashMap<String, String>;

impl CurrentDelegatedVoter {
    pub fn pk(&self) -> CurrentDelegatedVoterPK {
        (
            self.delegation_pool_address.clone(),
            self.delegator_address.clone(),
        )
    }

    /// There are 3 pieces of information we need in order to get the delegated voters
    /// 1. We need the mapping between pool address and table handle of the governance record. This will help us
    /// figure out what the pool address it is
    /// 2. We need to parse the governance record itself
    /// 3. All active shares prior to governance contract need to be tracked as well, the default voters are the delegators themselves
    pub async fn from_write_table_item(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        vote_delegation_handle_to_pool_address: &VoteDelegationTableHandleToPool,
        conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<CurrentDelegatedVoterMap> {
        let mut delegated_voter_map: CurrentDelegatedVoterMap = HashMap::new();

        let table_item_data = write_table_item.data.as_ref().unwrap();
        let table_handle = standardize_address(&write_table_item.handle);
        if let Some(VoteDelegationTableItem::VoteDelegationVector(vote_delegation_vector)) =
            VoteDelegationTableItem::from_table_item_type(
                table_item_data.value_type.as_str(),
                &table_item_data.value,
                txn_version,
            )?
        {
            let pool_address = match vote_delegation_handle_to_pool_address.get(&table_handle) {
                Some(pool_address) => pool_address.clone(),
                None => {
                    // look up from db
                    Self::get_delegation_pool_address_by_table_handle(conn, &table_handle).await
                        .unwrap_or_else(|_| {
                            tracing::error!(
                                transaction_version = txn_version,
                                lookup_key = &table_handle,
                                "Missing pool address for table handle. You probably should backfill db.",
                            );
                            "".to_string()
                        })
                },
            };
            if !pool_address.is_empty() {
                for inner in vote_delegation_vector {
                    let delegator_address = inner.get_delegator_address();
                    let voter = inner.value.get_voter();
                    let pending_voter = inner.value.get_pending_voter();

                    let delegated_voter = CurrentDelegatedVoter {
                        delegator_address: delegator_address.clone(),
                        delegation_pool_address: pool_address.clone(),
                        voter: Some(voter.clone()),
                        pending_voter: Some(pending_voter.clone()),
                        last_transaction_timestamp: txn_timestamp,
                        last_transaction_version: txn_version,
                        table_handle: Some(table_handle.clone()),
                    };
                    delegated_voter_map
                        .insert((pool_address.clone(), delegator_address), delegated_voter);
                }
            }
        }
        Ok(delegated_voter_map)
    }

    /// For delegators that have delegated before the vote delegation contract deployment, we
    /// need to mark them as default voters, but also be careful that we don't override the
    /// new data
    pub async fn get_delegators_pre_contract_deployment(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        active_pool_to_staking_pool: &ShareToStakingPoolMapping,
        previous_delegated_voters: &CurrentDelegatedVoterMap,
        conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<Option<Self>> {
        if let Some((_, active_balance)) =
            CurrentDelegatorBalance::get_active_share_from_write_table_item(
                write_table_item,
                txn_version,
                0, // placeholder
                active_pool_to_staking_pool,
            )
            .await?
        {
            let pool_address = active_balance.pool_address.clone();
            let delegator_address = active_balance.delegator_address.clone();

            let already_exists = match previous_delegated_voters
                .get(&(pool_address.clone(), delegator_address.clone()))
            {
                Some(_) => true,
                None => {
                    // look up from db
                    Self::get_existence_by_pk(conn, &delegator_address, &pool_address).await
                },
            };
            if !already_exists {
                return Ok(Some(CurrentDelegatedVoter {
                    delegator_address: delegator_address.clone(),
                    delegation_pool_address: pool_address,
                    table_handle: None,
                    voter: Some(delegator_address.clone()),
                    pending_voter: Some(delegator_address),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                }));
            }
        }
        Ok(None)
    }

    pub async fn get_delegation_pool_address_by_table_handle(
        conn: &mut PgPoolConnection<'_>,
        table_handle: &str,
    ) -> anyhow::Result<String> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match CurrentDelegatedVoterQuery::get_by_table_handle(conn, table_handle).await {
                Ok(current_delegated_voter_query_result) => {
                    return Ok(current_delegated_voter_query_result.delegation_pool_address)
                },
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        Err(anyhow::anyhow!(
            "Failed to get delegation pool address from vote delegation write table handle"
        ))
    }

    pub async fn get_existence_by_pk(
        conn: &mut PgPoolConnection<'_>,
        delegator_address: &str,
        delegation_pool_address: &str,
    ) -> bool {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match CurrentDelegatedVoterQuery::get_by_pk(
                conn,
                delegator_address,
                delegation_pool_address,
            )
            .await
            {
                Ok(_) => return true,
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        false
    }
}

impl CurrentDelegatedVoterQuery {
    pub async fn get_by_table_handle(
        conn: &mut PgPoolConnection<'_>,
        table_handle: &str,
    ) -> diesel::QueryResult<Self> {
        current_delegated_voter::table
            .filter(current_delegated_voter::table_handle.eq(table_handle))
            .first::<Self>(conn)
            .await
    }

    pub async fn get_by_pk(
        conn: &mut PgPoolConnection<'_>,
        delegator_address: &str,
        delegation_pool_address: &str,
    ) -> diesel::QueryResult<Self> {
        current_delegated_voter::table
            .filter(current_delegated_voter::delegator_address.eq(delegator_address))
            .filter(current_delegated_voter::delegation_pool_address.eq(delegation_pool_address))
            .first::<Self>(conn)
            .await
    }
}

impl Ord for CurrentDelegatedVoter {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delegator_address.cmp(&other.delegator_address).then(
            self.delegation_pool_address
                .cmp(&other.delegation_pool_address),
        )
    }
}

impl PartialOrd for CurrentDelegatedVoter {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
