// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use super::{
    delegator_balances::ShareToStakingPoolMapping,
    delegator_pools::DelegatorPool,
    stake_utils::{DelgationVoteGovernanceRecordsResource, VoteDelegationTableItem},
};
use crate::{
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema::current_delegated_voter,
    utils::{
        database::PgPoolConnection,
        util::{parse_timestamp, standardize_address},
    },
};
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, Transaction as TransactionPB,
    WriteResource, WriteTableItem,
};
use diesel::{prelude::*, ExpressionMethods};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Identifiable, Queryable)]
#[diesel(primary_key(delegator_address, delegation_pool_address))]
#[diesel(table_name = current_delegated_voter)]
pub struct CurrentDelegatedVoterQuery {
    pub delegator_address: String,
    pub delegation_pool_address: String,
    pub table_handle: String,
    pub voter: Option<String>,
    pub pending_voter: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(delegator_address, delegation_pool_address))]
#[diesel(table_name = current_delegated_voter)]
pub struct CurrentDelegatedVoter {
    pub delegator_address: String,
    pub delegation_pool_address: String,
    pub table_handle: String, // vote_delegation table handle
    pub voter: Option<String>,
    pub pending_voter: Option<String>, // voter to be in the next lockup period
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

// (delegation_pool_address, delegator_address)
pub type CurrentDelegatedVoterPK = (String, String);

pub type CurrentDelegatedVoterMap = HashMap<CurrentDelegatedVoterPK, CurrentDelegatedVoter>;
// table handle to delegation pool address mapping
pub type VoteDelegationTableHandleToDelegationPoolAddress = HashMap<String, String>;
// (optional voter, optional pending voter, table handle)
pub type PKtoVoteDelegation =
    HashMap<CurrentDelegatedVoterPK, (Option<String>, Option<String>, String)>;

impl CurrentDelegatedVoter {
    pub fn from_transaction(
        transaction: &TransactionPB,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<CurrentDelegatedVoterMap> {
        let mut current_delegated_voter_map: CurrentDelegatedVoterMap = HashMap::new();
        let mut vote_delegation_handle_to_pool_address: VoteDelegationTableHandleToDelegationPoolAddress = HashMap::new();
        let mut pk_to_vote_delegation: PKtoVoteDelegation = HashMap::new();
        let mut active_pool_to_staking_pool: ShareToStakingPoolMapping = HashMap::new();

        let txn_version = transaction.version as i64;
        let txn_timestamp = parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version);
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");

        if let TxnData::User(_) = txn_data {
            let transaction_info = transaction
                .info
                .as_ref()
                .expect("Transaction info doesn't exist!");

            // this write resource indexing is to get GovernanceRecords WR with the mapping of pool address to table handle
            for wsc in &transaction_info.changes {
                if let WriteSetChange::WriteResource(write_resource) = wsc.change.as_ref().unwrap()
                {
                    if let Some(DelgationVoteGovernanceRecordsResource::GovernanceRecords(inner)) =
                        DelgationVoteGovernanceRecordsResource::from_write_resource(
                            write_resource,
                            txn_version,
                        )?
                    {
                        let delegation_pool_address =
                            standardize_address(&write_resource.address.to_string());
                        let vote_delegation_handle =
                            inner.vote_delegation.buckets.inner.get_handle();

                        vote_delegation_handle_to_pool_address
                            .insert(vote_delegation_handle, delegation_pool_address.clone());
                    }
                }
            }

            // this write table item indexing is to get delegator address, table handle, and voter & pending voter
            for wsc in &transaction_info.changes {
                if let WriteSetChange::WriteTableItem(write_table_item) =
                    wsc.change.as_ref().unwrap()
                {
                    let table_item_data = write_table_item.data.as_ref().unwrap();
                    let table_handle = &write_table_item.handle;
                    if let Some(VoteDelegationTableItem::VoteDelegationMap(inner)) =
                        VoteDelegationTableItem::from_table_item_type(
                            table_item_data.value_type.as_str(),
                            &table_item_data.value,
                            txn_version,
                        )?
                    {
                        let delegator_address = standardize_address(&inner.key);
                        let voter = &inner.value.get_voter();
                        let pending_voter = &inner.value.get_pending_voter();
                        if let Some(maybe_delegation_pool_address) =
                            vote_delegation_handle_to_pool_address.get(table_handle)
                        {
                            let current_delegated_voter = CurrentDelegatedVoter {
                                delegator_address: delegator_address.clone(),
                                delegation_pool_address: maybe_delegation_pool_address.clone(),
                                voter: Some(voter.clone()),
                                pending_voter: Some(pending_voter.clone()),
                                last_transaction_timestamp: txn_timestamp,
                                last_transaction_version: txn_version,
                                table_handle: table_handle.clone(),
                            };
                            current_delegated_voter_map.insert(
                                (
                                    maybe_delegation_pool_address.clone(),
                                    delegator_address.clone(),
                                ),
                                current_delegated_voter,
                            );
                            pk_to_vote_delegation.insert(
                                (
                                    maybe_delegation_pool_address.clone(),
                                    delegator_address.clone(),
                                ),
                                (
                                    Some(voter.to_string()),
                                    Some(pending_voter.to_string()),
                                    table_handle.clone(),
                                ),
                            );
                        } else {
                            match Self::get_delegation_pool_address_by_table_handle(
                                conn,
                                table_handle,
                            ) {
                                Ok(delegation_pool_address_from_query_result) => {
                                    let current_delegated_voter = CurrentDelegatedVoter {
                                        delegator_address: delegator_address.clone(),
                                        delegation_pool_address:
                                            delegation_pool_address_from_query_result.clone(),
                                        voter: Some(voter.clone()),
                                        pending_voter: Some(pending_voter.clone()),
                                        last_transaction_timestamp: txn_timestamp,
                                        last_transaction_version: txn_version,
                                        table_handle: table_handle.clone(),
                                    };
                                    current_delegated_voter_map.insert(
                                        (
                                            delegation_pool_address_from_query_result.clone(),
                                            delegator_address.clone(),
                                        ),
                                        current_delegated_voter,
                                    );
                                    vote_delegation_handle_to_pool_address.insert(
                                        table_handle.clone(),
                                        delegation_pool_address_from_query_result.clone(),
                                    );
                                    pk_to_vote_delegation.insert(
                                        (
                                            delegation_pool_address_from_query_result.clone(),
                                            delegator_address.clone(),
                                        ),
                                        (
                                            Some(voter.to_string()),
                                            Some(pending_voter.to_string()),
                                            table_handle.clone(),
                                        ),
                                    );
                                },
                                Err(_) => {
                                    tracing::error!(
                                        transaction_version = txn_version,
                                        lookup_key = &table_handle,
                                        "Failed to get delegation pool address from vote delegation write table handle. You probably should backfill db.",
                                    );
                                },
                            }
                        }
                    }
                }
            }

            // this write resource indexing is to get active pool to DelegatorPoolBalanceMetadata mapping
            // as a helper for us to get delegation pool address and delegator address from DelegationPool
            // with the assumption that when delegator can delegate votes, they must have already staked
            for wsc in &transaction.info.as_ref().unwrap().changes {
                if let WriteSetChange::WriteResource(write_resource) = wsc.change.as_ref().unwrap()
                {
                    if let Some(map) =
                        Self::get_active_pool_to_staking_pool_mapping(write_resource, txn_version)?
                    {
                        active_pool_to_staking_pool.extend(map);
                    }
                }
            }

            // this write table item indexing is to get delegation pool address and delegator address PK
            // to cover the edge case where partial voting didn't exist in the pre vote delegation era
            // where we try to get the PK and check against the DB to check for its existence
            // if already there, don't overwrite, if not, try to get vote delegation info from mapping pk_to_vote_delegation
            // if not found, return vote delegation fields as none
            for wsc in &transaction.info.as_ref().unwrap().changes {
                if let WriteSetChange::WriteTableItem(write_table_item) =
                    wsc.change.as_ref().unwrap()
                {
                    if let Some(pk) = Self::get_pk_from_write_table_item(
                        write_table_item,
                        &active_pool_to_staking_pool,
                    )? {
                        if let Some((maybe_voter, maybe_pending_voter, table_handle)) =
                            pk_to_vote_delegation.get(&pk)
                        {
                            match Self::get_existance_by_pk(conn, &pk.0, &pk.1) {
                                Ok(pk_exist) => {
                                    if pk_exist {
                                        continue;
                                    }

                                    let new_current_delegated_voter = CurrentDelegatedVoter {
                                        delegator_address: pk.1.clone(),
                                        delegation_pool_address: pk.0.clone(),
                                        voter: maybe_voter.clone(),
                                        pending_voter: maybe_pending_voter.clone(),
                                        last_transaction_timestamp: txn_timestamp,
                                        last_transaction_version: txn_version,
                                        table_handle: table_handle.clone(),
                                    };

                                    current_delegated_voter_map
                                        .insert(pk, new_current_delegated_voter);
                                },
                                Err(_) => {
                                    tracing::error!(
                                        transaction_version = txn_version,
                                        lookup_key = &table_handle,
                                        "Failed to get row from PK.",
                                    );
                                },
                            }
                        }
                    }
                };
            }
        }

        Ok(current_delegated_voter_map)
    }

    pub fn get_delegation_pool_address_by_table_handle(
        conn: &mut PgPoolConnection,
        table_handle: &str,
    ) -> anyhow::Result<String> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match CurrentDelegatedVoterQuery::get_by_table_handle(conn, table_handle) {
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

    pub fn get_existance_by_pk(
        conn: &mut PgPoolConnection,
        delegation_pool_address: &str,
        delegator_address: &str,
    ) -> anyhow::Result<bool> {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match CurrentDelegatedVoterQuery::get_by_pk(
                conn,
                delegation_pool_address,
                delegator_address,
            ) {
                Ok(_) => return Ok(true),
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        Ok(false)
    }

    pub fn get_active_pool_to_staking_pool_mapping(
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<ShareToStakingPoolMapping>> {
        if let Some(balance) = DelegatorPool::get_delegated_pool_metadata_from_write_resource(
            write_resource,
            txn_version,
        )? {
            Ok(Some(HashMap::from([(
                balance.active_share_table_handle.clone(),
                balance,
            )])))
        } else {
            Ok(None)
        }
    }

    pub fn get_pk_from_write_table_item(
        write_table_item: &WriteTableItem,
        active_pool_to_staking_pool: &ShareToStakingPoolMapping,
    ) -> anyhow::Result<Option<(String, String)>> {
        let table_handle = standardize_address(&write_table_item.handle.to_string());
        if let Some(pool_balance) = active_pool_to_staking_pool.get(&table_handle) {
            let delegation_pool_address = pool_balance.staking_pool_address.clone();
            let delegator_address = standardize_address(&write_table_item.key.to_string());

            Ok(Some((delegation_pool_address, delegator_address)))
        } else {
            Ok(None)
        }
    }
}

impl CurrentDelegatedVoterQuery {
    pub fn get_by_table_handle(
        conn: &mut PgPoolConnection,
        table_handle: &str,
    ) -> diesel::QueryResult<Self> {
        current_delegated_voter::table
            .filter(current_delegated_voter::table_handle.eq(table_handle))
            .first::<Self>(conn)
    }

    pub fn get_by_pk(
        conn: &mut PgPoolConnection,
        delegation_pool_address: &str,
        delegator_address: &str,
    ) -> diesel::QueryResult<Self> {
        current_delegated_voter::table
            .filter(current_delegated_voter::delegation_pool_address.eq(delegation_pool_address))
            .filter(current_delegated_voter::delegator_address.eq(delegator_address))
            .first::<Self>(conn)
    }
}
