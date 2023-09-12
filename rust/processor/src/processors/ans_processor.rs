// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::ans_models::{
        ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
        ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2},
        ans_utils::{RenewNameEvent, SetReverseLookupEvent},
    },
    schema,
    utils::{
        custom_configs::CustomProcessorConfigs,
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
        },
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, Transaction,
};
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

pub const NAME: &str = "ans_processor";
pub struct AnsTransactionProcessor {
    connection_pool: PgDbPool,
    ans_v1_primary_names_table_handle: String,
    ans_v1_name_records_table_handle: String,
    ans_v2_contract_address: String,
}

impl AnsTransactionProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        custom_processor_configs: Option<CustomProcessorConfigs>,
    ) -> Self {
        if custom_processor_configs.is_none() {
            panic!("Custom processor configs are required for AnsProcessor");
        }

        let custom_processor_configs = custom_processor_configs.unwrap();
        if custom_processor_configs.ans_processor_config.is_none() {
            panic!("AnsProcessorConfig is required for AnsProcessor");
        }

        let ans_processor_config = custom_processor_configs.ans_processor_config.unwrap();
        let ans_v1_primary_names_table_handle =
            ans_processor_config.ans_v1_primary_names_table_handle;
        let ans_v1_name_records_table_handle =
            ans_processor_config.ans_v1_name_records_table_handle;
        let ans_v2_contract_address = ans_processor_config.ans_v2_contract_address;

        tracing::info!(
            ans_v1_primary_names_table_handle = ans_v1_primary_names_table_handle,
            ans_v1_name_records_table_handle = ans_v1_name_records_table_handle,
            ans_v2_contract_address = ans_v2_contract_address,
            "init AnsProcessor"
        );
        Self {
            connection_pool,
            ans_v1_primary_names_table_handle,
            ans_v1_name_records_table_handle,
            ans_v2_contract_address,
        }
    }
}

impl Debug for AnsTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AnsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    current_ans_lookups: Vec<CurrentAnsLookup>,
    ans_lookups: Vec<AnsLookup>,
    current_ans_primary_names: Vec<CurrentAnsPrimaryName>,
    ans_primary_names: Vec<AnsPrimaryName>,
    current_ans_lookups_v2: Vec<CurrentAnsLookupV2>,
    ans_lookups_v2: Vec<AnsLookupV2>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            insert_to_db_impl(
                pg_conn,
                &current_ans_lookups,
                &ans_lookups,
                &current_ans_primary_names,
                &ans_primary_names,
                &current_ans_lookups_v2,
                &ans_lookups_v2,
            )
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let current_ans_lookups = clean_data_for_db(current_ans_lookups, true);
                let ans_lookups = clean_data_for_db(ans_lookups, true);
                let current_ans_primary_names = clean_data_for_db(current_ans_primary_names, true);
                let ans_primary_names = clean_data_for_db(ans_primary_names, true);
                let current_ans_lookups_v2 = clean_data_for_db(current_ans_lookups_v2, true);
                let ans_lookups_v2 = clean_data_for_db(ans_lookups_v2, true);

                insert_to_db_impl(
                    pg_conn,
                    &current_ans_lookups,
                    &ans_lookups,
                    &current_ans_primary_names,
                    &ans_primary_names,
                    &current_ans_lookups_v2,
                    &ans_lookups_v2,
                )
            }),
    }
}

fn insert_to_db_impl(
    conn: &mut PgConnection,
    current_ans_lookups: &[CurrentAnsLookup],
    ans_lookups: &[AnsLookup],
    current_ans_primary_names: &[CurrentAnsPrimaryName],
    ans_primary_names: &[AnsPrimaryName],
    current_ans_lookups_v2: &[CurrentAnsLookupV2],
    ans_lookups_v2: &[AnsLookupV2],
) -> Result<(), diesel::result::Error> {
    insert_current_ans_lookups(conn, current_ans_lookups)?;
    insert_ans_lookups(conn, ans_lookups)?;
    insert_current_ans_primary_names(conn, current_ans_primary_names)?;
    insert_ans_primary_names(conn, ans_primary_names)?;
    insert_current_ans_lookups_v2(conn, current_ans_lookups_v2)?;
    insert_ans_lookups_v2(conn, ans_lookups_v2)?;
    Ok(())
}

fn insert_current_ans_lookups(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentAnsLookup],
) -> Result<(), diesel::result::Error> {
    use schema::current_ans_lookup::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentAnsLookup::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
                conn,
                diesel::insert_into(schema::current_ans_lookup::table)
                    .values(&items_to_insert[start_ind..end_ind])
                    .on_conflict((domain, subdomain))
                    .do_update()
                    .set((
                        registered_address.eq(excluded(registered_address)),
                        expiration_timestamp.eq(excluded(expiration_timestamp)),
                        last_transaction_version.eq(excluded(last_transaction_version)),
                        token_name.eq(excluded(token_name)),
                        is_deleted.eq(excluded(is_deleted)),
                        inserted_at.eq(excluded(inserted_at)),
                    )),
                    Some(" WHERE current_ans_lookup.last_transaction_version <= excluded.last_transaction_version "),
                )?;
    }
    Ok(())
}

fn insert_ans_lookups(
    conn: &mut PgConnection,
    items_to_insert: &[AnsLookup],
) -> Result<(), diesel::result::Error> {
    use schema::ans_lookup::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), AnsLookup::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::ans_lookup::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_ans_primary_names(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentAnsPrimaryName],
) -> Result<(), diesel::result::Error> {
    use schema::current_ans_primary_name::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentAnsPrimaryName::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_ans_primary_name::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(registered_address)
                .do_update()
                .set((
                    domain.eq(excluded(domain)),
                    subdomain.eq(excluded(subdomain)),
                    token_name.eq(excluded(token_name)),
                    is_deleted.eq(excluded(is_deleted)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE current_ans_primary_name.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_ans_primary_names(
    conn: &mut PgConnection,
    items_to_insert: &[AnsPrimaryName],
) -> Result<(), diesel::result::Error> {
    use schema::ans_primary_name::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), AnsPrimaryName::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::ans_primary_name::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_ans_lookups_v2(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentAnsLookupV2],
) -> Result<(), diesel::result::Error> {
    use schema::current_ans_lookup_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentAnsLookupV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
                conn,
                diesel::insert_into(schema::current_ans_lookup_v2::table)
                    .values(&items_to_insert[start_ind..end_ind])
                    .on_conflict((domain, subdomain))
                    .do_update()
                    .set((
                        token_name.eq(excluded(token_name)),
                        registered_address.eq(excluded(registered_address)),
                        expiration_timestamp.eq(excluded(expiration_timestamp)),
                        is_primary.eq(excluded(is_primary)),
                        is_deleted.eq(excluded(is_deleted)),
                        last_transaction_version.eq(excluded(last_transaction_version)),
                        inserted_at.eq(excluded(inserted_at)),
                    )),
                    Some(" WHERE current_ans_lookup_v2.last_transaction_version <= excluded.last_transaction_version "),
                )?;
    }
    Ok(())
}

fn insert_ans_lookups_v2(
    conn: &mut PgConnection,
    items_to_insert: &[AnsLookupV2],
) -> Result<(), diesel::result::Error> {
    use schema::ans_lookup_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), AnsLookupV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::ans_lookup_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for AnsTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn();

        let (
            all_current_ans_lookups,
            all_ans_lookups,
            all_current_ans_primary_names,
            all_ans_primary_names,
            all_current_ans_lookups_v2,
            all_ans_lookups_v2,
        ) = parse_ans(
            &mut conn,
            &transactions,
            self.ans_v1_primary_names_table_handle.clone(),
            self.ans_v1_name_records_table_handle.clone(),
            self.ans_v2_contract_address.clone(),
        );

        // Insert values to db
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            all_current_ans_lookups,
            all_ans_lookups,
            all_current_ans_primary_names,
            all_ans_primary_names,
            all_current_ans_lookups_v2,
            all_ans_lookups_v2,
        );

        match tx_result {
            Ok(_) => Ok((start_version, end_version)),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

fn parse_ans(
    conn: &mut PgPoolConnection,
    transactions: &[Transaction],
    ans_v1_primary_names_table_handle: String,
    ans_v1_name_records_table_handle: String,
    ans_v2_contract_address: String,
) -> (
    Vec<CurrentAnsLookup>,
    Vec<AnsLookup>,
    Vec<CurrentAnsPrimaryName>,
    Vec<AnsPrimaryName>,
    Vec<CurrentAnsLookupV2>,
    Vec<AnsLookupV2>,
) {
    let mut all_current_ans_lookups = HashMap::new();
    let mut all_ans_lookups = vec![];
    let mut all_current_ans_primary_names = HashMap::new();
    let mut all_ans_primary_names = vec![];
    let mut all_current_ans_lookups_v2 = HashMap::new();
    let mut all_ans_lookups_v2 = vec![];

    for transaction in transactions {
        let txn_version = transaction.version as i64;
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");

        // Extracts from user transactions. Other transactions won't have any ANS changes

        if let TxnData::User(user_txn) = txn_data {
            // TODO: Use the v2_renew_name_events to preserve metadata once we switch to a single ANS table to store everything
            let mut v2_renew_name_events = HashMap::new();
            let mut v2_set_reverse_lookup_events = vec![];

            // Parse V2 ANS Events. We only care about the following events:
            // 1. RenewNameEvents: helps to fill in metadata for name records with updated expiration time
            // 2. SetReverseLookupEvents: parse to get current_ans_primary_names
            for (_event_index, event) in user_txn.events.iter().enumerate() {
                if let Some(renew_name_event) =
                    RenewNameEvent::from_event(event, &ans_v2_contract_address, txn_version)
                        .unwrap()
                {
                    v2_renew_name_events.insert(
                        (
                            renew_name_event.get_domain_trunc(),
                            renew_name_event.get_subdomain_trunc(),
                        ),
                        renew_name_event,
                    );
                }
                if let Some(set_reverse_lookup_event) =
                    SetReverseLookupEvent::from_event(event, &ans_v2_contract_address, txn_version)
                        .unwrap()
                {
                    v2_set_reverse_lookup_events.push(set_reverse_lookup_event);
                }
            }

            // Parse ANS v1 and v2 write set changes
            for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    // Parse ANS v1 write and delete table items
                    WriteSetChange::WriteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 name record from write table item"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup);
                            all_ans_lookups.push(ans_lookup);
                        }
                        if let Some((current_primary_name, primary_name)) =
                        CurrentAnsPrimaryName::parse_primary_name_record_from_write_table_item_v1(
                            table_item,
                            &ans_v1_primary_names_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS v1 primary name from write table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_primary_names
                            .insert(current_primary_name.pk(), current_primary_name);
                        all_ans_primary_names.push(primary_name);
                    }
                    },
                    WriteSetChange::DeleteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 name record from delete table item"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup);
                            all_ans_lookups.push(ans_lookup);
                        }
                        if let Some((current_primary_name, primary_name)) =
                        CurrentAnsPrimaryName::parse_primary_name_record_from_delete_table_item_v1(
                            table_item,
                            &ans_v1_primary_names_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS v1 primary name from delete table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_primary_names
                            .insert(current_primary_name.pk(), current_primary_name);
                        all_ans_primary_names.push(primary_name);
                    }
                    },
                    // Parse ANS v2 write resource to get the name records
                    WriteSetChange::WriteResource(write_resource) => {
                        if let Some((current_ans_lookup_v2, ans_lookup_v2)) =
                            CurrentAnsLookupV2::parse_name_record_from_write_resource_v2(
                                write_resource,
                                &ans_v2_contract_address,
                                txn_version,
                                wsc_index as i64,
                                &v2_renew_name_events,
                            )
                            .unwrap_or_else(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v2 name record from write resource"
                                );
                                panic!();
                            })
                        {
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                    },
                    // For ANS V2, there are no delete resource changes
                    // 1. Unsetting a primary name will show up as a ReverseRecord write resource with empty fields
                    // 2. Name record v2 tokens are never deleted
                    _ => continue,
                }
            }

            // Loop through the SetReverseLookups to get is_primary state of the name records in this txn
            for (event_index, event) in v2_set_reverse_lookup_events.iter().enumerate() {
                let (maybe_current_ans_lookup_v2, maybe_previous_ans_lookup_v2) =
                    CurrentAnsLookupV2::parse_primary_name_record_from_set_reverse_lookup_event(
                        conn,
                        event,
                        txn_version,
                        event_index as i64,
                        &all_current_ans_lookups_v2,
                    )
                    .unwrap_or_else(|e| {
                        error!(
                            error = ?e,
                            "Error parsing ANS v2 primary name record from SetReverseLookupEvent"
                        );
                        panic!();
                    });

                if let Some((current_ans_lookup_v2, ans_lookup_v2)) = maybe_current_ans_lookup_v2 {
                    all_current_ans_lookups_v2
                        .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                    all_ans_lookups_v2.push(ans_lookup_v2);
                }
                if let Some((previous_ans_lookup_v2, ans_lookup_v2)) = maybe_previous_ans_lookup_v2
                {
                    all_current_ans_lookups_v2
                        .insert(previous_ans_lookup_v2.pk(), previous_ans_lookup_v2);
                    all_ans_lookups_v2.push(ans_lookup_v2);
                }
            }
        }
    }
    // Boilerplate after this for diesel
    // Sort ans lookup values for postgres insert
    let mut all_current_ans_lookups = all_current_ans_lookups
        .into_values()
        .collect::<Vec<CurrentAnsLookup>>();
    let mut all_current_ans_primary_names = all_current_ans_primary_names
        .into_values()
        .collect::<Vec<CurrentAnsPrimaryName>>();
    let mut all_current_ans_lookups_v2 = all_current_ans_lookups_v2
        .into_values()
        .collect::<Vec<CurrentAnsLookupV2>>();

    all_current_ans_lookups.sort();
    all_current_ans_primary_names.sort();
    all_current_ans_lookups_v2.sort();
    (
        all_current_ans_lookups,
        all_ans_lookups,
        all_current_ans_primary_names,
        all_ans_primary_names,
        all_current_ans_lookups_v2,
        all_ans_lookups_v2,
    )
}
