// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::ans_models::ans_lookup::{
        parse_name_record_from_delete_table_item_v1, parse_name_record_from_write_table_item_v1,
        parse_primary_name_record_from_delete_table_item_v1,
        parse_primary_name_record_from_write_table_item_v1, Address, AnsLookup, AnsPrimaryName,
        CurrentAnsLookup, CurrentAnsLookupPK, CurrentAnsPrimaryName, CurrentAnsPrimaryNamePK,
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
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
    ans_primary_names_table_handle: Option<String>,
    ans_name_records_table_handle: Option<String>,
}

impl AnsTransactionProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        ans_primary_names_table_handle: Option<String>,
        ans_name_records_table_handle: Option<String>,
    ) -> Self {
        tracing::info!(
            ans_primary_names_table_handle = ans_primary_names_table_handle,
            ans_name_records_table_handle = ans_name_records_table_handle,
            "init AnsProcessor"
        );
        Self {
            connection_pool,
            ans_primary_names_table_handle,
            ans_name_records_table_handle,
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

                insert_to_db_impl(
                    pg_conn,
                    &current_ans_lookups,
                    &ans_lookups,
                    &current_ans_primary_names,
                    &ans_primary_names,
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
) -> Result<(), diesel::result::Error> {
    insert_current_ans_lookups(conn, current_ans_lookups)?;
    insert_ans_lookups(conn, ans_lookups)?;
    insert_current_ans_primary_names(conn, current_ans_primary_names)?;
    insert_ans_primary_names(conn, ans_primary_names)?;
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
                        inserted_at.eq(excluded(inserted_at)),
                        token_name.eq(excluded(token_name)),
                        is_primary.eq(excluded(is_primary)),
                        is_deleted.eq(excluded(is_deleted)),
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
                .on_conflict((transaction_version, domain, subdomain))
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
                .on_conflict((registered_address, domain, subdomain))
                .do_update()
                .set((
                    token_name.eq(excluded(token_name)),
                    is_primary.eq(excluded(is_primary)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
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
                .on_conflict((transaction_version, wsc_index, domain, subdomain))
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
            mut all_ans_lookups,
            all_current_ans_primary_names,
            mut all_ans_primary_names,
        ) = parse_ans(
            &transactions,
            self.ans_name_records_table_handle.clone(),
            self.ans_primary_names_table_handle.clone(),
            &mut conn,
        );

        // Sort ans lookup values for postgres insert
        let mut all_current_ans_lookups = all_current_ans_lookups
            .into_values()
            .collect::<Vec<CurrentAnsLookup>>();
        let mut all_current_ans_primary_names = all_current_ans_primary_names
            .into_values()
            .collect::<Vec<CurrentAnsPrimaryName>>();

        all_current_ans_lookups.sort_by(|a: &CurrentAnsLookup, b| {
            a.domain.cmp(&b.domain).then(a.subdomain.cmp(&b.subdomain))
        });
        all_ans_lookups.sort_by(|a: &AnsLookup, b| {
            (&a.transaction_version, &a.domain, &a.subdomain).cmp(&(
                &b.transaction_version,
                &b.domain,
                &b.subdomain,
            ))
        });
        all_current_ans_primary_names.sort_by(|a: &CurrentAnsPrimaryName, b| {
            (&a.registered_address, &a.domain, &a.subdomain).cmp(&(
                &b.registered_address,
                &b.domain,
                &b.subdomain,
            ))
        });
        all_ans_primary_names.sort_by(|a: &AnsPrimaryName, b| {
            (
                &a.transaction_version,
                &a.wsc_index,
                &a.domain,
                &a.subdomain,
            )
                .cmp(&(
                    &b.transaction_version,
                    &b.wsc_index,
                    &b.domain,
                    &b.subdomain,
                ))
        });

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
    transactions: &[Transaction],
    maybe_ans_primary_names_table_handle: Option<String>,
    maybe_ans_name_records_table_handle: Option<String>,
    conn: &mut PgPoolConnection,
) -> (
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    Vec<AnsLookup>,
    HashMap<CurrentAnsPrimaryNamePK, CurrentAnsPrimaryName>,
    Vec<AnsPrimaryName>,
) {
    let mut all_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> = HashMap::new();
    let mut all_ans_lookups: Vec<AnsLookup> = vec![];
    let mut all_current_ans_primary_names: HashMap<CurrentAnsPrimaryNamePK, CurrentAnsPrimaryName> =
        HashMap::new();
    let mut all_ans_primary_names: Vec<AnsPrimaryName> = vec![];

    // Create a map to lookup primary name by address. This is necessary for primary name deletes and updates.
    let mut lookup_ans_primary_names: HashMap<Address, CurrentAnsPrimaryName> = HashMap::new();

    for transaction in transactions {
        let txn_version = transaction.version as i64;
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");

        // Extracts from user transactions. Other transactions won't have any ANS changes
        if let (
            TxnData::User(_),
            Some(ans_primary_names_table_handle),
            Some(ans_name_records_table_handle),
        ) = (
            txn_data,
            maybe_ans_primary_names_table_handle.clone(),
            maybe_ans_name_records_table_handle.clone(),
        ) {
            let transaction_info = transaction
                .info
                .as_ref()
                .expect("Transaction info doesn't exist!");

            // Loop 1: Extract name record changes
            for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    // TODO: Add ANS v2 indexing
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        let parse_result = parse_name_record_from_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            wsc_index as i64,
                        );
                        match parse_result {
                            Ok((current_ans_lookup, ans_lookup)) => {
                                all_current_ans_lookups
                                    .insert(current_ans_lookup.pk(), current_ans_lookup);
                                all_ans_lookups.push(ans_lookup);
                            },
                            Err(e) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    e = ?e,
                                    "Failed to parse name record from write table item v1"
                                );
                                continue;
                            },
                        }
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        let parse_result = parse_name_record_from_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                            wsc_index as i64,
                        );
                        match parse_result {
                            Ok((current_ans_lookup, ans_lookup)) => {
                                all_current_ans_lookups
                                    .insert(current_ans_lookup.pk(), current_ans_lookup);
                                all_ans_lookups.push(ans_lookup);
                            },
                            Err(e) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    e = ?e,
                                    "Failed to parse name record from delete table item v1"
                                );
                                continue;
                            },
                        }
                    },
                    _ => continue,
                };
            }

            // Loop 2: Extract primary name reverse lookup changes
            for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    // TODO: Add ANS v2 indexing
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        let parse_result = parse_primary_name_record_from_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            wsc_index as i64,
                            &lookup_ans_primary_names,
                            conn,
                        );
                        match parse_result {
                            Ok((
                                current_ans_primary_name,
                                ans_primary_name,
                                maybe_old_current_ans_primary_name,
                                maybe_old_ans_primary_name,
                            )) => {
                                // New primary name record
                                all_current_ans_primary_names.insert(
                                    current_ans_primary_name.pk(),
                                    current_ans_primary_name.clone(),
                                );
                                lookup_ans_primary_names.insert(
                                    current_ans_primary_name.clone().registered_address,
                                    current_ans_primary_name.clone(),
                                );
                                all_ans_primary_names.push(ans_primary_name.clone());

                                // Old primary name record
                                if let Some(old_current_ans_primary_name) =
                                    maybe_old_current_ans_primary_name
                                {
                                    all_current_ans_primary_names.insert(
                                        old_current_ans_primary_name.pk(),
                                        old_current_ans_primary_name,
                                    );
                                }
                                if let Some(old_ans_primary_name) = maybe_old_ans_primary_name {
                                    all_ans_primary_names.push(old_ans_primary_name);
                                }
                            },
                            Err(e) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    e = ?e,
                                    "Failed to parse primary name record from write table item v1"
                                );
                                continue;
                            },
                        }
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        let parse_result = parse_primary_name_record_from_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                            wsc_index as i64,
                            &lookup_ans_primary_names,
                            conn,
                        );
                        match parse_result {
                            Ok(Some((current_ans_primary_name, ans_primary_name))) => {
                                all_current_ans_primary_names.insert(
                                    current_ans_primary_name.pk(),
                                    current_ans_primary_name.clone(),
                                );
                                lookup_ans_primary_names
                                    .remove(&current_ans_primary_name.clone().registered_address);
                                all_ans_primary_names.push(ans_primary_name);
                            },
                            Ok(None) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    "Failed to parse primary name record from delete table item v1"
                                );
                                continue;
                            },
                            Err(e) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    e = ?e,
                                    "Failed to parse primary name record from delete table item v1"
                                );
                                continue;
                            },
                        }
                    },
                    _ => continue,
                };
            }
        }
    }
    (
        all_current_ans_lookups,
        all_ans_lookups,
        all_current_ans_primary_names,
        all_ans_primary_names,
    )
}
