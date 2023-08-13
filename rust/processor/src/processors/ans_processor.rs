// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::ans_models::ans_lookup::{
        AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsLookupPK, CurrentAnsPrimaryName,
        CurrentAnsPrimaryNamePK,
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::{
    write_set_change::Change as WriteSetChange, Transaction,
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
        ) = parse_ans(
            &transactions,
            self.ans_name_records_table_handle.clone(),
            self.ans_primary_names_table_handle.clone(),
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
) -> (
    Vec<CurrentAnsLookup>,
    Vec<AnsLookup>,
    Vec<CurrentAnsPrimaryName>,
    Vec<AnsPrimaryName>,
) {
    let mut all_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> = HashMap::new();
    let mut all_ans_lookups: Vec<AnsLookup> = vec![];
    let mut all_current_ans_primary_names: HashMap<CurrentAnsPrimaryNamePK, CurrentAnsPrimaryName> =
        HashMap::new();
    let mut all_ans_primary_names: Vec<AnsPrimaryName> = vec![];

    for transaction in transactions {
        let txn_version = transaction.version as i64;

        // Extracts from user transactions. Other transactions won't have any ANS changes
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");

        for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
            match wsc.change.as_ref().unwrap() {
                // TODO: Add ANS v2 indexing
                WriteSetChange::WriteTableItem(table_item) => {
                    if let Some((current_ans_lookup, ans_lookup)) =
                        CurrentAnsLookup::parse_name_record_from_write_table_item_v1(
                            table_item,
                            &maybe_ans_name_records_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS name record from write table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_lookups.insert(
                            (
                                current_ans_lookup.domain.clone(),
                                current_ans_lookup.subdomain.clone(),
                            ),
                            current_ans_lookup,
                        );
                        all_ans_lookups.push(ans_lookup);
                    }
                    if let Some((current_primary_name, primary_name)) =
                        CurrentAnsPrimaryName::parse_primary_name_record_from_write_table_item_v1(
                            table_item,
                            &maybe_ans_primary_names_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS primary name from write table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_primary_names.insert(
                            current_primary_name.registered_address.clone(),
                            current_primary_name,
                        );
                        all_ans_primary_names.push(primary_name);
                    }
                },
                WriteSetChange::DeleteTableItem(table_item) => {
                    if let Some((current_ans_lookup, ans_lookup)) =
                        CurrentAnsLookup::parse_name_record_from_delete_table_item_v1(
                            table_item,
                            &maybe_ans_name_records_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS name record from delete table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_lookups.insert(
                            (
                                current_ans_lookup.domain.clone(),
                                current_ans_lookup.subdomain.clone(),
                            ),
                            current_ans_lookup,
                        );
                        all_ans_lookups.push(ans_lookup);
                    }
                    if let Some((current_primary_name, primary_name)) =
                        CurrentAnsPrimaryName::parse_primary_name_record_from_delete_table_item_v1(
                            table_item,
                            &maybe_ans_primary_names_table_handle,
                            txn_version,
                            wsc_index as i64,
                        )
                        .unwrap_or_else(|e| {
                            error!(
                                error = ?e,
                                "Error parsing ANS primary name from delete table item"
                            );
                            panic!();
                        })
                    {
                        all_current_ans_primary_names.insert(
                            current_primary_name.registered_address.clone(),
                            current_primary_name,
                        );
                        all_ans_primary_names.push(primary_name);
                    }
                },
                _ => continue,
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

    all_current_ans_lookups
        .sort_by(|a, b| a.domain.cmp(&b.domain).then(a.subdomain.cmp(&b.subdomain)));
    all_current_ans_primary_names.sort_by(|a, b| a.registered_address.cmp(&b.registered_address));
    (
        all_current_ans_lookups,
        all_ans_lookups,
        all_current_ans_primary_names,
        all_ans_primary_names,
    )
}
