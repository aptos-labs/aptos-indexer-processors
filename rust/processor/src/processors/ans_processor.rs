// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::ans_models::ans_lookup::{
        AddressToPrimaryNameRecord, CurrentAnsLookup, CurrentAnsLookupPK,
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::Transaction;
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
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn();

        let mut all_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> =
            HashMap::new();

        // Map to track all the ans records in this transaction. This is used to lookup a previous ANS record
        // that is in the same transaction batch.
        let mut address_to_primary_name_records: AddressToPrimaryNameRecord = HashMap::new();

        for txn in &transactions {
            // ANS lookups
            let current_ans_lookups = CurrentAnsLookup::from_transaction(
                txn,
                &all_current_ans_lookups,
                &address_to_primary_name_records,
                self.ans_primary_names_table_handle.clone(),
                self.ans_name_records_table_handle.clone(),
                &mut conn,
            );
            all_current_ans_lookups.extend(current_ans_lookups);

            // Create address_to_primary_name_records from all_current_ans_lookups. We have to recreate this every time because
            // registered_address may be None in the current_ans_lookups, so we don't know which value to delete from the map.
            address_to_primary_name_records = all_current_ans_lookups
                .iter()
                .filter_map(|(_, v)| {
                    if v.registered_address.is_some() {
                        Some((v.registered_address.clone().unwrap(), v.clone()))
                    } else {
                        None
                    }
                })
                .collect();
        }

        // Sort ans lookup values for postgres insert
        let mut all_current_ans_lookups = all_current_ans_lookups
            .into_values()
            .collect::<Vec<CurrentAnsLookup>>();
        all_current_ans_lookups.sort_by(|a: &CurrentAnsLookup, b| {
            a.domain.cmp(&b.domain).then(a.subdomain.cmp(&b.subdomain))
        });

        // Insert values to db
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            all_current_ans_lookups,
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

    // fn parse_primary_name_wsc(wsc: &)
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    current_ans_lookups: Vec<CurrentAnsLookup>,
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
        .run::<_, Error, _>(|pg_conn| insert_to_db_impl(pg_conn, &current_ans_lookups))
    {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let current_ans_lookups = clean_data_for_db(current_ans_lookups, true);

                insert_to_db_impl(pg_conn, &current_ans_lookups)
            }),
    }
}

fn insert_to_db_impl(
    conn: &mut PgConnection,
    current_ans_lookups: &[CurrentAnsLookup],
) -> Result<(), diesel::result::Error> {
    insert_current_ans_lookups(conn, current_ans_lookups)?;
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
