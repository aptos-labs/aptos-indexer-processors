// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::transaction_metadata_model::{
        event_size_info::EventSize, transaction_size_info::TransactionSize,
        write_set_size_info::WriteSetSize,
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment};
use std::fmt::Debug;
use tracing::{error, warn};

pub struct TransactionMetadataProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl TransactionMetadataProcessor {
    pub fn new(connection_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for TransactionMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TransactionMetadataProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    transaction_sizes: &[TransactionSize],
    event_sizes: &[EventSize],
    write_set_sizes: &[WriteSetSize],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    execute_in_chunks(
        conn.clone(),
        insert_transaction_sizes_query,
        transaction_sizes,
        get_config_table_chunk_size::<TransactionSize>(
            "transaction_size_info",
            per_table_chunk_sizes,
        ),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_event_sizes_query,
        event_sizes,
        get_config_table_chunk_size::<EventSize>("event_size_info", per_table_chunk_sizes),
    )
    .await?;
    execute_in_chunks(
        conn,
        insert_write_set_sizes_query,
        write_set_sizes,
        get_config_table_chunk_size::<WriteSetSize>("write_set_size_info", per_table_chunk_sizes),
    )
    .await?;

    Ok(())
}

fn insert_transaction_sizes_query(
    items_to_insert: Vec<TransactionSize>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::transaction_size_info::dsl::*;
    (
        diesel::insert_into(schema::transaction_size_info::table)
            .values(items_to_insert)
            .on_conflict(transaction_version)
            .do_nothing(),
        None,
    )
}

fn insert_event_sizes_query(
    items_to_insert: Vec<EventSize>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::event_size_info::dsl::*;
    (
        diesel::insert_into(schema::event_size_info::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, index))
            .do_nothing(),
        None,
    )
}

fn insert_write_set_sizes_query(
    items_to_insert: Vec<WriteSetSize>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::write_set_size_info::dsl::*;
    (
        diesel::insert_into(schema::write_set_size_info::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, index))
            .do_nothing(),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for TransactionMetadataProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::TransactionMetadataProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut transaction_sizes = vec![];
        let mut event_sizes = vec![];
        let mut write_set_sizes = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let size_info = match txn.size_info.as_ref() {
                Some(size_info) => size_info,
                None => {
                    warn!(version = txn.version, "Transaction size info not found");
                    continue;
                },
            };
            transaction_sizes.push(TransactionSize::from_transaction_info(
                size_info,
                txn_version,
            ));
            for (index, event_size_info) in size_info.event_size_info.iter().enumerate() {
                event_sizes.push(EventSize::from_event_size_info(
                    event_size_info,
                    txn_version,
                    index as i64,
                ));
            }
            for (index, write_set_size_info) in size_info.write_op_size_info.iter().enumerate() {
                write_set_sizes.push(WriteSetSize::from_transaction_info(
                    write_set_size_info,
                    txn_version,
                    index as i64,
                ));
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &transaction_sizes,
            &event_sizes,
            &write_set_sizes,
            &self.per_table_chunk_sizes,
        )
        .await;
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp: transactions.last().unwrap().timestamp.clone(),
            }),
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

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
