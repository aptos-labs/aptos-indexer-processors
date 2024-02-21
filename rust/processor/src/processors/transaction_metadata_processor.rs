// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::transaction_metadata_model::{
        event_size_info::EventSize, transaction_size_info::TransactionSize,
        write_set_size_info::WriteSetSize,
    },
    schema,
};
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use tracing::{error, warn};

pub struct TransactionMetadataProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl TransactionMetadataProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

impl std::fmt::Debug for TransactionMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool().state();
        write!(
            f,
            "{:} {{ connections: {:?}  idle_connections: {:?} }}",
            self.name(),
            state.connections,
            state.idle_connections
        )
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    transaction_sizes: Vec<TransactionSize>,
    event_sizes: Vec<EventSize>,
    write_set_sizes: Vec<WriteSetSize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let tsi = db_writer.send_in_chunks("transaction_size_info", transaction_sizes);
    let esi = db_writer.send_in_chunks("event_size_info", event_sizes);
    let wssi = db_writer.send_in_chunks("write_set_size_info", write_set_sizes);

    tokio::join!(tsi, esi, wssi);

    Ok(())
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<TransactionSize> {
    async fn execute_query(
        &self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use schema::transaction_size_info::dsl::*;

        let query = diesel::insert_into(schema::transaction_size_info::table)
            .values(self)
            .on_conflict(transaction_version)
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<EventSize> {
    async fn execute_query(
        &self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use schema::event_size_info::dsl::*;

        let query = diesel::insert_into(schema::event_size_info::table)
            .values(self)
            .on_conflict((transaction_version, index))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<WriteSetSize> {
    async fn execute_query(
        &self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use schema::write_set_size_info::dsl::*;

        let query = diesel::insert_into(schema::write_set_size_info::table)
            .values(self)
            .on_conflict((transaction_version, index))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
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
        let db_channel_insertion_duration_in_secs = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            transaction_sizes,
            event_sizes,
            write_set_sizes,
        )
        .await;
        let db_channel_insertion_duration_in_secs = db_channel_insertion_duration_in_secs
            .elapsed()
            .as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_channel_insertion_duration_in_secs,
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

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
