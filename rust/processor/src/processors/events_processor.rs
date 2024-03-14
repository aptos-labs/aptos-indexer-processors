// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    models::events_models::events::EventModel,
    schema,
    schema::events::{event_index, indexed_type, inserted_at, transaction_version},
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, QueryResult};
use std::{borrow::Cow, future::Future, pin::Pin};
use tracing::error;

pub struct EventsProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl std::fmt::Debug for EventsProcessor {
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

impl EventsProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    events: Vec<EventModel>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    db_writer
        .send_in_chunks_with_query("events", events, insert_events_query)
        .await;
    Ok(())
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<EventModel> {
    async fn execute_query(
        &self,
        _conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        unimplemented!("execute_query")
    }
}

pub fn insert_events_query<'a>(
    items_to_insert: &'a [EventModel],
    conn: crate::utils::database::PgDbPool,
) -> Pin<Box<dyn Future<Output = QueryResult<usize>> + Send + 'a>> {
    Box::pin(async move {
        let query = diesel::insert_into(schema::events::table)
            .values(items_to_insert.as_ref())
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                indexed_type.eq(excluded(indexed_type)),
            ));
        crate::db_writer::execute_with_better_error(conn, query).await
    })
}

#[async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::EventsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut events = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = match txn.txn_data.as_ref() {
                Some(data) => data,
                None => {
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["EventsProcessor"])
                        .inc();
                    continue;
                },
            };
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
            events.extend(txn_events);
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            events,
        )
        .await;

        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_channel_insertion_duration_in_secs,
                last_transaction_timestamp,
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
