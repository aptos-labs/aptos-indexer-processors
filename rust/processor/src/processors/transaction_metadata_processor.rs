// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    latest_version_tracker::{PartialBatch, VersionTrackerItem},
    models::transaction_metadata_model::{
        event_size_info::EventSize, transaction_size_info::TransactionSize,
        write_set_size_info::WriteSetSize,
    },
    schema,
};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::query_builder::QueryFragment;
use tracing::warn;

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
    last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    transaction_sizes: Vec<TransactionSize>,
    event_sizes: Vec<EventSize>,
    write_set_sizes: Vec<WriteSetSize>,
) {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Finished parsing, sending to DB",
    );
    let version_tracker_item = VersionTrackerItem::PartialBatch(PartialBatch {
        start_version,
        end_version,
        last_transaction_timestamp,
    });
    let tsi = db_writer.send_in_chunks(
        "transaction_size_info",
        transaction_sizes,
        insert_transaction_size_infos_query,
        version_tracker_item.clone(),
    );
    let esi = db_writer.send_in_chunks(
        "event_size_info",
        event_sizes,
        insert_event_size_infos_query,
        version_tracker_item.clone(),
    );
    let wssi = db_writer.send_in_chunks(
        "write_set_size_info",
        write_set_sizes,
        insert_write_set_size_infos_query,
        version_tracker_item,
    );

    tokio::join!(tsi, esi, wssi);
}

pub fn insert_transaction_size_infos_query(
    items_to_insert: &[TransactionSize],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::transaction_size_info::dsl::*;

    diesel::insert_into(schema::transaction_size_info::table)
        .values(items_to_insert)
        .on_conflict(transaction_version)
        .do_nothing()
}

pub fn insert_event_size_infos_query(
    items_to_insert: &[EventSize],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::event_size_info::dsl::*;

    diesel::insert_into(schema::event_size_info::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, index))
        .do_nothing()
}

pub fn insert_write_set_size_infos_query(
    items_to_insert: &[WriteSetSize],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::write_set_size_info::dsl::*;

    diesel::insert_into(schema::write_set_size_info::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, index))
        .do_nothing()
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
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
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

        insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            last_transaction_timestamp.clone(),
            transaction_sizes,
            event_sizes,
            write_set_sizes,
        )
        .await;
        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_channel_insertion_duration_in_secs,
            last_transaction_timestamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
