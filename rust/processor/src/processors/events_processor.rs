// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    latest_version_tracker::{PartialBatch, VersionTrackerItem},
    models::events_models::events::EventModel,
    schema,
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
};

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

pub fn insert_events_query(
    items_to_insert: &[EventModel],
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use schema::events::dsl::*;

    diesel::insert_into(schema::events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_update()
        .set((
            inserted_at.eq(excluded(inserted_at)),
            indexed_type.eq(excluded(indexed_type)),
        ))
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

        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Finished parsing, sending to DB",
        );
        let version_tracker_item = VersionTrackerItem::PartialBatch(PartialBatch {
            start_version,
            end_version,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
        });
        self.db_writer()
            .send_in_chunks("events", events, insert_events_query, version_tracker_item)
            .await;

        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_channel_insertion_duration_in_secs,
            last_transaction_timestamp,
        })
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
