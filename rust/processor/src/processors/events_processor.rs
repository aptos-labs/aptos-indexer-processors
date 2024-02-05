// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::events_models::events::EventModel,
    schema,
    utils::database::{execute_in_chunks, PgDbPool, PgPoolConnection},
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use field_count::FieldCount;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct EventStreamSchema {
    chain_id: u64,
    events: Vec<String>,
    transaction_version: i64,
    timestamp: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventProcessorConfig {
    pub pubsub_topic_name: String,
    pub google_application_credentials: Option<String>,
}

pub struct EventsProcessor {
    publisher: Publisher,
    connection_pool: PgDbPool,
}

impl EventsProcessor {
    pub fn new(connection_pool: PgDbPool, publisher: Publisher) -> Self {
        Self {
            connection_pool,
            publisher,
        }
    }
}

impl Debug for EventsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "EventsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
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
    execute_in_chunks(conn, insert_events_query, events, EventModel::field_count()).await?;
    Ok(())
}

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::events::dsl::*;
    (
        diesel::insert_into(schema::events::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                indexed_type.eq(excluded(indexed_type)),
            )),
        None,
    )
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
        db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;
        let mut events = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
            let pubsub_message = events_to_pubsub_message(db_chain_id.unwrap(), &txn_events, txn);
            self.publisher
                .publish(pubsub_message)
                .await
                .get()
                .await
                .unwrap_or_else(|e| {
                    error!(
                        start_version = start_version,
                        end_version = end_version,
                        processor_name = self.name(),
                        error = ?e,
                        "Error publishing to pubsub",
                    );
                    panic!();
                });
            events.extend(txn_events);
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result =
            insert_to_db(&mut conn, self.name(), start_version, end_version, events).await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
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

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

pub fn events_to_pubsub_message(
    chain_id: u64,
    events: &Vec<EventModel>,
    txn: &Transaction,
) -> PubsubMessage {
    let transaction_version = txn.version as i64;
    let txn_timestamp = txn
        .timestamp
        .as_ref()
        .expect("Transaction timestamp doesn't exist!")
        .seconds;
    let pubsub_message = EventStreamSchema {
        chain_id,
        events: events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap_or_default())
            .collect(),
        transaction_version,
        timestamp: NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
            .unwrap_or_default()
            .to_string(),
    };

    PubsubMessage {
        data: serde_json::to_string(&pubsub_message)
            .unwrap_or_default()
            .into(),
        ..Default::default()
    }
}
