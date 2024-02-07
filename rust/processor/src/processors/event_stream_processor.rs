// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{models::events_models::events::EventModel, utils::database::PgDbPool};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
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
pub struct EventStreamProcessorConfig {
    pub pubsub_topic_name: String,
    pub google_application_credentials: Option<String>,
}

pub struct EventStreamProcessor {
    publisher: Publisher,
    connection_pool: PgDbPool,
}

impl EventStreamProcessor {
    pub fn new(connection_pool: PgDbPool, publisher: Publisher) -> Self {
        Self {
            connection_pool,
            publisher,
        }
    }
}

impl Debug for EventStreamProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "EventStreamProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for EventStreamProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::EventStreamProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
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
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs: 0.0,
        })
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
