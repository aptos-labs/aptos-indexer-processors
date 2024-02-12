// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{models::events_models::events::EventModel, utils::database::PgDbPool};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::broadcast::Sender;

pub struct EventStreamProcessor {
    connection_pool: PgDbPool,
    channel: Sender<EventModel>,
}

impl EventStreamProcessor {
    pub fn new(connection_pool: PgDbPool, channel: Sender<EventModel>) -> Self {
        Self {
            connection_pool,
            channel,
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
        _: Option<u64>,
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
            for event in txn_events {
                self.channel.send(event)?;
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs: 0.0,
            last_transaction_timstamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
