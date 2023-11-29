// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    entity::events::{self, ActiveModel as EventsActiveModel},
    utils::database::PgDbPool,
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction as TransactionPB};
use async_trait::async_trait;
use migration::{
    sea_orm::{DatabaseConnection, EntityTrait},
    sea_query,
};
use std::fmt::Debug;
use tracing::error;

pub struct EventsProcessorV2 {
    connection_pool: PgDbPool,
    cockroach_connection_pool: DatabaseConnection,
}

impl EventsProcessorV2 {
    pub fn new(connection_pool: PgDbPool, cockroach_connection_pool: DatabaseConnection) -> Self {
        tracing::info!("init EventsProcessorV2");
        Self {
            connection_pool,
            cockroach_connection_pool,
        }
    }

    // Additional methods specific to event processing...
}

impl Debug for EventsProcessorV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "EventsProcessorV2 {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for EventsProcessorV2 {
    fn name(&self) -> &'static str {
        ProcessorName::EventsProcessorV2.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<TransactionPB>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
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

            let txn_events = EventsActiveModel::from_events(raw_events, txn_version, block_height);
            events.extend(txn_events);
        }
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();
        let insert_result = events::Entity::insert_many(events)
            .on_conflict(
                sea_query::OnConflict::columns([
                    events::Column::TransactionVersion,
                    events::Column::EventIndex,
                ])
                .do_nothing()
                .to_owned(),
            )
            .do_nothing()
            .exec(&self.cockroach_connection_pool.clone())
            .await;
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        if let Err(err) = insert_result {
            error!(
                start_version = start_version,
                end_version = end_version,
                processor_name = self.name(),
                "[Parser] Error inserting {} to db: {:?}",
                "transactions",
                err
            );

            bail!(format!(
                "Error inserting {} to db. Processor {}. Start {}. End {}. Error {:?}",
                "transactions",
                self.name(),
                start_version,
                end_version,
                err
            ));
        }
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs,
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
