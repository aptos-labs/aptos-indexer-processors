// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    entity::transactions::{self, ActiveModel as TransactionsActiveModel},
    utils::database::PgDbPool,
};
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction as TransactionPB;
use async_trait::async_trait;
use migration::{
    sea_orm::{DatabaseConnection, EntityTrait},
    sea_query,
};
use std::fmt::Debug;
use tracing::error;

pub const NAME: &str = "transaction_processor_v2";
pub const CHUNK_SIZE: usize = 1000;

pub struct TransactionsProcessorV2 {
    connection_pool: PgDbPool,
    cockroach_connection_pool: DatabaseConnection,
}

impl TransactionsProcessorV2 {
    pub fn new(connection_pool: PgDbPool, cockroach_connection_pool: DatabaseConnection) -> Self {
        tracing::info!("init TransactionsProcessorV2");
        Self {
            connection_pool,
            cockroach_connection_pool,
        }
    }
}

impl Debug for TransactionsProcessorV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TransactionsProcessorV2 {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for TransactionsProcessorV2 {
    fn name(&self) -> &'static str {
        ProcessorName::TransactionsProcessorV2.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<TransactionPB>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let txns = TransactionsActiveModel::from_transactions(transactions);
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();
        let insert_result = transactions::Entity::insert_many(txns)
            .on_conflict(
                sea_query::OnConflict::columns([transactions::Column::TransactionVersion])
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
