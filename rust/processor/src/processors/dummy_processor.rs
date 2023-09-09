// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::{
        transactions::TransactionModel, write_set_changes::WriteSetChangeDetail,
    },
    utils::database::PgDbPool,
};
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    statement::Statement,
};
use std::{collections::HashMap, fmt::Debug, time::Instant};
use tracing::info;

pub const NAME: &str = "dummy_processor";
pub struct DummyProcessor {
    connection_pool: PgDbPool,
    spanner_db: String,
}

impl DummyProcessor {
    pub fn new(connection_pool: PgDbPool, spanner_db: String) -> Self {
        tracing::info!("init DummyProcessor");
        Self {
            connection_pool,
            spanner_db,
        }
    }
}

impl Debug for DummyProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DummyProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for DummyProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        // Create spanner client
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(self.spanner_db.clone(), config).await?;

        let mut table_metadata = HashMap::new();
        for transaction in transactions {
            let (_, _, _, _, wsc_detail) = TransactionModel::from_transaction(&transaction);
            let _ = wsc_detail.iter().map(|detail| {
                if let WriteSetChangeDetail::Table(_, _, metadata) = detail {
                    if let Some(meta) = metadata {
                        table_metadata.insert(meta.handle.clone(), meta.clone());
                    }
                }
            });
        }

        for metadata in table_metadata.into_values() {
            let start_time = Instant::now();
            let query = format!(
                "SELECT * FROM table_metadatas WHERE handle = '{}'",
                metadata.handle
            );
            let stmt = Statement::new(query.clone());
            let mut tx = client.single().await?;
            tx.query(stmt.clone()).await?;
            info!(
                time_elapsed = start_time.elapsed().as_nanos(),
                query = query,
                "Queried transaction"
            );
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
