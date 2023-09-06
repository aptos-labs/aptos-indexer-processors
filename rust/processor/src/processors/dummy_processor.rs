// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::utils::database::PgDbPool;
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    statement::Statement,
};
use std::fmt::Debug;
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

        for transaction in transactions {
            let query = format!(
                "SELECT * FROM transactions WHERE transaction_version >= '{}' AND transaction_version <= '{}'",
                start_version, transaction.version
            );
            let stmt = Statement::new(query.clone());
            let mut tx = client.single().await?;
            tx.query(stmt.clone()).await?;
            info!("Queried transaction {:?}", query);
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
