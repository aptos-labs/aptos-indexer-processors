// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::utils::database::PgDbPool;
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    reader::AsyncIterator,
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
        _: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        // Create spanner client
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(self.spanner_db.clone(), config).await?;

        let stmt = Statement::new("SELECT * FROM transactions LIMIT 100");
        let mut tx = client.single().await?;
        let mut iter = tx.query(stmt).await?;
        while let Some(row) = iter.next().await? {
            let guild_id = row.column_by_name::<i64>("transaction_version");
            info!("guild_id: {:?}", guild_id);
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
