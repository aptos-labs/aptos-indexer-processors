// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::utils::database::PgDbPool;
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    mutation::insert_or_update,
};
use std::fmt::Debug;
use tracing::info;

pub const NAME: &str = "default_processor2";
pub struct DefaultProcessor2 {
    connection_pool: PgDbPool,
    spanner_db: String,
}

impl DefaultProcessor2 {
    pub fn new(connection_pool: PgDbPool, spanner_db: String) -> Self {
        tracing::info!("init DefaultProcessor2");
        Self {
            connection_pool,
            spanner_db,
        }
    }
}

impl Debug for DefaultProcessor2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultProcessor2 {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for DefaultProcessor2 {
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

        let mutations = transactions
            .iter()
            .map(|transaction| {
                insert_or_update(
                    "transactions",
                    &["transaction_version", "json_dump"],
                    &[&(transaction.version as i64), &"{\"test\": \"hi\"}"],
                )
            })
            .collect();

        if let Some(commit_timestamp) = client.apply(mutations).await? {
            info!(
                spanner_info = self.spanner_db.clone(),
                commit_timestamp = commit_timestamp.seconds,
                start_version = start_version,
                end_version = end_version,
                "Inserted or updated transaction",
            );
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
