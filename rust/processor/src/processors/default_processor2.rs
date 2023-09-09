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
    mutation::insert_or_update,
};
use std::{collections::HashMap, fmt::Debug};
use tracing::info;

pub const NAME: &str = "default_processor2";
pub const CHUNK_SIZE: usize = 1000;

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

        let mut mutations = Vec::new();
        let mut table_metadata = HashMap::new();

        for transaction in transactions {
            let (txn, txn_detail, event, write_set_change, wsc_detail) =
                TransactionModel::from_transaction(&transaction);

            let _ = wsc_detail.iter().map(|detail| {
                if let WriteSetChangeDetail::Table(_, _, metadata) = detail {
                    if let Some(meta) = metadata {
                        table_metadata.insert(meta.handle.clone(), meta.clone());
                    }
                }
            });

            mutations.push(insert_or_update(
                "transactions",
                &[
                    "transaction_version",
                    "transaction",
                    "transaction_detail",
                    "event",
                    "write_set_change",
                    "write_set_change_details",
                ],
                &[
                    &(transaction.version as i64),
                    &(serde_json::to_string(&txn).unwrap_or_default()),
                    &(serde_json::to_string(&txn_detail).unwrap_or_default()),
                    &(serde_json::to_string(&event).unwrap_or_default()),
                    &(serde_json::to_string(&write_set_change).unwrap_or_default()),
                    &(serde_json::to_string(&wsc_detail).unwrap_or_default()),
                ],
            ));
        }

        for detail in table_metadata.into_values() {
            mutations.push(insert_or_update(
                "table_metadatas",
                &["handle", "key_type", "value_type"],
                &[&detail.handle, &detail.key_type, &detail.value_type],
            ));
        }

        let chunks = mutations.chunks(CHUNK_SIZE).map(|chunk| chunk.to_vec());
        for chunk in chunks {
            if let Some(commit_timestamp) = client.apply(chunk).await? {
                info!(
                    spanner_info = self.spanner_db.clone(),
                    commit_timestamp = commit_timestamp.seconds,
                    start_version = start_version,
                    end_version = end_version,
                    "Inserted or updated transaction",
                );
            }
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
