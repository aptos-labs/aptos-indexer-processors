// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::{
        block_metadata_transactions::{BlockMetadataTransaction, BlockMetadataTransactionModel}, move_modules::MoveModule, move_tables::{CurrentTableItem, TableItem, TableMetadata}, new_transactions::TransactionModel, parquet_move_resources::MoveResource, parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel}
    }, parquet_manager::ParquetData, utils::database::PgDbPool
    // schemas::default_schemas::new_transactions::TRANSACTION_SCHEMA,
};
use ahash::AHashMap;

use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;

use std::{fmt::Debug, fmt::{Formatter, Result}};
use tracing::{error, info};
use tokio::io::{self, AsyncReadExt};
use parquet::{file::{properties::WriterProperties}};
use google_cloud_storage::{
    client::Client,
    http::{objects::upload::{Media, UploadObjectRequest, UploadType}, Error as StorageError},
};
use serde::{Deserialize, Serialize};
use anyhow::{Result as AnyhowResult, anyhow};
use hyper::Body;
use std::path::PathBuf;
use tokio::fs::File as TokioFile;
use tokio::time::{timeout, Duration};
use arrow::error::ArrowError;


const MAX_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024; // TODO: revisit this is local file size limit which needs to be dynamic per struct type
const TABLE: &str = "transactions";


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetProcessorConfig {
    pub bucket: String,
    pub google_application_credentials: Option<String>,
    pub is_parquet_backfill_enabled: bool,
}

pub struct ParquetProcessor {
    connection_pool: PgDbPool,
    parquet_manager_sender: AsyncSender<ParquetData>,
}

impl ParquetProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        config: ParquetProcessorConfig,
        parquet_manager_sender: AsyncSender<ParquetData>
    ) -> Self {
        
        tracing::info!("init ParquetProcessor");

        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }

        Self {
            connection_pool,
            parquet_manager_sender,
        }
    }
}

impl Debug for ParquetProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of channel: {:?}  }}",
            &self.parquet_manager_sender.capacity()
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>, 
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
        client: &Client,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start: std::time::Instant = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (
            write_set_changes, move_resources,
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_duration_in_secs: f64 = 0.0;
        let parquet_data = ParquetData {
            data: move_resources,  
            last_transaction_timestamp: last_transaction_timestamp.clone(),
        };
        self.parquet_manager_sender
            .send(parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;
        
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            last_transaction_timestamp,
            db_insertion_duration_in_secs,
            parquet_insertion_duration_in_secs: None,
        })
    }

    
    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

/**
 * Phase 1 is to migrate table that we know for sure we will deprecate and is huge in size.
 */
pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<WriteSetChangeModel>,
    Vec<MoveResource>,
) {
    let (txns, block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(&transactions);
    
    let mut move_modules = vec![];
    let mut move_resources = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata = AHashMap::new();

    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
            WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
            WriteSetChangeDetail::Table(item, current_item, metadata) => {
                table_items.push(item.clone());
                current_table_items.insert(
                    (
                        current_item.table_handle.clone(),
                        current_item.key_hash.clone(),
                    ),
                    current_item.clone(),
                );
                if let Some(meta) = metadata {
                    table_metadata.insert(meta.handle.clone(), meta.clone());
                }
            },
        }
    }

    (
        write_set_changes,
        move_resources,
    )
}