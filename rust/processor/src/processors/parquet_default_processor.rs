// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessorName, ProcessorTrait};
use crate::{
    gap_detectors::ProcessingResult,
    models::default_models::{
        move_tables::{CurrentTableItem, TableMetadata},
        parquet_move_resources::MoveResource,
        parquet_transactions::{Transaction as ParquetTransaction, TransactionModel},
        parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    parquet_handler::create_parquet_handler_loop,
    parquet_processors::{generic_parquet_processor::ParquetDataGeneric, ParquetProcessingResult},
    utils::database::PgDbPool,
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter, Result};

pub const RESOURCE_TYPES: [&str; 3] = ["transaction", "move_resource", "write_set_changes"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultParquetProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub parquet_handler_response_channel_size: usize,
}

pub struct DefaultParquetProcessor {
    connection_pool: PgDbPool,
    transaction_sender: AsyncSender<ParquetDataGeneric<ParquetTransaction>>,
    move_resource_sender: AsyncSender<ParquetDataGeneric<MoveResource>>,
    wsc_sender: AsyncSender<ParquetDataGeneric<WriteSetChangeModel>>,
}

// TODO: Since each table item has different size allocated, the pace of being backfilled to PQ varies a lot.
// Maybe we can have also have a way to configure different starting version for each table later.
impl DefaultParquetProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        config: DefaultParquetProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }

        let transaction_sender = create_parquet_handler_loop::<ParquetTransaction>(
            new_gap_detector_sender.clone(),
            ProcessorName::DefaultParquetProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
        );

        let move_resource_sender = create_parquet_handler_loop::<MoveResource>(
            new_gap_detector_sender.clone(),
            ProcessorName::DefaultParquetProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
        );

        let wsc_sender = create_parquet_handler_loop::<WriteSetChangeModel>(
            new_gap_detector_sender.clone(),
            ProcessorName::DefaultParquetProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
        );

        Self {
            connection_pool,
            transaction_sender,
            move_resource_sender,
            wsc_sender,
        }
    }
}

impl Debug for DefaultParquetProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of t channel: {:?}, capacity of mr channel: {:?}, capacity of wsc channel: {:?} }}",
            &self.transaction_sender.capacity(),
            &self.move_resource_sender.capacity(),
            &self.wsc_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for DefaultParquetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::DefaultParquetProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let ((mr, wsc, t), transaction_version_to_struct_count) =
            tokio::task::spawn_blocking(move || process_transactions(transactions))
                .await
                .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let mr_parquet_data = ParquetDataGeneric {
            data: mr,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };

        self.move_resource_sender
            .send(mr_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let wsc_parquet_data = ParquetDataGeneric {
            data: wsc,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };
        self.wsc_sender
            .send(wsc_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let t_parquet_data = ParquetDataGeneric {
            data: t,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };
        self.transaction_sender
            .send(t_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        Ok(ProcessingResult::ParquetProcessingResult(
            ParquetProcessingResult {
                start_version: start_version as i64,
                end_version: end_version as i64,
                last_transaction_timestamp: last_transaction_timestamp.clone(),
                txn_version_to_struct_count: AHashMap::new(),
            },
        ))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    (
        Vec<MoveResource>,
        Vec<WriteSetChangeModel>,
        Vec<TransactionModel>,
    ),
    AHashMap<i64, i64>,
) {
    let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();
    let (txns, _block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(
            &transactions,
            &mut transaction_version_to_struct_count,
        );

    let mut move_modules = vec![];
    let mut move_resources = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata: AHashMap<String, TableMetadata> = AHashMap::new();

    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => {
                move_modules.push(module.clone());
                // transaction_version_to_struct_count.entry(module.transaction_version).and_modify(|e| *e += 1); // TODO: uncomment in Tranche2
            },
            WriteSetChangeDetail::Resource(resource) => {
                transaction_version_to_struct_count
                    .entry(resource.txn_version)
                    .and_modify(|e| *e += 1);
                move_resources.push(resource);
            },
            WriteSetChangeDetail::Table(item, current_item, metadata) => {
                table_items.push(item);
                // transaction_version_to_struct_count.entry(item.transaction_version).and_modify(|e| *e += 1); // TODO: uncomment in Tranche2

                current_table_items.insert(
                    (
                        current_item.table_handle.clone(),
                        current_item.key_hash.clone(),
                    ),
                    current_item,
                );
                // transaction_version_to_struct_count.entry(current_item.last_transaction_version).and_modify(|e| *e += 1); // TODO: uncomment in Tranche2

                if let Some(meta) = metadata {
                    table_metadata.insert(meta.handle.clone(), meta);
                    // transaction_version_to_struct_count.entry(current_item.last_transaction_version).and_modify(|e| *e += 1); // TODO: uncomment in Tranche2
                }
            },
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<CurrentTableItem>>();
    let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

    (
        (move_resources, write_set_changes, txns),
        transaction_version_to_struct_count,
    )
}
