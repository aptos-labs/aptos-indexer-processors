// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::default_models::{
        parquet_move_modules::MoveModule,
        parquet_move_resources::MoveResource,
        parquet_move_tables::TableItem,
        parquet_transactions::{Transaction as ParquetTransaction, TransactionModel},
        parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    gap_detectors::ProcessingResult,
    processors::{parquet_processors::ParquetProcessorTrait, ProcessorName, ProcessorTrait},
    utils::database::ArcDbPool,
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter, Result},
    time::Duration,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetDefaultProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}
impl ParquetProcessorTrait for ParquetDefaultProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetDefaultProcessor {
    connection_pool: ArcDbPool,
    transaction_sender: AsyncSender<ParquetDataGeneric<ParquetTransaction>>,
    move_resource_sender: AsyncSender<ParquetDataGeneric<MoveResource>>,
    wsc_sender: AsyncSender<ParquetDataGeneric<WriteSetChangeModel>>,
    table_item_sender: AsyncSender<ParquetDataGeneric<TableItem>>,
    move_module_sender: AsyncSender<ParquetDataGeneric<MoveModule>>,
}

// TODO: Since each table item has different size allocated, the pace of being backfilled to PQ varies a lot.
// Maybe we can have also have a way to configure different starting version for each table later.
impl ParquetDefaultProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetDefaultProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let transaction_sender = create_parquet_handler_loop::<ParquetTransaction>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetDefaultProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        let move_resource_sender = create_parquet_handler_loop::<MoveResource>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetDefaultProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        let wsc_sender = create_parquet_handler_loop::<WriteSetChangeModel>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetDefaultProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        let table_item_sender = create_parquet_handler_loop::<TableItem>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetDefaultProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );
        let move_module_sender = create_parquet_handler_loop::<MoveModule>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetDefaultProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            transaction_sender,
            move_resource_sender,
            wsc_sender,
            table_item_sender,
            move_module_sender,
        }
    }
}

impl Debug for ParquetDefaultProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of trnasactions channel: {:?}, capacity of move resource channel: {:?}, capacity of wsc channel: {:?}, capacity of table items channel: {:?}, capacity of move_module channel: {:?}}}",
            &self.transaction_sender.capacity(),
            &self.move_resource_sender.capacity(),
            &self.wsc_sender.capacity(),
            &self.table_item_sender.capacity(),
            &self.move_module_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetDefaultProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetDefaultProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (
            (move_resources, write_set_changes, transactions, table_items, move_modules),
            transaction_version_to_struct_count,
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let mr_parquet_data = ParquetDataGeneric {
            data: move_resources,
        };

        self.move_resource_sender
            .send(mr_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let wsc_parquet_data = ParquetDataGeneric {
            data: write_set_changes,
        };
        self.wsc_sender
            .send(wsc_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let t_parquet_data = ParquetDataGeneric { data: transactions };
        self.transaction_sender
            .send(t_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let ti_parquet_data = ParquetDataGeneric { data: table_items };

        self.table_item_sender
            .send(ti_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let mm_parquet_data = ParquetDataGeneric { data: move_modules };

        self.move_module_sender
            .send(mm_parquet_data)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        Ok(ProcessingResult::ParquetProcessingResult(
            ParquetProcessingResult {
                start_version: start_version as i64,
                end_version: end_version as i64,
                last_transaction_timestamp: last_transaction_timestamp.clone(),
                txn_version_to_struct_count: Some(transaction_version_to_struct_count),
                parquet_processed_structs: None,
                table_name: "".to_string(),
            },
        ))
    }

    fn connection_pool(&self) -> &ArcDbPool {
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
        Vec<TableItem>,
        Vec<MoveModule>,
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

    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => {
                let txn_version = module.txn_version;
                move_modules.push(module);
                transaction_version_to_struct_count
                    .entry(txn_version)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            },
            WriteSetChangeDetail::Resource(resource) => {
                transaction_version_to_struct_count
                    .entry(resource.txn_version)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
                move_resources.push(resource);
            },
            WriteSetChangeDetail::Table(item, _current_item, _) => {
                let txn_version = item.txn_version;
                transaction_version_to_struct_count
                    .entry(txn_version)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
                table_items.push(item);
            },
        }
    }

    (
        (
            move_resources,
            write_set_changes,
            txns,
            table_items,
            move_modules,
        ),
        transaction_version_to_struct_count,
    )
}
