// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::transaction_metadata_model::parquet_write_set_size_info::WriteSetSize,
    gap_detectors::ProcessingResult,
    processors::{parquet_processors::ParquetProcessorTrait, ProcessorName, ProcessorTrait},
    utils::{database::ArcDbPool, util::parse_timestamp},
};
use ahash::AHashMap;
use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};
use tracing::warn;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetTransactionMetadataProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetTransactionMetadataProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetTransactionMetadataProcessor {
    connection_pool: ArcDbPool,
    write_set_size_info_sender: AsyncSender<ParquetDataGeneric<WriteSetSize>>,
}

impl ParquetTransactionMetadataProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetTransactionMetadataProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let write_set_size_info_sender = create_parquet_handler_loop::<WriteSetSize>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTransactionMetadataProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );
        Self {
            connection_pool,
            write_set_size_info_sender,
        }
    }
}

impl Debug for ParquetTransactionMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetTransactionMetadataProcessor {{ capacity of write set size info channel: {:?} }}",
            self.write_set_size_info_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetTransactionMetadataProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetTransactionMetadataProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

        let mut write_set_sizes = vec![];

        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
            let size_info = match txn.size_info.as_ref() {
                Some(size_info) => size_info,
                None => {
                    warn!(version = txn.version, "Transaction size info not found");
                    continue;
                },
            };
            for (index, write_set_size_info) in size_info.write_op_size_info.iter().enumerate() {
                write_set_sizes.push(WriteSetSize::from_transaction_info(
                    write_set_size_info,
                    txn_version,
                    index as i64,
                    block_timestamp,
                ));
                transaction_version_to_struct_count
                    .entry(txn_version)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }
        }

        let write_set_size_info_parquet_data = ParquetDataGeneric {
            data: write_set_sizes,
        };

        self.write_set_size_info_sender
            .send(write_set_size_info_parquet_data)
            .await
            .context("Error sending write set size info to parquet handler")?;

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
