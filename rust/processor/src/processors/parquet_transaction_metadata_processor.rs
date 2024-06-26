// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessorName, ProcessorTrait};
use crate::{
    bq_analytics::{
        generic_parquet_processor::ParquetDataGeneric,
        parquet_handler::create_parquet_handler_loop, ParquetProcessingResult,
    },
    db::common::models::transaction_metadata_model::{
        parquet_event_size_info::EventSize, parquet_transaction_size_info::TransactionSize,
        parquet_write_set_size_info::WriteSetSize,
    },
    gap_detectors::ProcessingResult,
    utils::database::ArcDbPool,
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::warn;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetTransactionMetadataProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
}

pub struct ParquetTransactionMetadataProcessor {
    connection_pool: ArcDbPool,
    transaction_size_info_sender: AsyncSender<ParquetDataGeneric<TransactionSize>>,
    event_size_sender: AsyncSender<ParquetDataGeneric<EventSize>>,
    ws_size_sender: AsyncSender<ParquetDataGeneric<WriteSetSize>>,
}

impl ParquetTransactionMetadataProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetTransactionMetadataProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var(
                crate::processors::GOOGLE_APPLICATION_CREDENTIALS,
                credentials,
            );
        }

        let transaction_size_info_sender = create_parquet_handler_loop::<TransactionSize>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTransactionMetadataProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
        );

        let event_size_sender = create_parquet_handler_loop::<EventSize>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTransactionMetadataProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
        );

        let ws_size_sender = create_parquet_handler_loop::<WriteSetSize>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTransactionMetadataProcessor.into(),
            config.bucket_name.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
        );

        Self {
            connection_pool,
            transaction_size_info_sender,
            event_size_sender,
            ws_size_sender,
        }
    }
}

impl Debug for ParquetTransactionMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetTransactionMetadataProcessor {{ capacity of tsi channel: {:?}, capacity of es channel: {:?}, capacity of ws channel: {:?} }}",
            &self.transaction_size_info_sender.capacity(),
            &self.event_size_sender.capacity(),
            &self.ws_size_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetTransactionMetadataProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::TransactionMetadataProcessor.into()
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

        let mut transaction_sizes = vec![];
        let mut event_sizes = vec![];
        let mut write_set_sizes = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let size_info = match txn.size_info.as_ref() {
                Some(size_info) => size_info,
                None => {
                    warn!(version = txn.version, "Transaction size info not found");
                    continue;
                },
            };
            transaction_sizes.push(TransactionSize::from_transaction_info(
                size_info,
                txn_version,
            ));
            transaction_version_to_struct_count
                .entry(txn_version)
                .and_modify(|e| *e += 1)
                .or_insert(1);

            for (index, event_size_info) in size_info.event_size_info.iter().enumerate() {
                event_sizes.push(EventSize::from_event_size_info(
                    event_size_info,
                    txn_version,
                    index as i64,
                ));
            }
            transaction_version_to_struct_count
                .entry(txn_version)
                .and_modify(|e| *e += event_sizes.len() as i64)
                .or_insert(1);

            for (index, write_set_size_info) in size_info.write_op_size_info.iter().enumerate() {
                write_set_sizes.push(WriteSetSize::from_transaction_info(
                    write_set_size_info,
                    txn_version,
                    index as i64,
                ));
            }
            transaction_version_to_struct_count
                .entry(txn_version)
                .and_modify(|e| *e += write_set_sizes.len() as i64)
                .or_insert(1);
        }

        let tsi = ParquetDataGeneric {
            data: transaction_sizes,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };

        self.transaction_size_info_sender
            .send(tsi)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let esi = ParquetDataGeneric {
            data: event_sizes,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };

        self.event_size_sender
            .send(esi)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let wssi = ParquetDataGeneric {
            data: write_set_sizes,
            last_transaction_timestamp: last_transaction_timestamp.clone(),
            transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            first_txn_version: start_version,
            last_txn_version: end_version,
        };

        self.ws_size_sender
            .send(wssi)
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

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
