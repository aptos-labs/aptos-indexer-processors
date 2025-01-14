// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::{
        parquet::models::user_transaction_models::parquet_user_transactions::UserTransaction,
        postgres::models::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
    },
    gap_detectors::ProcessingResult,
    processors::{parquet_processors::ParquetProcessorTrait, ProcessorName, ProcessorTrait},
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, database::ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Context;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParquetUserTransactionsProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetUserTransactionsProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetUserTransactionsProcessor {
    connection_pool: ArcDbPool,
    user_transactions_sender: AsyncSender<ParquetDataGeneric<UserTransaction>>,
}

impl ParquetUserTransactionsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetUserTransactionsProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let user_transactions_sender: AsyncSender<ParquetDataGeneric<UserTransaction>> =
            create_parquet_handler_loop::<UserTransaction>(
                new_gap_detector_sender.clone(),
                ProcessorName::ParquetUserTransactionsProcessor.into(),
                config.bucket_name.clone(),
                config.bucket_root.clone(),
                config.parquet_handler_response_channel_size,
                config.max_buffer_size,
                config.parquet_upload_interval_in_secs(),
            );

        Self {
            connection_pool,
            user_transactions_sender,
        }
    }
}

impl Debug for ParquetUserTransactionsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of user transactions channel: {:?} }}",
            &self.user_transactions_sender.capacity(),
        )
    }
}

pub async fn process_transactions(
    transactions: Vec<Transaction>,
) -> (Vec<UserTransaction>, AHashMap<i64, i64>) {
    let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();
    let mut user_transactions = vec![];
    for txn in &transactions {
        let txn_version = txn.version as i64;
        let block_height = txn.block_height as i64;
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        let txn_data = match txn.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["UserTransactionProcessor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        if let TxnData::User(inner) = txn_data {
            let fee_statement = inner.events.iter().find_map(|event| {
                let event_type = event.type_str.as_str();
                FeeStatement::from_event(event_type, &event.data, txn_version)
            });
            let user_transaction = UserTransaction::from_transaction(
                inner,
                transaction_info,
                fee_statement,
                txn.timestamp.as_ref().unwrap(),
                block_height,
                txn.epoch as i64,
                txn_version,
            );
            transaction_version_to_struct_count
                .entry(txn_version)
                .and_modify(|e| *e += 1)
                .or_insert(1);
            user_transactions.push(user_transaction);
        }
    }
    (user_transactions, transaction_version_to_struct_count)
}

#[async_trait]
impl ProcessorTrait for ParquetUserTransactionsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetUserTransactionsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp;
        let (user_transactions, transaction_version_to_struct_count) =
            process_transactions(transactions).await;

        let user_transaction_parquet_data = ParquetDataGeneric {
            data: user_transactions,
        };

        self.user_transactions_sender
            .send(user_transaction_parquet_data)
            .await
            .context("Failed to send to parquet manager")?;

        Ok(ProcessingResult::ParquetProcessingResult(
            ParquetProcessingResult {
                start_version: start_version as i64,
                end_version: end_version as i64,
                last_transaction_timestamp,
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
