// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::ParquetProcessorTrait;
use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::ans_models::{
        ans_lookup::CurrentAnsPrimaryName,
        parquet_ans_lookup_v2::{AnsPrimaryNameV2, CurrentAnsPrimaryNameV2},
    },
    gap_detectors::ProcessingResult,
    processors::{ProcessorName, ProcessorTrait},
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, database::ArcDbPool},
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, Transaction,
};
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetAnsProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub ans_v1_primary_names_table_handle: String,
    pub ans_v1_name_records_table_handle: String,
    pub ans_v2_contract_address: String,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetAnsProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetAnsProcessor {
    connection_pool: ArcDbPool,
    config: ParquetAnsProcessorConfig,
    ans_primary_name_v2_sender: AsyncSender<ParquetDataGeneric<AnsPrimaryNameV2>>,
}

impl ParquetAnsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetAnsProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let ans_primary_name_v2_sender = create_parquet_handler_loop::<AnsPrimaryNameV2>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetAnsProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            config,
            ans_primary_name_v2_sender,
        }
    }
}

impl Debug for ParquetAnsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetAnsProcessor {{ capacity of ans_primary_name_v2 channel: {:?}}}",
            &self.ans_primary_name_v2_sender.capacity()
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetAnsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetAnsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

        let all_ans_primary_names_v2 = parse_ans(
            &transactions,
            &mut transaction_version_to_struct_count,
            self.config.ans_v1_primary_names_table_handle.clone(),
            self.config.ans_v2_contract_address.clone(),
        );

        let ans_primary_name_v2_parquet_data = ParquetDataGeneric {
            data: all_ans_primary_names_v2,
        };

        self.ans_primary_name_v2_sender
            .send(ans_primary_name_v2_parquet_data)
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

fn parse_ans(
    transactions: &[Transaction],
    transaction_version_to_struct_count: &mut AHashMap<i64, i64>,
    ans_v1_primary_names_table_handle: String,
    ans_v2_contract_address: String,
) -> Vec<AnsPrimaryNameV2> {
    let mut all_ans_primary_names_v2 = vec![];

    for transaction in transactions {
        let txn_version = transaction.version as i64;
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["AnsProcessor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        #[allow(deprecated)]
        let block_timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
            .expect("Txn Timestamp is invalid!");
        // Extracts from user transactions. Other transactions won't have any ANS changes
        if let TxnData::User(user_txn) = txn_data {
            // Parse V2 ANS Events. We only care about the following events:
            // 1. RenewNameEvents: helps to fill in metadata for name records with updated expiration time
            // 2. SetReverseLookupEvents: parse to get current_ans_primary_names
            for (event_index, event) in user_txn.events.iter().enumerate() {
                if let Some((_, ans_lookup_v2)) =
                    CurrentAnsPrimaryNameV2::parse_v2_primary_name_record_from_event(
                        event,
                        txn_version,
                        event_index as i64,
                        &ans_v2_contract_address,
                        block_timestamp,
                    )
                    .unwrap()
                {
                    all_ans_primary_names_v2.push(ans_lookup_v2);
                    transaction_version_to_struct_count
                        .entry(txn_version)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                }
            }

            // Parse V1 ANS write set changes
            for (wsc_index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteTableItem(table_item) => {
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                                .unwrap_or_else(|e| {
                                    error!(
                                error = ?e,
                                "Error parsing ANS v1 primary name from write table item"
                            );
                                    panic!();
                                })
                        {
                            // Include all v1 primary names in v2 data
                            let (_, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name.clone(), primary_name.clone(), block_timestamp);
                            all_ans_primary_names_v2.push(primary_name_v2);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                    },
                    WriteSetChange::DeleteTableItem(table_item) => {
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                                .unwrap_or_else(|e| {
                                    error!(
                                error = ?e,
                                "Error parsing ANS v1 primary name from delete table item"
                            );
                                    panic!();
                                })
                        {
                            // Include all v1 primary names in v2 data
                            let (_, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name, primary_name, block_timestamp);
                            all_ans_primary_names_v2.push(primary_name_v2);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                    },
                    _ => continue,
                }
            }
        }
    }

    all_ans_primary_names_v2
}
