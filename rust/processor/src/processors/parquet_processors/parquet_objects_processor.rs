// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{UploadIntervalConfig, GOOGLE_APPLICATION_CREDENTIALS};
use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::object_models::{
        parquet_v2_objects::Object,
        v2_object_utils::{ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata},
    },
    gap_detectors::ProcessingResult,
    processors::{ProcessorName, ProcessorTrait},
    utils::{database::ArcDbPool, util::standardize_address},
    IndexerGrpcProcessorConfig,
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetObjectsProcessorConfig {
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}
pub struct ParquetObjectsProcessor {
    connection_pool: ArcDbPool,
    config: ParquetObjectsProcessorConfig,
    objects_sender: AsyncSender<ParquetDataGeneric<Object>>,
}

impl UploadIntervalConfig for ParquetObjectsProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

impl ParquetObjectsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetObjectsProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS, credentials);
        }

        let objects_sender = create_parquet_handler_loop::<Object>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetObjectsProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            config,
            objects_sender,
        }
    }
}

impl Debug for ParquetObjectsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetObjectsProcessor {{ capacity of objects channel: {:?} }} ",
            &self.objects_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetObjectsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetObjectsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut conn = self.get_conn().await;
        let query_retries = self.config.query_retries;
        let query_retry_delay_ms = self.config.query_retry_delay_ms;

        // Moving object handling here because we need a single object
        // map through transactions for lookups
        let mut all_objects = vec![];
        let mut all_current_objects = AHashMap::new();
        let mut object_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

        for txn in &transactions {
            let txn_version = txn.version as i64;
            let timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!");
            #[allow(deprecated)]
            let block_timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
                .expect("Txn Timestamp is invalid!");
            let changes = &txn
                .info
                .as_ref()
                .unwrap_or_else(|| {
                    panic!(
                        "Transaction info doesn't exist! Transaction {}",
                        txn_version
                    )
                })
                .changes;

            // First pass to get all the object cores
            for wsc in changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(object_with_metadata) =
                        ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                    {
                        // Object core is the first struct that we need to get
                        object_metadata_helper.insert(address.clone(), ObjectAggregatedData {
                            object: object_with_metadata,
                            ..ObjectAggregatedData::default()
                        });
                    }
                }
            }

            // Second pass to construct the object data
            for (index, wsc) in changes.iter().enumerate() {
                let index: i64 = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(inner) => {
                        if let Some((object, current_object)) = &Object::from_write_resource(
                            inner,
                            txn_version,
                            index,
                            &object_metadata_helper,
                            block_timestamp,
                        )
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    Change::DeleteResource(inner) => {
                        // Passing all_current_objects into the function so that we can get the owner of the deleted
                        // resource if it was handled in the same batch
                        if let Some((object, current_object)) = Object::from_delete_resource(
                            inner,
                            txn_version,
                            index,
                            &all_current_objects,
                            &mut conn,
                            query_retries,
                            query_retry_delay_ms,
                            block_timestamp,
                        )
                        .await
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    _ => {},
                };
            }
        }

        let objects_parquet_data = ParquetDataGeneric {
            data: all_objects,
            transaction_version_to_struct_count,
        };

        self.objects_sender
            .send(objects_parquet_data)
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
