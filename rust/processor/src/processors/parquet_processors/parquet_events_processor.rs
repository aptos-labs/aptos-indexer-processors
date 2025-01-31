// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::{
        common::models::event_models::raw_events::parse_events,
        parquet::models::event_models::parquet_events::EventPQ,
    },
    gap_detectors::ProcessingResult,
    processors::{parquet_processors::ParquetProcessorTrait, ProcessorName, ProcessorTrait},
    utils::database::ArcDbPool,
};
use ahash::AHashMap;
use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParquetEventsProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetEventsProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetEventsProcessor {
    connection_pool: ArcDbPool,
    event_sender: AsyncSender<ParquetDataGeneric<EventPQ>>,
}

impl ParquetEventsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetEventsProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let event_sender = create_parquet_handler_loop::<EventPQ>(
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
            event_sender,
        }
    }
}

impl Debug for ParquetEventsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of event channel: {:?}}}",
            &self.event_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetEventsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetEventsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp;

        let (transaction_version_to_struct_count, events) =
            process_transactions_parquet(transactions);

        let event_parquet_data = ParquetDataGeneric { data: events };

        self.event_sender
            .send(event_parquet_data)
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

pub fn process_transactions_parquet(
    transactions: Vec<Transaction>,
) -> (AHashMap<i64, i64>, Vec<EventPQ>) {
    let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

    let mut events = vec![];
    for txn in &transactions {
        let txn_version = txn.version as i64;
        let txn_events: Vec<EventPQ> = parse_events(txn, "ParquetEventsProcessor")
            .into_iter()
            .map(|e| e.into())
            .collect();
        transaction_version_to_struct_count
            .entry(txn_version)
            .and_modify(|e| *e += txn_events.len() as i64)
            .or_insert(txn_events.len() as i64);

        events.extend(txn_events);
    }
    (transaction_version_to_struct_count, events)
}
