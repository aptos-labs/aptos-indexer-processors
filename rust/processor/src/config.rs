// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    gap_detector::DEFAULT_GAP_DETECTION_BATCH_SIZE, processors::ProcessorConfig,
    transaction_filter::TransactionFilter, worker::Worker,
};
use ahash::AHashMap;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use server_framework::RunnableConfig;
use std::time::Duration;
use url::Url;

pub const QUERY_DEFAULT_RETRIES: u32 = 5;
pub const QUERY_DEFAULT_RETRY_DELAY_MS: u64 = 500;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcProcessorConfig {
    pub processor_config: ProcessorConfig,
    pub postgres_connection_string: String,
    // TODO: Add TLS support.
    pub indexer_grpc_data_service_address: Url,
    #[serde(flatten)]
    pub grpc_http2_config: IndexerGrpcHttp2Config,
    pub auth_token: String,
    // Version to start indexing from
    pub starting_version: Option<u64>,
    // Version to end indexing at
    pub ending_version: Option<u64>,
    // Number of tasks waiting to pull transaction batches from the channel and process them
    pub number_concurrent_processing_tasks: Option<usize>,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    pub db_pool_size: Option<u32>,
    // Maximum number of batches "missing" before we assume we have an issue with gaps and abort
    #[serde(default = "IndexerGrpcProcessorConfig::default_gap_detection_batch_size")]
    pub gap_detection_batch_size: u64,
    // Number of protobuff transactions to send per chunk to the processor tasks
    #[serde(default = "IndexerGrpcProcessorConfig::default_pb_channel_txn_chunk_size")]
    pub pb_channel_txn_chunk_size: usize,
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    pub enable_verbose_logging: Option<bool>,
    #[serde(default)]
    pub transaction_filter: TransactionFilter,
}

impl IndexerGrpcProcessorConfig {
    pub const fn default_gap_detection_batch_size() -> u64 {
        DEFAULT_GAP_DETECTION_BATCH_SIZE
    }

    pub const fn default_query_retries() -> u32 {
        QUERY_DEFAULT_RETRIES
    }

    pub const fn default_query_retry_delay_ms() -> u64 {
        QUERY_DEFAULT_RETRY_DELAY_MS
    }

    /// Make the default very large on purpose so that by default it's not chunked
    /// This prevents any unexpected changes in behavior
    pub const fn default_pb_channel_txn_chunk_size() -> usize {
        100_000
    }
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcProcessorConfig {
    async fn run(&self) -> Result<()> {
        let mut worker = Worker::new(
            self.processor_config.clone(),
            self.postgres_connection_string.clone(),
            self.indexer_grpc_data_service_address.clone(),
            self.grpc_http2_config.clone(),
            self.auth_token.clone(),
            self.starting_version,
            self.ending_version,
            self.number_concurrent_processing_tasks,
            self.db_pool_size,
            self.gap_detection_batch_size,
            self.pb_channel_txn_chunk_size,
            self.per_table_chunk_sizes.clone(),
            self.enable_verbose_logging,
            self.transaction_filter.clone(),
        )
        .await
        .context("Failed to build worker")?;
        worker.run().await;
        Ok(())
    }

    fn get_server_name(&self) -> String {
        // Get the part before the first _ and trim to 12 characters.
        let before_underscore = self
            .processor_config
            .name()
            .split('_')
            .next()
            .unwrap_or("unknown");
        before_underscore[..before_underscore.len().min(12)].to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct IndexerGrpcHttp2Config {
    /// Indexer GRPC http2 ping interval in seconds. Defaults to 30.
    /// Tonic ref: https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html#method.http2_keep_alive_interval
    indexer_grpc_http2_ping_interval_in_secs: u64,

    /// Indexer GRPC http2 ping timeout in seconds. Defaults to 10.
    indexer_grpc_http2_ping_timeout_in_secs: u64,

    /// Seconds before timeout for grpc connection.
    indexer_grpc_connection_timeout_secs: u64,
}

impl IndexerGrpcHttp2Config {
    pub fn grpc_http2_ping_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_interval_in_secs)
    }

    pub fn grpc_http2_ping_timeout_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_timeout_in_secs)
    }

    pub fn grpc_connection_timeout_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_connection_timeout_secs)
    }
}

impl Default for IndexerGrpcHttp2Config {
    fn default() -> Self {
        Self {
            indexer_grpc_http2_ping_interval_in_secs: 30,
            indexer_grpc_http2_ping_timeout_in_secs: 10,
            indexer_grpc_connection_timeout_secs: 5,
        }
    }
}
