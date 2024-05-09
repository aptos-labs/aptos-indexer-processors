// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    gap_detector::DEFAULT_GAP_DETECTION_BATCH_SIZE, processors::ProcessorConfig,
    transaction_filter::TransactionFilter, worker::Worker,
};
use ahash::AHashMap;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use server_framework::{GenericConfig, RunnableConfig};
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
    #[serde(default = "IndexerGrpcProcessorConfigV2::default_gap_detection_batch_size")]
    pub gap_detection_batch_size: u64,
    // Number of protobuff transactions to send per chunk to the processor tasks
    #[serde(default = "IndexerGrpcProcessorConfigV2::default_pb_channel_txn_chunk_size")]
    pub pb_channel_txn_chunk_size: usize,
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    pub enable_verbose_logging: Option<bool>,
    #[serde(default)]
    pub transaction_filter: TransactionFilter,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcProcessorConfigV2 {
    pub common: CommonConfig,
    pub db_connection: DbConnectionConfig,
    pub indexer_grpc: IndexerGrpcConnectionConfig,
    pub processors: Vec<ProcessorConfigV2>,
}

impl IndexerGrpcProcessorConfigV2 {
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
impl RunnableConfig for IndexerGrpcProcessorConfigV2 {
    async fn run(&self) -> Result<()> {
        // TODO(grao): Implement the following things.
        if self.processors.len() != 1 {
            unimplemented!("Only support 1 processor now.");
        }
        if self.db_connection.db_connection_urls.len() != 1 {
            unimplemented!("Only support 1 db connection URL now.");
        }
        let mut worker = Worker::new(
            self.processors[0].processor.clone(),
            self.db_connection.db_connection_urls[0].clone(),
            self.indexer_grpc.data_service_address.clone(),
            self.indexer_grpc.http2_config.clone(),
            self.indexer_grpc.auth_token.clone(),
            self.common.version_config.starting_version,
            self.common.version_config.ending_version,
            self.common.number_concurrent_processing_tasks,
            self.db_connection.db_pool_size_per_url,
            self.common.gap_detection_batch_size,
            self.common.pb_channel_txn_chunk_size,
            self.common.per_table_chunk_sizes.clone(),
            self.common.enable_verbose_logging,
            self.common.transaction_filter.clone(),
        )
        .await
        .context("Failed to build worker")?;
        worker.run().await;
        Ok(())
    }

    fn get_server_name(&self) -> String {
        // Get the part before the first _ and trim to 12 characters.
        let before_underscore = self.processors[0]
            .processor
            .name()
            .split('_')
            .next()
            .unwrap_or("unknown");
        before_underscore[..before_underscore.len().min(12)].to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommonConfig {
    #[serde(flatten)]
    pub version_config: VersionConfig,
    // Maximum number of batches "missing" before we assume we have an issue with gaps and abort
    #[serde(default = "IndexerGrpcProcessorConfigV2::default_gap_detection_batch_size")]
    pub gap_detection_batch_size: u64,
    // Number of protobuf transactions to send per chunk to the processor tasks
    #[serde(default = "IndexerGrpcProcessorConfigV2::default_pb_channel_txn_chunk_size")]
    pub pb_channel_txn_chunk_size: usize,
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    // TODO(grao): Revisit this config, it's probably better to have it on each processor config.
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    // Number of tasks waiting to pull transaction batches from the channel and process them
    pub number_concurrent_processing_tasks: Option<usize>,
    #[serde(default)]
    pub transaction_filter: TransactionFilter,
    pub enable_verbose_logging: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorConfigV2 {
    pub processor: ProcessorConfig,
    pub overrides: ConfigOverrides,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DbConnectionConfig {
    pub db_connection_urls: Vec<String>,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    pub db_pool_size_per_url: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcConnectionConfig {
    // TODO: Add TLS support.
    pub data_service_address: Url,
    #[serde(flatten)]
    pub http2_config: IndexerGrpcHttp2Config,
    pub auth_token: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverrides {
    #[serde(flatten)]
    pub version_overrides: VersionConfig,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionConfig {
    pub starting_version: Option<u64>,
    pub ending_version: Option<u64>,
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

pub fn from_v1_config(
    v1_config: GenericConfig<IndexerGrpcProcessorConfig>,
) -> GenericConfig<IndexerGrpcProcessorConfigV2> {
    GenericConfig::<IndexerGrpcProcessorConfigV2> {
        health_check_port: v1_config.health_check_port,
        server_config: v1_config.server_config.into(),
    }
}

impl From<IndexerGrpcProcessorConfig> for IndexerGrpcProcessorConfigV2 {
    fn from(v1_config: IndexerGrpcProcessorConfig) -> Self {
        IndexerGrpcProcessorConfigV2 {
            common: CommonConfig {
                version_config: VersionConfig {
                    starting_version: v1_config.starting_version,
                    ending_version: v1_config.ending_version,
                },
                gap_detection_batch_size: v1_config.gap_detection_batch_size,
                pb_channel_txn_chunk_size: v1_config.pb_channel_txn_chunk_size,
                per_table_chunk_sizes: v1_config.per_table_chunk_sizes,
                number_concurrent_processing_tasks: v1_config.number_concurrent_processing_tasks,
                enable_verbose_logging: v1_config.enable_verbose_logging,
                transaction_filter: v1_config.transaction_filter,
            },
            db_connection: DbConnectionConfig {
                db_connection_urls: vec![v1_config.postgres_connection_string],
                db_pool_size_per_url: v1_config.db_pool_size,
            },
            indexer_grpc: IndexerGrpcConnectionConfig {
                data_service_address: v1_config.indexer_grpc_data_service_address,
                http2_config: v1_config.grpc_http2_config,
                auth_token: v1_config.auth_token,
            },
            processors: vec![ProcessorConfigV2 {
                processor: v1_config.processor_config,
                overrides: Default::default(),
            }],
        }
    }
}
