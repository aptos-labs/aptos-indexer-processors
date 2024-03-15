// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    db_writer::QueryGenerator,
    processors::ProcessorConfig,
    transaction_filter::TransactionFilter,
    utils::database::{new_db_pool, DEFAULT_MAX_POOL_SIZE},
    worker::Worker,
};
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use server_framework::RunnableConfig;
use std::time::Duration;
use url::Url;

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
    // Number of tables this processor writes to. This is used in the version tracker.
    // Since DB table writes are parallel, it needs to know when all tables have been written to before it can move the latest version forward.
    pub number_db_tables: u64,
    // Number of tasks waiting to pull transaction batches from the channel and process them
    #[serde(default = "IndexerGrpcProcessorConfig::number_concurrent_processing_tasks")]
    pub number_concurrent_processing_tasks: usize,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "IndexerGrpcProcessorConfig::default_db_pool_size")]
    pub db_pool_size: u32,
    // Number of protobuff transactions to send per chunk to the processor tasks
    #[serde(default = "IndexerGrpcProcessorConfig::default_pb_channel_txn_chunk_size")]
    pub pb_channel_txn_chunk_size: usize,
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    // Any tables to skip indexing for
    #[serde(default = "AHashSet::new")]
    pub skip_tables: AHashSet<String>,
    // Number of parallel db writer tasks that pull from the channel and do inserts
    #[serde(default = "IndexerGrpcProcessorConfig::default_number_concurrent_db_writer_tasks")]
    pub number_concurrent_db_writer_tasks: usize,
    // Size of the channel between the processor tasks and the db writer tasks
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_executor_channel_size")]
    pub query_executor_channel_size: usize,
    #[serde(default)]
    pub transaction_filter: TransactionFilter,
    #[serde(default = "IndexerGrpcProcessorConfig::default_false")]
    pub enable_verbose_logging: bool,
}

impl IndexerGrpcProcessorConfig {
    /// Make the default very large on purpose so that by default it's not chunked
    /// This prevents any unexpected changes in behavior
    pub const fn default_pb_channel_txn_chunk_size() -> usize {
        100_000
    }

    pub const fn default_false() -> bool {
        false
    }

    pub const fn default_db_pool_size() -> u32 {
        DEFAULT_MAX_POOL_SIZE
    }

    // TODO: ideally the default is a multiple of pool_size and/or number_concurrent_processing_tasks
    pub const fn default_number_concurrent_db_writer_tasks() -> usize {
        50
    }

    // TODO: ideally the default is based on the number of cores
    pub const fn number_concurrent_processing_tasks() -> usize {
        10
    }

    /// Make the default very large on purpose so that by default it's not chunked
    /// This _minimizes_ some unexpected changes in behavior
    pub const fn default_query_executor_channel_size() -> usize {
        100_000
    }
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcProcessorConfig {
    async fn run(&self) -> Result<()> {
        let conn_pool = new_db_pool(&self.postgres_connection_string, self.db_pool_size)
            .await
            .context("Failed to create connection pool")?;

        let processor_name = self.processor_config.name();

        let (query_sender, query_receiver) =
            kanal::bounded_async::<QueryGenerator>(self.query_executor_channel_size);
        let db_writer = crate::db_writer::DbWriter::new(
            processor_name,
            conn_pool.clone(),
            query_sender,
            self.per_table_chunk_sizes.clone(),
            self.skip_tables.clone(),
        );

        let mut worker = Worker::new(
            self.processor_config.clone(),
            self.postgres_connection_string.clone(),
            self.indexer_grpc_data_service_address.clone(),
            self.grpc_http2_config.clone(),
            self.auth_token.clone(),
            self.starting_version,
            self.ending_version,
            self.number_concurrent_processing_tasks,
            self.number_db_tables,
            self.pb_channel_txn_chunk_size,
            self.number_concurrent_db_writer_tasks,
            self.enable_verbose_logging,
            self.transaction_filter.clone(),
            query_receiver,
            db_writer,
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
}

impl IndexerGrpcHttp2Config {
    pub fn grpc_http2_ping_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_interval_in_secs)
    }

    pub fn grpc_http2_ping_timeout_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_timeout_in_secs)
    }
}

impl Default for IndexerGrpcHttp2Config {
    fn default() -> Self {
        Self {
            indexer_grpc_http2_ping_interval_in_secs: 30,
            indexer_grpc_http2_ping_timeout_in_secs: 10,
        }
    }
}
