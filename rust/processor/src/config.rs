// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{processors::ProcessorConfig, worker::Worker};
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
    pub starting_version: Option<u64>,
    pub ending_version: Option<u64>,
    pub number_concurrent_processing_tasks: Option<usize>,
    pub enable_verbose_logging: Option<bool>,
    pub cockroach_db: Option<String>,
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
            self.enable_verbose_logging,
            self.cockroach_db.clone(),
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
