// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::{build_processor, ProcessorConfig},
    utils::{
        database::{new_db_pool, run_pending_migrations, PgDbPool},
        progress_storage::DieselProgressStorage,
    },
};
use anyhow::{Context, Result};
use aptos_processor_sdk::{
    dispatcher::{Dispatcher, DispatcherConfig, PROCESSOR_SERVICE_TYPE},
    progress_storage::ProgressStorageTrait,
    stream_subscriber::{
        ChannelHandle, GrpcStreamSubscriber, GrpcStreamSubscriberConfig, IndexerGrpcHttp2Config,
        StreamSubscriberTrait,
    },
};
use aptos_protos::transaction::v1::Transaction;
use tracing::info;
use url::Url;

#[derive(Clone)]
pub struct TransactionsPBResponse {
    pub transactions: Vec<Transaction>,
    pub chain_id: u64,
    pub size_in_bytes: u64,
}

pub struct Worker {
    pub db_pool: PgDbPool,
    pub processor_config: ProcessorConfig,
    pub postgres_connection_string: String,
    pub indexer_grpc_data_service_address: Url,
    pub grpc_http2_config: IndexerGrpcHttp2Config,
    pub auth_token: String,
    pub starting_version: Option<u64>,
    pub ending_version: Option<u64>,
    pub number_concurrent_processing_tasks: usize,
    pub enable_verbose_logging: Option<bool>,
}

impl Worker {
    pub async fn new(
        processor_config: ProcessorConfig,
        postgres_connection_string: String,
        indexer_grpc_data_service_address: Url,
        grpc_http2_config: IndexerGrpcHttp2Config,
        auth_token: String,
        starting_version: Option<u64>,
        ending_version: Option<u64>,
        number_concurrent_processing_tasks: Option<usize>,
        enable_verbose_logging: Option<bool>,
    ) -> Result<Self> {
        let processor_name = processor_config.name();
        info!(processor_name = processor_name, "[Parser] Kicking off");

        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            "[Parser] Creating connection pool"
        );
        let conn_pool = new_db_pool(&postgres_connection_string)
            .await
            .context("Failed to create connection pool")?;
        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            "[Parser] Finish creating the connection pool"
        );
        let number_concurrent_processing_tasks = number_concurrent_processing_tasks.unwrap_or(10);
        Ok(Self {
            db_pool: conn_pool,
            processor_config,
            postgres_connection_string,
            indexer_grpc_data_service_address,
            grpc_http2_config,
            starting_version,
            ending_version,
            auth_token,
            number_concurrent_processing_tasks,
            enable_verbose_logging,
        })
    }

    /// This is the main logic of the processor. We will do a few large parts:
    /// 1. Connect to GRPC and handling all the stuff before starting the stream such as diesel migration
    /// 2. Start a thread specifically to fetch data from GRPC. We will keep a buffer of X batches of transactions
    /// 3. Start a loop to consume from the buffer. We will have Y threads to process the transactions in parallel. (Y should be less than X for obvious reasons)
    ///   * Note that the batches will be sequential so we won't have problems with gaps
    /// 4. We will keep track of the last processed version and monitoring things like TPS
    pub async fn run(&mut self) {
        let processor_name = self.processor_config.name();
        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            "[Parser] Running migrations"
        );
        let migration_time = std::time::Instant::now();
        self.run_migrations().await;
        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            duration_in_secs = migration_time.elapsed().as_secs_f64(),
            "[Parser] Finished migrations"
        );

        let progress_storage = DieselProgressStorage::new(self.db_pool.clone());

        let starting_version_from_db = progress_storage
            .read_last_processed_version(processor_name)
            .await
            .expect("[Parser] Database error when getting starting version")
            .map(|v| v + 1)
            .unwrap_or_else(|| {
                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    "[Parser] No starting version from db so starting from version 0"
                );
                0
            });

        let starting_version = match self.starting_version {
            None => starting_version_from_db,
            Some(version) => version,
        };

        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.to_string(),
            final_start_version = starting_version,
            start_version_from_config = self.starting_version,
            start_version_from_db = starting_version_from_db,
            "[Parser] Building processor",
        );

        // Build the processor based on the config.
        let processor = build_processor(&self.processor_config, self.db_pool.clone());

        let stream_subscriber = GrpcStreamSubscriber::new(
            GrpcStreamSubscriberConfig {
                grpc_data_service_address: self.indexer_grpc_data_service_address.clone(),
                grpc_http2_config: self.grpc_http2_config.clone(),
                auth_token: self.auth_token.clone(),
                ending_version: self.ending_version,
            },
            processor_name.to_string(),
            starting_version,
        );

        // Create a transaction fetcher thread that will continuously fetch
        // transactions from the GRPC stream and write into a channel
        // The each item will be (chain_id, batch of transactions)
        let ChannelHandle {
            join_handle: _,
            receiver,
        } = stream_subscriber
            .start()
            .expect("Failed to start stream subscriber");

        // Create a Dispatcher, which pulls transactions from that channel, dispatches
        // them to the processor, and keeps track of the progress.
        let mut dispatcher = Dispatcher {
            config: DispatcherConfig {
                number_concurrent_processing_tasks: self.number_concurrent_processing_tasks,
                enable_verbose_logging: self.enable_verbose_logging.unwrap_or(false),
            },
            progress_storage,
            processor,
            receiver,
            starting_version,
            indexer_grpc_data_service_address: self.indexer_grpc_data_service_address.clone(),
            auth_token: self.auth_token.clone(),
        };

        // The main dispatcher loop. This should never end.
        dispatcher.dispatch().await;
    }

    async fn run_migrations(&self) {
        let mut conn = self
            .db_pool
            .get()
            .await
            .expect("[Parser] Failed to get connection");
        run_pending_migrations(&mut conn).await;
    }
}
