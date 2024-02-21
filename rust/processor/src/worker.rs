// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::IndexerGrpcHttp2Config,
    models::{
        events_models::events::EventModel, ledger_info::LedgerInfo,
        processor_status::ProcessorStatusQuery,
    },
    processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        coin_processor::CoinProcessor, default_processor::DefaultProcessor,
        event_filter_processor::EventFilterProcessor, events_processor::EventsProcessor,
        fungible_asset_processor::FungibleAssetProcessor,
        monitoring_processor::MonitoringProcessor, nft_metadata_processor::NftMetadataProcessor,
        objects_processor::ObjectsProcessor, stake_processor::StakeProcessor,
        token_processor::TokenProcessor, token_v2_processor::TokenV2Processor,
        user_transaction_processor::UserTransactionProcessor, Processor, ProcessorConfig,
        ProcessorTrait,
    },
    schema::ledger_infos,
    utils::{
        counters::GRPC_TO_PROCESSOR_2_EXTRACT_LATENCY_IN_SECS,
        database::{execute_with_better_error, new_db_pool, run_pending_migrations, PgDbPool},
        EventStreamMessage,
    },
};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::Transaction;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use tracing::{error, info};
use url::Url;

// Consumer thread will wait X seconds before panicking if it doesn't receive any data
pub(crate) const PROCESSOR_SERVICE_TYPE: &str = "processor";

/// Config from Event Stream YAML
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventFilterConfig {
    pub server_port: u16,
    pub num_sec_valid: Option<i64>,
    pub websocket_alive_duration: Option<u64>,
}

#[derive(Clone)]
pub struct FilterContext {
    pub channel: broadcast::Sender<EventModel>,
    pub websocket_alive_duration: u64,
}

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
    pub gap_detection_batch_size: u64,
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
        db_pool_size: Option<u32>,
        gap_detection_batch_size: u64,
        enable_verbose_logging: Option<bool>,
    ) -> Result<Self> {
        let processor_name = processor_config.name();
        info!(processor_name = processor_name, "[Parser] Kicking off");

        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            "[Parser] Creating connection pool"
        );
        let conn_pool = new_db_pool(&postgres_connection_string, db_pool_size)
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
            gap_detection_batch_size,
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

        let starting_version_from_db = self
            .get_start_version()
            .await
            .expect("[Parser] Database error when getting starting version")
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
        let (processor, ip) = build_processor(&self.processor_config, self.db_pool.clone()).await;
        let processor = Arc::new(processor);

        if processor.name() == "event_filter_processor" {
            // Create Event broadcast channel
            // Can use channel size to help with pubsub lagging
            let (broadcast_tx, mut broadcast_rx) = broadcast::channel(10000);

            // Receive all messages with initial Receiver to keep channel open
            tokio::spawn(async move {
                loop {
                    broadcast_rx.recv().await.unwrap_or_else(|e| {
                        error!(
                            error = ?e,
                            "[Event Filter] Failed to receive message from channel"
                        );
                        panic!();
                    });
                }
            });

            // Create and start ingestor
            let broadcast_tx_write = broadcast_tx.clone();
            tokio::spawn(async move {
                let url = Url::parse(&format!("ws://{}:12345/stream", ip)).unwrap_or_else(|e| {
                    error!(error = ?e, "Failed to parse URL");
                    panic!();
                });

                let (ws_stream, _) = connect_async(url).await.unwrap_or_else(|e| {
                    error!(error = ?e, "Failed to connect to WebSocket");
                    panic!();
                });

                let (_, mut read) = ws_stream.split();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            GRPC_TO_PROCESSOR_2_EXTRACT_LATENCY_IN_SECS
                                .with_label_values(&["event_stream"])
                                .set({
                                    let transaction_timestamp = chrono::Utc::now();
                                    let transaction_timestamp =
                                        std::time::SystemTime::from(transaction_timestamp);
                                    std::time::SystemTime::now()
                                        .duration_since(transaction_timestamp)
                                        .unwrap_or_default()
                                        .as_secs_f64()
                                });
                            broadcast_tx_write
                                .send(
                                    serde_json::from_str::<EventStreamMessage>(&msg.to_string())
                                        .unwrap(),
                                )
                                .unwrap();
                        },
                        Err(e) => {
                            eprintln!("Error receiving message: {}", e);
                            break;
                        },
                    }
                }
            });
        }
    }

    async fn run_migrations(&self) {
        let mut conn = self
            .db_pool
            .get()
            .await
            .expect("[Parser] Failed to get connection");
        run_pending_migrations(&mut conn).await;
    }

    /// Gets the start version for the processor. If not found, start from 0.
    pub async fn get_start_version(&self) -> Result<Option<u64>> {
        let mut conn = self.db_pool.get().await?;

        match ProcessorStatusQuery::get_by_processor(self.processor_config.name(), &mut conn)
            .await?
        {
            Some(status) => Ok(Some(status.last_success_version as u64 + 1)),
            None => Ok(None),
        }
    }

    /// Verify the chain id from GRPC against the database.
    pub async fn check_or_update_chain_id(&self, grpc_chain_id: i64) -> Result<u64> {
        let processor_name = self.processor_config.name();
        info!(
            processor_name = processor_name,
            "[Parser] Checking if chain id is correct"
        );
        let mut conn = self.db_pool.get().await?;

        let maybe_existing_chain_id = LedgerInfo::get(&mut conn).await?.map(|li| li.chain_id);

        match maybe_existing_chain_id {
            Some(chain_id) => {
                anyhow::ensure!(chain_id == grpc_chain_id, "[Parser] Wrong chain detected! Trying to index chain {} now but existing data is for chain {}", grpc_chain_id, chain_id);
                info!(
                    processor_name = processor_name,
                    chain_id = chain_id,
                    "[Parser] Chain id matches! Continue to index...",
                );
                Ok(chain_id as u64)
            },
            None => {
                info!(
                    processor_name = processor_name,
                    chain_id = grpc_chain_id,
                    "[Parser] Adding chain id to db, continue to index..."
                );
                execute_with_better_error(
                    self.db_pool.clone(),
                    diesel::insert_into(ledger_infos::table)
                        .values(LedgerInfo {
                            chain_id: grpc_chain_id,
                        })
                        .on_conflict_do_nothing(),
                    None,
                )
                .await
                .context("[Parser] Error updating chain_id!")
                .map(|_| grpc_chain_id as u64)
            },
        }
    }
}

/// Given a config and a db pool, build a concrete instance of a processor.
// As time goes on there might be other things that we need to provide to certain
// processors. As that happens we can revist whether this function (which tends to
// couple processors together based on their args) makes sense.
pub async fn build_processor(config: &ProcessorConfig, db_pool: PgDbPool) -> (Processor, String) {
    let mut out = String::new();
    let processor = match config {
        ProcessorConfig::AccountTransactionsProcessor => {
            Processor::from(AccountTransactionsProcessor::new(db_pool))
        },
        ProcessorConfig::AnsProcessor(config) => {
            Processor::from(AnsProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::CoinProcessor => Processor::from(CoinProcessor::new(db_pool)),
        ProcessorConfig::DefaultProcessor => Processor::from(DefaultProcessor::new(db_pool)),
        ProcessorConfig::EventFilterProcessor(config) => {
            out = config.clone().stream_ip.unwrap_or("localhost".to_string());
            Processor::from(EventFilterProcessor::new(db_pool))
        },
        ProcessorConfig::EventsProcessor => Processor::from(EventsProcessor::new(db_pool)),
        ProcessorConfig::FungibleAssetProcessor => {
            Processor::from(FungibleAssetProcessor::new(db_pool))
        },
        ProcessorConfig::MonitoringProcessor => Processor::from(MonitoringProcessor::new(db_pool)),
        ProcessorConfig::NftMetadataProcessor(config) => {
            Processor::from(NftMetadataProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::ObjectsProcessor => Processor::from(ObjectsProcessor::new(db_pool)),
        ProcessorConfig::StakeProcessor => Processor::from(StakeProcessor::new(db_pool)),
        ProcessorConfig::TokenProcessor(config) => {
            Processor::from(TokenProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::TokenV2Processor => Processor::from(TokenV2Processor::new(db_pool)),
        ProcessorConfig::UserTransactionProcessor => {
            Processor::from(UserTransactionProcessor::new(db_pool))
        },
    };
    (processor, out.to_string())
}
