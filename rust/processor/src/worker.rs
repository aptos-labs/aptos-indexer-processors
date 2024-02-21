// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::IndexerGrpcHttp2Config,
    models::{
        events_models::events::EventStreamMessage, ledger_info::LedgerInfo,
        processor_status::ProcessorStatusQuery,
    },
    processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        coin_processor::CoinProcessor, default_processor::DefaultProcessor,
        event_stream_processor::EventStreamProcessor, events_processor::EventsProcessor,
        fungible_asset_processor::FungibleAssetProcessor,
        monitoring_processor::MonitoringProcessor, nft_metadata_processor::NftMetadataProcessor,
        objects_processor::ObjectsProcessor, stake_processor::StakeProcessor,
        token_processor::TokenProcessor, token_v2_processor::TokenV2Processor,
        user_transaction_processor::UserTransactionProcessor, ProcessingResult, Processor,
        ProcessorConfig, ProcessorTrait,
    },
    schema::ledger_infos,
    utils::{
        counters::{
            ProcessorStep, GRPC_LATENCY_BY_PROCESSOR_IN_SECS, LATEST_PROCESSED_VERSION,
            MULTI_BATCH_PROCESSING_TIME_IN_SECS, NUM_TRANSACTIONS_PROCESSED_COUNT,
            PROCESSED_BYTES_COUNT, PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS, PROCESSOR_ERRORS_COUNT,
            PROCESSOR_INVOCATIONS_COUNT, PROCESSOR_SUCCESSES_COUNT,
            SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS, SINGLE_BATCH_PARSING_TIME_IN_SECS,
            SINGLE_BATCH_PROCESSING_TIME_IN_SECS, TRANSACTION_UNIX_TIMESTAMP,
        },
        database::{execute_with_better_error, new_db_pool, run_pending_migrations, PgDbPool},
        event_ordering::{EventOrdering, TransactionEvents},
        stream::spawn_stream,
        util::{time_diff_since_pb_timestamp_in_secs, timestamp_to_iso, timestamp_to_unixtime},
    },
};
use anyhow::{Context, Result};
use aptos_moving_average::MovingAverage;
use aptos_protos::transaction::v1::Transaction;
use kanal::AsyncReceiver;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast, time::timeout};
use tracing::{error, info};
use url::Url;
use warp::Filter;

// this is how large the fetch queue should be. Each bucket should have a max of 80MB or so, so a batch
// of 50 means that we could potentially have at least 4.8GB of data in memory at any given time and that we should provision
// machines accordingly.
const BUFFER_SIZE: usize = 50;
// Consumer thread will wait X seconds before panicking if it doesn't receive any data
const CONSUMER_THREAD_TIMEOUT_IN_SECS: u64 = 60 * 5;
pub(crate) const PROCESSOR_SERVICE_TYPE: &str = "processor";

#[derive(Clone)]
pub struct StreamContext {
    pub channel: broadcast::Sender<EventStreamMessage>,
    pub websocket_alive_duration: u64,
}

/// Handles WebSocket connection from /stream endpoint
async fn handle_websocket(
    websocket: warp::ws::Ws,
    context: Arc<StreamContext>,
) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(websocket.on_upgrade(move |ws| {
        spawn_stream(
            ws,
            context.channel.subscribe(),
            context.websocket_alive_duration,
        )
    }))
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
        let enable_verbose_logging = self.enable_verbose_logging.unwrap_or(false);
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

        let concurrent_tasks = self.number_concurrent_processing_tasks;

        // Build the processor based on the config.
        let (processor, transaction_events_rx) =
            build_processor(&self.processor_config, self.db_pool.clone()).await;
        let processor = Arc::new(processor);

        // This is the moving average that we use to calculate TPS
        let mut ma = MovingAverage::new(10);
        let mut batch_start_version = starting_version;

        let ending_version = self.ending_version;
        let indexer_grpc_data_service_address = self.indexer_grpc_data_service_address.clone();
        let indexer_grpc_http2_ping_interval =
            self.grpc_http2_config.grpc_http2_ping_interval_in_secs();
        let indexer_grpc_http2_ping_timeout =
            self.grpc_http2_config.grpc_http2_ping_timeout_in_secs();
        // Create a transaction fetcher thread that will continuously fetch transactions from the GRPC stream
        // and write into a channel
        // The each item will be (chain_id, batch of transactions)
        let (tx, receiver) = kanal::bounded_async::<TransactionsPBResponse>(BUFFER_SIZE);
        let request_ending_version = self.ending_version;
        let auth_token = self.auth_token.clone();
        tokio::spawn(async move {
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                end_version = ending_version,
                start_version = batch_start_version,
                "[Parser] Starting fetcher thread"
            );
            crate::grpc_stream::create_fetcher_loop(
                tx,
                indexer_grpc_data_service_address,
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                starting_version,
                request_ending_version,
                auth_token,
                processor_name.to_string(),
                batch_start_version,
                BUFFER_SIZE,
            )
            .await
        });

        // Create a gap detector task that will panic if there is a gap in the processing
        let (gap_detector_sender, gap_detector_receiver) =
            kanal::bounded_async::<ProcessingResult>(BUFFER_SIZE);
        let processor_clone = processor.clone();
        let gap_detection_batch_size = self.gap_detection_batch_size;
        tokio::spawn(async move {
            crate::gap_detector::create_gap_detector_status_tracker_loop(
                gap_detector_receiver,
                processor_clone,
                batch_start_version,
                gap_detection_batch_size,
            )
            .await;
        });

        if processor.name() == "event_stream_processor" {
            let (broadcast_tx, mut broadcast_rx) = broadcast::channel(10000);
            let ordering_broadcast_tx = broadcast_tx.clone();
            tokio::spawn(async move {
                let mut event_ordering =
                    EventOrdering::new(transaction_events_rx, ordering_broadcast_tx.clone());
                event_ordering.run(starting_version as i64).await;
            });

            // Receive all messages with initial Receiver to keep channel open
            tokio::spawn(async move {
                loop {
                    broadcast_rx.recv().await.unwrap_or_else(|e| {
                        error!(
                            error = ?e,
                            "[Event Stream] Failed to receive message from channel"
                        );
                        panic!();
                    });
                }
            });

            tokio::spawn(async move {
                // Create web server
                let stream_context = Arc::new(StreamContext {
                    channel: broadcast_tx.clone(),
                    websocket_alive_duration: 3000,
                });

                let ws_route = warp::path("stream")
                    .and(warp::ws())
                    .and(warp::any().map(move || stream_context.clone()))
                    .and_then(handle_websocket);

                warp::serve(ws_route).run(([0, 0, 0, 0], 8081)).await;
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
pub async fn build_processor(
    config: &ProcessorConfig,
    db_pool: PgDbPool,
) -> (Processor, AsyncReceiver<TransactionEvents>) {
    let (tx, rx) = kanal::bounded_async::<TransactionEvents>(10000);
    let processor = match config {
        ProcessorConfig::AccountTransactionsProcessor => {
            Processor::from(AccountTransactionsProcessor::new(db_pool))
        },
        ProcessorConfig::AnsProcessor(config) => {
            Processor::from(AnsProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::CoinProcessor => Processor::from(CoinProcessor::new(db_pool)),
        ProcessorConfig::DefaultProcessor => Processor::from(DefaultProcessor::new(db_pool)),
        ProcessorConfig::EventStreamProcessor => {
            Processor::from(EventStreamProcessor::new(db_pool, tx.clone()))
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
    (processor, rx)
}
