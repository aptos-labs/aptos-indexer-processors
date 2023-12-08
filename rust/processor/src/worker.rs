// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::IndexerGrpcHttp2Config,
    models::{ledger_info::LedgerInfo, processor_status::ProcessorStatusQuery},
    processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        coin_processor::CoinProcessor, default_processor::DefaultProcessor,
        events_processor::EventsProcessor, fungible_asset_processor::FungibleAssetProcessor,
        nft_metadata_processor::NftMetadataProcessor, objects_processor::ObjectsProcessor,
        stake_processor::StakeProcessor, token_processor::TokenProcessor,
        token_v2_processor::TokenV2Processor, user_transaction_processor::UserTransactionProcessor,
        ProcessingResult, Processor, ProcessorConfig, ProcessorTrait,
    },
    schema::ledger_infos,
    utils::{
        counters::{
            ProcessorStep, FETCHER_THREAD_CHANNEL_SIZE, LATEST_PROCESSED_VERSION,
            MULTI_BATCH_PROCESSING_TIME_IN_SECS, NUM_TRANSACTIONS_PROCESSED_COUNT,
            PROCESSED_BYTES_COUNT, PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS, PROCESSOR_ERRORS_COUNT,
            PROCESSOR_INVOCATIONS_COUNT, PROCESSOR_SUCCESSES_COUNT,
            SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS, SINGLE_BATCH_PARSING_TIME_IN_SECS,
            SINGLE_BATCH_PROCESSING_TIME_IN_SECS, TRANSACTION_UNIX_TIMESTAMP,
        },
        database::{execute_with_better_error, new_db_pool, run_pending_migrations, PgDbPool},
        util::{time_diff_since_pb_timestamp_in_secs, timestamp_to_iso, timestamp_to_unixtime},
    },
};
use anyhow::{Context, Result};
use aptos_moving_average::MovingAverage;
use aptos_protos::{
    indexer::v1::{raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse},
    transaction::v1::Transaction,
};
use futures::StreamExt;
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::error::TryRecvError, time::timeout};
use tonic::{Response, Streaming};
use tracing::{error, info};
use url::Url;

/// GRPC request metadata key for the token ID.
const GRPC_API_GATEWAY_API_KEY_HEADER: &str = "authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
/// GRPC connection id
const GRPC_CONNECTION_ID: &str = "x-aptos-connection-id";
// this is how large the fetch queue should be. Each bucket should have a max of 80MB or so, so a batch
// of 50 means that we could potentially have at least 4.8GB of data in memory at any given time and that we should provision
// machines accordingly.
const BUFFER_SIZE: usize = 50;
// 20MB
const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 20;
// We will try to reconnect to GRPC 5 times in case upstream connection is being updated
const RECONNECTION_MAX_RETRIES: u64 = 100;
// Consumer thread will wait X seconds before panicking if it doesn't receive any data
const CONSUMER_THREAD_TIMEOUT_IN_SECS: u64 = 60 * 5;
const PROCESSOR_SERVICE_TYPE: &str = "processor";

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
        let processor = build_processor(&self.processor_config, self.db_pool.clone());
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
        let (tx, mut receiver) = tokio::sync::mpsc::channel::<TransactionsPBResponse>(BUFFER_SIZE);
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
            create_fetcher_loop(
                tx,
                indexer_grpc_data_service_address,
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                starting_version,
                request_ending_version,
                auth_token,
                processor_name.to_string(),
                batch_start_version,
            )
            .await
        });

        // This is the consumer side of the channel. These are the major states:
        // 1. We're backfilling so we should expect many concurrent threads to process transactions
        // 2. We're caught up so we should expect a single thread to process transactions
        // 3. We have received either an empty batch or a batch with a gap. We should panic.
        // 4. We have not received anything in X seconds, we should panic.
        // 5. If it's the wrong chain, panic.
        let mut db_chain_id = None;
        loop {
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = self.indexer_grpc_data_service_address.as_str(),
                "[Parser] Fetching transaction batches from channel",
            );
            let txn_channel_fetch_latency = std::time::Instant::now();
            let mut transactions_batches = vec![];
            let mut last_fetched_version = batch_start_version as i64 - 1;
            for task_index in 0..concurrent_tasks {
                let receive_status = match task_index {
                    0 => {
                        // If we're the first task, we should wait until we get data. If `None`, it means the channel is closed.
                        match timeout(
                            Duration::from_secs(CONSUMER_THREAD_TIMEOUT_IN_SECS),
                            receiver.recv(),
                        )
                        .await
                        {
                            Ok(result) => result.ok_or(TryRecvError::Disconnected),
                            Err(_) => {
                                error!(
                                    processor_name = processor_name,
                                    service_type = PROCESSOR_SERVICE_TYPE,
                                    stream_address =
                                        self.indexer_grpc_data_service_address.as_str(),
                                    "[Parser] Consumer thread timed out waiting for transactions",
                                );
                                panic!(
                                    "[Parser] Consumer thread timed out waiting for transactions"
                                );
                            },
                        }
                        // If we're the first task, we should wait until we get data. If `None`, it means the channel is closed.
                        // receiver.recv().await.ok_or(TryRecvError::Disconnected)
                    },
                    _ => {
                        // If we're not the first task, we should poll to see if we get any data.
                        receiver.try_recv()
                    },
                };
                match receive_status {
                    Ok(txn_pb) => {
                        if let Some(existing_id) = db_chain_id {
                            if txn_pb.chain_id != existing_id {
                                error!(
                                    processor_name = processor_name,
                                    stream_address =
                                        self.indexer_grpc_data_service_address.as_str(),
                                    chain_id = txn_pb.chain_id,
                                    existing_id = existing_id,
                                    "[Parser] Stream somehow changed chain id!",
                                );
                                panic!("[Parser] Stream somehow changed chain id!");
                            }
                        } else {
                            db_chain_id = Some(
                                self.check_or_update_chain_id(txn_pb.chain_id as i64)
                                    .await
                                    .unwrap(),
                            );
                        }
                        let current_fetched_version =
                            txn_pb.transactions.as_slice().first().unwrap().version;
                        if last_fetched_version + 1 != current_fetched_version as i64 {
                            error!(
                                batch_start_version = batch_start_version,
                                last_fetched_version = last_fetched_version,
                                current_fetched_version = current_fetched_version,
                                "[Parser] Received batch with gap from GRPC stream"
                            );
                            panic!("[Parser] Received batch with gap from GRPC stream");
                        }
                        last_fetched_version =
                            txn_pb.transactions.as_slice().last().unwrap().version as i64;
                        transactions_batches.push(txn_pb);
                    },
                    // Channel is empty and send is not drpped which we definitely expect. Wait for a bit and continue polling.
                    Err(TryRecvError::Empty) => {
                        break;
                    },
                    // This happens when the channel is closed. We should panic.
                    Err(TryRecvError::Disconnected) => {
                        error!(
                            processor_name = processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self.indexer_grpc_data_service_address.as_str(),
                            "[Parser] Channel closed; stream ended."
                        );
                        panic!("[Parser] Channel closed");
                    },
                }
            }

            let size_in_bytes = transactions_batches
                .iter()
                .fold(0.0, |acc, txn_batch| acc + txn_batch.size_in_bytes as f64);
            let batch_start_txn_timestamp = transactions_batches
                .first()
                .unwrap()
                .transactions
                .as_slice()
                .first()
                .unwrap()
                .timestamp
                .clone();
            let batch_end_txn_timestamp = transactions_batches
                .last()
                .unwrap()
                .transactions
                .as_slice()
                .last()
                .unwrap()
                .timestamp
                .clone();
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                start_version = batch_start_version,
                end_version = last_fetched_version,
                num_of_transactions = last_fetched_version - batch_start_version as i64 + 1,
                size_in_bytes,
                duration_in_secs = txn_channel_fetch_latency.elapsed().as_secs_f64(),
                tps = (last_fetched_version as f64 - batch_start_version as f64)
                    / txn_channel_fetch_latency.elapsed().as_secs_f64(),
                bytes_per_sec = size_in_bytes / txn_channel_fetch_latency.elapsed().as_secs_f64(),
                "[Parser] Successfully fetched transaction batches from channel."
            );

            // Process the transactions in parallel
            let mut tasks = vec![];
            for transactions_pb in transactions_batches {
                let processor_clone = processor.clone();
                let auth_token = self.auth_token.clone();
                let task = tokio::spawn(async move {
                    let start_version = transactions_pb
                        .transactions
                        .as_slice()
                        .first()
                        .unwrap()
                        .version;
                    let end_version = transactions_pb
                        .transactions
                        .as_slice()
                        .last()
                        .unwrap()
                        .version;
                    let start_txn_timestamp = transactions_pb
                        .transactions
                        .as_slice()
                        .first()
                        .unwrap()
                        .timestamp
                        .clone();
                    let end_txn_timestamp = transactions_pb
                        .transactions
                        .as_slice()
                        .last()
                        .unwrap()
                        .timestamp
                        .clone();
                    let txn_time = transactions_pb
                        .transactions
                        .as_slice()
                        .first()
                        .unwrap()
                        .timestamp
                        .clone();
                    if let Some(ref t) = txn_time {
                        PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS
                            .with_label_values(&[auth_token.as_str(), processor_name])
                            .set(time_diff_since_pb_timestamp_in_secs(t));
                    }
                    PROCESSOR_INVOCATIONS_COUNT
                        .with_label_values(&[processor_name])
                        .inc();

                    if enable_verbose_logging {
                        info!(
                            processor_name = processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            start_version,
                            end_version,
                            size_in_bytes = transactions_pb.size_in_bytes,
                            "[Parser] Started processing one batch of transactions"
                        );
                    }

                    let processing_duration = std::time::Instant::now();

                    let processed_result = processor_clone
                        .process_transactions(
                            transactions_pb.transactions,
                            start_version,
                            end_version,
                            db_chain_id,
                        ) // TODO: Change how we fetch chain_id, ideally can be accessed by processors when they are initiallized (e.g. so they can have a chain_id field set on new() funciton)
                        .await;
                    if let Some(ref t) = txn_time {
                        PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS
                            .with_label_values(&[auth_token.as_str(), processor_name])
                            .set(time_diff_since_pb_timestamp_in_secs(t));
                    }

                    let start_txn_timestamp_unix = start_txn_timestamp
                        .clone()
                        .map(|t| timestamp_to_unixtime(&t))
                        .unwrap_or_default();
                    let start_txn_timestamp_iso = start_txn_timestamp
                        .map(|t| timestamp_to_iso(&t))
                        .unwrap_or_default();
                    let end_txn_timestamp_iso = end_txn_timestamp
                        .map(|t| timestamp_to_iso(&t))
                        .unwrap_or_default();

                    LATEST_PROCESSED_VERSION
                        .with_label_values(&[
                            &processor_name,
                            ProcessorStep::ProcessedBatch.get_step(),
                            ProcessorStep::ProcessedBatch.get_label(),
                        ])
                        .set(end_version as i64);
                    TRANSACTION_UNIX_TIMESTAMP
                        .with_label_values(&[
                            &processor_name,
                            ProcessorStep::ProcessedBatch.get_step(),
                            ProcessorStep::ProcessedBatch.get_label(),
                        ])
                        .set(start_txn_timestamp_unix);
                    PROCESSED_BYTES_COUNT
                        .with_label_values(&[
                            &processor_name,
                            ProcessorStep::ProcessedBatch.get_step(),
                            ProcessorStep::ProcessedBatch.get_label(),
                        ])
                        .inc_by(transactions_pb.size_in_bytes);
                    NUM_TRANSACTIONS_PROCESSED_COUNT
                        .with_label_values(&[
                            &processor_name,
                            ProcessorStep::ProcessedBatch.get_step(),
                            ProcessorStep::ProcessedBatch.get_label(),
                        ])
                        .inc_by(end_version - start_version + 1);

                    if let Ok(res) = processed_result {
                        SINGLE_BATCH_PROCESSING_TIME_IN_SECS
                            .with_label_values(&[&processor_name])
                            .set(processing_duration.elapsed().as_secs_f64());
                        SINGLE_BATCH_PARSING_TIME_IN_SECS
                            .with_label_values(&[&processor_name])
                            .set(res.processing_duration_in_secs);
                        SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS
                            .with_label_values(&[&processor_name])
                            .set(res.db_insertion_duration_in_secs);

                        if enable_verbose_logging {
                            info!(
                                processor_name = processor_name,
                                service_type = PROCESSOR_SERVICE_TYPE,
                                start_version,
                                end_version,
                                start_txn_timestamp_iso,
                                end_txn_timestamp_iso,
                                size_in_bytes = transactions_pb.size_in_bytes,
                                duration_in_secs = res.db_insertion_duration_in_secs,
                                tps = (end_version - start_version) as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                bytes_per_sec = transactions_pb.size_in_bytes as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                "[Parser] DB insertion time of one batch of transactions"
                            );
                            info!(
                                processor_name = processor_name,
                                service_type = PROCESSOR_SERVICE_TYPE,
                                start_version,
                                end_version,
                                start_txn_timestamp_iso,
                                end_txn_timestamp_iso,
                                size_in_bytes = transactions_pb.size_in_bytes,
                                duration_in_secs = res.processing_duration_in_secs,
                                tps = (end_version - start_version) as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                bytes_per_sec = transactions_pb.size_in_bytes as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                "[Parser] Parsing time of one batch of transactions"
                            );
                            info!(
                                processor_name = processor_name,
                                service_type = PROCESSOR_SERVICE_TYPE,
                                start_version,
                                end_version,
                                start_txn_timestamp_iso,
                                end_txn_timestamp_iso,
                                num_of_transactions = end_version - start_version + 1,
                                size_in_bytes = transactions_pb.size_in_bytes,
                                processing_duration_in_secs = res.processing_duration_in_secs,
                                db_insertion_duration_in_secs = res.db_insertion_duration_in_secs,
                                duration_in_secs = processing_duration.elapsed().as_secs_f64(),
                                tps = (end_version - start_version) as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                bytes_per_sec = transactions_pb.size_in_bytes as f64
                                    / processing_duration.elapsed().as_secs_f64(),
                                step = ProcessorStep::ProcessedBatch.get_step(),
                                "{}",
                                ProcessorStep::ProcessedBatch.get_label(),
                            );
                        }
                    }

                    processed_result
                });
                tasks.push(task);
            }
            let processing_time = std::time::Instant::now();
            let task_count = tasks.len();
            let batches = match futures::future::try_join_all(tasks).await {
                Ok(res) => res,
                Err(err) => panic!("[Parser] Error processing transaction batches: {:?}", err),
            };

            // Update states depending on results of the batch processing
            let mut processed_versions = vec![];
            for res in batches {
                let processed: ProcessingResult = match res {
                    Ok(versions) => {
                        PROCESSOR_SUCCESSES_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        versions
                    },
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = self.indexer_grpc_data_service_address.to_string(),
                            error = ?e,
                            "[Parser] Error processing transactions"
                        );
                        PROCESSOR_ERRORS_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        panic!();
                    },
                };
                processed_versions.push(processed);
            }

            // Make sure there are no gaps and advance states
            processed_versions.sort_by(|a, b| a.start_version.cmp(&b.start_version));
            let mut prev_start = None;
            let mut prev_end = None;
            let mut max_processing_duration_in_secs: f64 = 0.0;
            let mut max_db_insertion_duration_in_secs: f64 = 0.0;
            let processed_versions_sorted = processed_versions.clone();
            for processing_result in processed_versions {
                let start = processing_result.start_version;
                let end = processing_result.end_version;
                max_processing_duration_in_secs = max_processing_duration_in_secs
                    .max(processing_result.processing_duration_in_secs);
                max_db_insertion_duration_in_secs = max_db_insertion_duration_in_secs
                    .max(processing_result.db_insertion_duration_in_secs);
                if prev_start.is_none() {
                    prev_start = Some(start);
                    prev_end = Some(end);
                } else {
                    if prev_end.unwrap() + 1 != start {
                        error!(
                            processor_name = processor_name,
                            stream_address = self.indexer_grpc_data_service_address.to_string(),
                            processed_versions = processed_versions_sorted
                                .iter()
                                .map(|result| format!(
                                    "{}-{}",
                                    result.start_version, result.end_version
                                ))
                                .collect::<Vec<_>>()
                                .join(", "),
                            "[Parser] Gaps in processing stream"
                        );
                        panic!();
                    }
                    prev_start = Some(start);
                    prev_end = Some(end);
                }
            }
            let batch_start = processed_versions_sorted.first().unwrap().start_version;
            let batch_end = processed_versions_sorted.last().unwrap().end_version;
            batch_start_version = batch_end + 1;

            processor
                .update_last_processed_version(batch_end)
                .await
                .unwrap();

            ma.tick_now(batch_end - batch_start + 1);
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                start_version = batch_start,
                end_version = batch_end,
                start_txn_timestamp_iso = batch_start_txn_timestamp
                    .clone()
                    .map(|t| timestamp_to_iso(&t))
                    .unwrap_or_default(),
                end_txn_timestamp_iso = batch_end_txn_timestamp
                    .map(|t| timestamp_to_iso(&t))
                    .unwrap_or_default(),
                num_of_transactions = batch_end - batch_start + 1,
                task_count,
                size_in_bytes,
                duration_in_secs = processing_time.elapsed().as_secs_f64(),
                tps = (ma.avg() * 1000.0) as u64,
                bytes_per_sec = size_in_bytes / processing_time.elapsed().as_secs_f64(),
                step = ProcessorStep::ProcessedMultipleBatches.get_step(),
                "{}",
                ProcessorStep::ProcessedMultipleBatches.get_label(),
            );
            LATEST_PROCESSED_VERSION
                .with_label_values(&[
                    &processor_name,
                    ProcessorStep::ProcessedMultipleBatches.get_step(),
                    ProcessorStep::ProcessedMultipleBatches.get_label(),
                ])
                .set(batch_end as i64);
            TRANSACTION_UNIX_TIMESTAMP
                .with_label_values(&[
                    &processor_name,
                    ProcessorStep::ProcessedMultipleBatches.get_step(),
                    ProcessorStep::ProcessedMultipleBatches.get_label(),
                ])
                .set(
                    batch_start_txn_timestamp
                        .map(|t| timestamp_to_unixtime(&t))
                        .unwrap_or_default(),
                );
            PROCESSED_BYTES_COUNT
                .with_label_values(&[
                    &processor_name,
                    ProcessorStep::ProcessedMultipleBatches.get_step(),
                    ProcessorStep::ProcessedMultipleBatches.get_label(),
                ])
                .inc_by(size_in_bytes as u64);
            NUM_TRANSACTIONS_PROCESSED_COUNT
                .with_label_values(&[
                    &processor_name,
                    ProcessorStep::ProcessedMultipleBatches.get_step(),
                    ProcessorStep::ProcessedMultipleBatches.get_label(),
                ])
                .inc_by(batch_end - batch_start + 1);
            MULTI_BATCH_PROCESSING_TIME_IN_SECS
                .with_label_values(&[&processor_name])
                .set(processing_time.elapsed().as_secs_f64());
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
                    &mut conn,
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
pub fn build_processor(config: &ProcessorConfig, db_pool: PgDbPool) -> Processor {
    match config {
        ProcessorConfig::AccountTransactionsProcessor => {
            Processor::from(AccountTransactionsProcessor::new(db_pool))
        },
        ProcessorConfig::AnsProcessor(config) => {
            Processor::from(AnsProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::CoinProcessor => Processor::from(CoinProcessor::new(db_pool)),
        ProcessorConfig::DefaultProcessor => Processor::from(DefaultProcessor::new(db_pool)),
        ProcessorConfig::EventsProcessor => Processor::from(EventsProcessor::new(db_pool)),
        ProcessorConfig::FungibleAssetProcessor => {
            Processor::from(FungibleAssetProcessor::new(db_pool))
        },
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
    }
}

pub fn grpc_request_builder(
    starting_version: u64,
    transactions_count: Option<u64>,
    grpc_auth_token: String,
    processor_name: String,
) -> tonic::Request<GetTransactionsRequest> {
    let mut request = tonic::Request::new(GetTransactionsRequest {
        starting_version: Some(starting_version),
        transactions_count,
        ..GetTransactionsRequest::default()
    });
    request.metadata_mut().insert(
        GRPC_API_GATEWAY_API_KEY_HEADER,
        format!("Bearer {}", grpc_auth_token.clone())
            .parse()
            .unwrap(),
    );
    request
        .metadata_mut()
        .insert(GRPC_REQUEST_NAME_HEADER, processor_name.parse().unwrap());
    request
}

pub async fn get_stream(
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    starting_version: u64,
    ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
) -> Response<Streaming<TransactionsResponse>> {
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        "[Parser] Setting up rpc channel"
    );

    let channel = tonic::transport::Channel::from_shared(
        indexer_grpc_data_service_address.to_string(),
    )
    .expect(
        "[Parser] Failed to build GRPC channel, perhaps because the data service URL is invalid",
    )
    .http2_keep_alive_interval(indexer_grpc_http2_ping_interval)
    .keep_alive_timeout(indexer_grpc_http2_ping_timeout);

    // If the scheme is https, add a TLS config.
    let channel = if indexer_grpc_data_service_address.scheme() == "https" {
        let config = tonic::transport::channel::ClientTlsConfig::new();
        channel
            .tls_config(config)
            .expect("[Parser] Failed to create TLS config")
    } else {
        channel
    };

    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        "[Parser] Setting up GRPC client"
    );
    let mut rpc_client = match RawDataClient::connect(channel).await {
        Ok(client) => client
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_RESPONSE_SIZE)
            .max_encoding_message_size(MAX_RESPONSE_SIZE),
        Err(e) => {
            error!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                start_version = starting_version,
                ending_version = ending_version,
                error = ?e,
                "[Parser] Error connecting to GRPC client"
            );
            panic!("[Parser] Error connecting to GRPC client");
        },
    };
    let count = ending_version.map(|v| (v as i64 - starting_version as i64 + 1) as u64);
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        num_of_transactions = ?count,
        "[Parser] Setting up GRPC stream",
    );
    let request = grpc_request_builder(starting_version, count, auth_token, processor_name);
    rpc_client
        .get_transactions(request)
        .await
        .expect("[Parser] Failed to get grpc response. Is the server running?")
}

/// Gets a batch of transactions from the stream. Batch size is set in the grpc server.
/// The number of batches depends on our config
/// There could be several special scenarios:
/// 1. If we lose the connection, we will try reconnecting X times within Y seconds before crashing.
/// 2. If we specified an end version and we hit that, we will stop fetching, but we will make sure that
/// all existing transactions are processed
pub async fn create_fetcher_loop(
    tx: tokio::sync::mpsc::Sender<TransactionsPBResponse>,
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    starting_version: u64,
    request_ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
    batch_start_version: u64,
) {
    let mut grpc_channel_recv_latency = std::time::Instant::now();
    let mut next_version_to_fetch = batch_start_version;
    let mut reconnection_retries = 0;
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = request_ending_version,
        "[Parser] Connecting to GRPC stream",
    );
    let mut response = get_stream(
        indexer_grpc_data_service_address.clone(),
        indexer_grpc_http2_ping_interval,
        indexer_grpc_http2_ping_timeout,
        starting_version,
        request_ending_version,
        auth_token.clone(),
        processor_name.to_string(),
    )
    .await;
    let mut connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        connection_id,
        start_version = starting_version,
        end_version = request_ending_version,
        "[Parser] Successfully connected to GRPC stream",
    );

    loop {
        let is_success = match resp_stream.next().await {
            Some(Ok(r)) => {
                reconnection_retries = 0;
                let start_version = r.transactions.as_slice().first().unwrap().version;
                let start_txn_timestamp =
                    r.transactions.as_slice().first().unwrap().timestamp.clone();
                let end_version = r.transactions.as_slice().last().unwrap().version;
                let end_txn_timestamp = r.transactions.as_slice().last().unwrap().timestamp.clone();
                next_version_to_fetch = end_version + 1;
                let size_in_bytes = r.encoded_len() as u64;
                let chain_id: u64 = r.chain_id.expect("[Parser] Chain Id doesn't exist.");

                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version,
                    end_version,
                    start_txn_timestamp_iso = start_txn_timestamp
                        .clone()
                        .map(|t| timestamp_to_iso(&t))
                        .unwrap_or_default(),
                    end_txn_timestamp_iso = end_txn_timestamp
                        .map(|t| timestamp_to_iso(&t))
                        .unwrap_or_default(),
                    num_of_transactions = end_version - start_version + 1,
                    channel_size = BUFFER_SIZE - tx.capacity(),
                    size_in_bytes = r.encoded_len() as f64,
                    duration_in_secs = grpc_channel_recv_latency.elapsed().as_secs_f64(),
                    tps = (r.transactions.len() as f64
                        / grpc_channel_recv_latency.elapsed().as_secs_f64())
                        as u64,
                    bytes_per_sec =
                        r.encoded_len() as f64 / grpc_channel_recv_latency.elapsed().as_secs_f64(),
                    step = ProcessorStep::ReceivedTxnsFromGrpc.get_step(),
                    "{}",
                    ProcessorStep::ReceivedTxnsFromGrpc.get_label(),
                );
                LATEST_PROCESSED_VERSION
                    .with_label_values(&[
                        &processor_name,
                        ProcessorStep::ReceivedTxnsFromGrpc.get_step(),
                        ProcessorStep::ReceivedTxnsFromGrpc.get_label(),
                    ])
                    .set(end_version as i64);
                TRANSACTION_UNIX_TIMESTAMP
                    .with_label_values(&[
                        &processor_name,
                        ProcessorStep::ReceivedTxnsFromGrpc.get_step(),
                        ProcessorStep::ReceivedTxnsFromGrpc.get_label(),
                    ])
                    .set(
                        start_txn_timestamp
                            .map(|t| timestamp_to_unixtime(&t))
                            .unwrap_or_default(),
                    );
                PROCESSED_BYTES_COUNT
                    .with_label_values(&[
                        &processor_name,
                        ProcessorStep::ReceivedTxnsFromGrpc.get_step(),
                        ProcessorStep::ReceivedTxnsFromGrpc.get_label(),
                    ])
                    .inc_by(size_in_bytes);
                NUM_TRANSACTIONS_PROCESSED_COUNT
                    .with_label_values(&[
                        &processor_name,
                        ProcessorStep::ReceivedTxnsFromGrpc.get_step(),
                        ProcessorStep::ReceivedTxnsFromGrpc.get_label(),
                    ])
                    .inc_by(end_version - start_version + 1);

                let txn_channel_send_latency = std::time::Instant::now();
                let txn_pb = TransactionsPBResponse {
                    transactions: r.transactions,
                    chain_id,
                    size_in_bytes,
                };
                match tx.send(txn_pb.clone()).await {
                    Ok(()) => {},
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = indexer_grpc_data_service_address.to_string(),
                            connection_id,
                            channel_size = BUFFER_SIZE - tx.capacity(),
                            error = ?e,
                            "[Parser] Error sending GRPC response to channel."
                        );
                        panic!("[Parser] Error sending GRPC response to channel.")
                    },
                }
                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version = start_version,
                    end_version = end_version,
                    channel_size = BUFFER_SIZE - tx.capacity(),
                    size_in_bytes = txn_pb.size_in_bytes,
                    duration_in_secs = txn_channel_send_latency.elapsed().as_secs_f64(),
                    tps = (txn_pb.transactions.len() as f64
                        / txn_channel_send_latency.elapsed().as_secs_f64())
                        as u64,
                    bytes_per_sec = txn_pb.size_in_bytes as f64
                        / txn_channel_send_latency.elapsed().as_secs_f64(),
                    "[Parser] Successfully sent transactions to channel."
                );
                FETCHER_THREAD_CHANNEL_SIZE
                    .with_label_values(&[&processor_name])
                    .set((BUFFER_SIZE - tx.capacity()) as i64);
                grpc_channel_recv_latency = std::time::Instant::now();
                true
            },
            Some(Err(rpc_error)) => {
                tracing::warn!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version = starting_version,
                    end_version = request_ending_version,
                    error = ?rpc_error,
                    "[Parser] Error receiving datastream response."
                );
                false
            },
            None => {
                tracing::warn!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version = starting_version,
                    end_version = request_ending_version,
                    "[Parser] Stream ended."
                );
                false
            },
        };
        // Check if we're at the end of the stream
        let is_end = if let Some(ending_version) = request_ending_version {
            next_version_to_fetch > ending_version
        } else {
            false
        };
        if is_end {
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                ending_version = request_ending_version,
                next_version_to_fetch = next_version_to_fetch,
                "[Parser] Reached ending version.",
            );
            // Wait for the fetched transactions to finish processing before closing the channel
            loop {
                let channel_capacity = tx.capacity();
                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    channel_size = BUFFER_SIZE - channel_capacity,
                    "[Parser] Waiting for channel to be empty"
                );
                if channel_capacity == BUFFER_SIZE {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                "[Parser] The stream is ended."
            );
            break;
        } else {
            // The rest is to see if we need to reconnect
            if is_success {
                continue;
            }

            // Sleep for 100ms between reconnect tries
            // TODO: Turn this into exponential backoff
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if reconnection_retries >= RECONNECTION_MAX_RETRIES {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    "[Parser] Reconnected more than 100 times. Will not retry.",
                );
                panic!("[Parser] Reconnected more than 100 times. Will not retry.")
            }
            reconnection_retries += 1;
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Reconnecting to GRPC stream"
            );
            response = get_stream(
                indexer_grpc_data_service_address.clone(),
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                next_version_to_fetch,
                request_ending_version,
                auth_token.clone(),
                processor_name.to_string(),
            )
            .await;
            connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
                Some(connection_id) => connection_id.to_str().unwrap().to_string(),
                None => "".to_string(),
            };
            resp_stream = response.into_inner();
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Successfully reconnected to GRPC stream"
            );
        }
    }
}
