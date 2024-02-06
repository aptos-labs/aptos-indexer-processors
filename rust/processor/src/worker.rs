// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::IndexerGrpcHttp2Config,
    models::{ledger_info::LedgerInfo, processor_status::ProcessorStatusQuery},
    processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        coin_processor::CoinProcessor, default_processor::DefaultProcessor,
        events_processor::EventsProcessor, fungible_asset_processor::FungibleAssetProcessor,
        monitoring_processor::MonitoringProcessor, nft_metadata_processor::NftMetadataProcessor,
        objects_processor::ObjectsProcessor, stake_processor::StakeProcessor,
        token_processor::TokenProcessor, token_v2_processor::TokenV2Processor,
        user_transaction_processor::UserTransactionProcessor, ProcessingResult, Processor,
        ProcessorConfig, ProcessorTrait,
    },
    schema::ledger_infos,
    utils::{
        counters::{
            // TODO: where do I stick `GRPC_LATENCY_BY_PROCESSOR_IN_SECS` now?
            ProcessorStep,
            LATEST_PROCESSED_VERSION,
            MULTI_BATCH_PROCESSING_TIME_IN_SECS,
            NUM_TRANSACTIONS_PROCESSED_COUNT,
            PB_CHANNEL_FETCH_WAIT_TIME_SECS,
            PROCESSED_BYTES_COUNT,
            PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS,
            PROCESSOR_ERRORS_COUNT,
            PROCESSOR_INVOCATIONS_COUNT,
            PROCESSOR_SUCCESSES_COUNT,
            SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS,
            SINGLE_BATCH_PARSING_TIME_IN_SECS,
            SINGLE_BATCH_PROCESSING_TIME_IN_SECS,
            TRANSACTION_UNIX_TIMESTAMP,
        },
        database::{execute_with_better_error_conn, new_db_pool, run_pending_migrations, PgDbPool},
        util::{time_diff_since_pb_timestamp_in_secs, timestamp_to_iso, timestamp_to_unixtime},
    },
};
use anyhow::{Context, Result};
use aptos_moving_average::MovingAverage;
use aptos_protos::transaction::v1::Transaction;
use diesel::Connection;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::timeout};
use tracing::{error, info};
use url::Url;

// this is how large the fetch queue should be. Each bucket should have a max of 80MB or so, so a batch
// of 50 means that we could potentially have at least 4.8GB of data in memory at any given time and that we should provision
// machines accordingly.
pub const BUFFER_SIZE: usize = 100;
// Consumer thread will wait X seconds before panicking if it doesn't receive any data
pub const CONSUMER_THREAD_TIMEOUT_IN_SECS: u64 = 60 * 5;
pub const PROCESSOR_SERVICE_TYPE: &str = "processor";

#[derive(Clone)]
pub struct TransactionsPBResponse {
    pub transactions: Vec<Arc<Transaction>>,
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
        // TODO: MAKE THIS POOL SIZE CONFIGURABLE!
        // TODO: MAKE THIS POOL SIZE  CONFIGURABLE!
        // TODO: MAKE THIS POOL SIZE  CONFIGURABLE!
        // TODO: MAKE THIS POOL SIZE  CONFIGURABLE!
        let conn_pool = new_db_pool(&postgres_connection_string, 200)
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

        let starting_version = self.starting_version.unwrap_or(starting_version_from_db);

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
        let processor = Arc::new(build_processor(
            &self.processor_config,
            self.db_pool.clone(),
        ));

        // get the chain id
        let chain_id = crate::grpc_stream::get_chain_id(
            self.indexer_grpc_data_service_address.clone(),
            self.grpc_http2_config.grpc_http2_ping_interval_in_secs(),
            self.grpc_http2_config.grpc_http2_ping_timeout_in_secs(),
            self.auth_token.clone(),
            processor_name.to_string(),
        )
        .await;
        self.check_or_update_chain_id(chain_id as i64)
            .await
            .unwrap();

        // This is the moving average that we use to calculate TPS
        let ma = Arc::new(Mutex::new(MovingAverage::new(10)));

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
                start_version = starting_version,
                "[Parser] Starting fetcher thread"
            );

            crate::grpc_stream::create_fetcher_loop(
                tx.clone(),
                indexer_grpc_data_service_address.clone(),
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                starting_version,
                request_ending_version,
                auth_token.clone(),
                processor_name.to_string(),
                starting_version,
                BUFFER_SIZE,
            )
            .await
        });

        // This is the consumer side of the channel. These are the major states:
        // 1. We're backfilling so we should expect many concurrent threads to process transactions
        // 2. We're caught up so we should expect a single thread to process transactions
        // 3. We have received either an empty batch or a batch with a gap. We should panic.
        // 4. We have not received anything in X seconds, we should panic.
        // 5. If it's the wrong chain, panic.

        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.as_str(),
            "[Parser] Fetching transaction batches from channel",
        );
        let txn_channel_fetch_latency = std::time::Instant::now();

        let mut tasks = vec![];
        for task_index in 0..concurrent_tasks {
            let auth_token = self.auth_token.clone();
            let stream_address = self.indexer_grpc_data_service_address.to_string();
            let processor_clone = processor.clone();
            let ma_clone = ma.clone();
            let receiver_clone = receiver.clone();

            let join_handle = tokio::spawn(async move {
                loop {
                    let pb_channel_fetch_time = std::time::Instant::now();
                    let txn_pb_res = timeout(
                        Duration::from_secs(CONSUMER_THREAD_TIMEOUT_IN_SECS),
                        receiver_clone.recv(),
                    )
                    .await;
                    // Track how much time this thread spent waiting for a pb bundle
                    PB_CHANNEL_FETCH_WAIT_TIME_SECS
                        .with_label_values(&[processor_name, &task_index.to_string()])
                        .set(pb_channel_fetch_time.elapsed().as_secs_f64());

                    let txn_pb = match txn_pb_res {
                        Ok(txn_pb) => match txn_pb {
                            Ok(txn_pb) => txn_pb,
                            // This happens when the channel is closed. We should panic.
                            Err(_e) => {
                                error!(
                                    processor_name = processor_name,
                                    service_type = PROCESSOR_SERVICE_TYPE,
                                    stream_address = stream_address,
                                    "[Parser][T#{}] Channel closed; stream ended.",
                                    task_index
                                );
                                panic!("[Parser][T#{}] Channel closed", task_index);
                            },
                        },
                        Err(_e) => {
                            error!(
                                processor_name = processor_name,
                                service_type = PROCESSOR_SERVICE_TYPE,
                                stream_address = stream_address,
                                "[Parser][T#{}] Consumer thread timed out waiting for transactions",
                                task_index
                            );
                            panic!(
                                "[Parser][T#{}] Consumer thread timed out waiting for transactions",
                                task_index
                            );
                        },
                    };

                    let size_in_bytes = txn_pb.size_in_bytes as f64;
                    let first_txn = txn_pb.transactions.as_slice().first().unwrap();
                    let first_txn_version = first_txn.version;
                    let first_txn_timestamp = first_txn.timestamp.clone();
                    let last_txn = txn_pb.transactions.as_slice().last().unwrap();
                    let last_txn_version = last_txn.version;
                    let last_txn_timestamp = last_txn.timestamp.clone();

                    info!(
                        processor_name = processor_name,
                        service_type = PROCESSOR_SERVICE_TYPE,
                        start_version = first_txn_version,
                        end_version = last_txn_version,
                        num_of_transactions = (last_txn_version - first_txn_version) as i64 + 1,
                        size_in_bytes,
                        duration_in_secs = txn_channel_fetch_latency.elapsed().as_secs_f64(),
                        tps = (last_txn_version as f64 - first_txn_version as f64)
                            / txn_channel_fetch_latency.elapsed().as_secs_f64(),
                        bytes_per_sec =
                            size_in_bytes / txn_channel_fetch_latency.elapsed().as_secs_f64(),
                        "[Parser][T#{}] Successfully fetched transaction batches from channel.",
                        task_index
                    );

                    // Ensure chain_id has not changed
                    if txn_pb.chain_id != chain_id {
                        error!(
                            processor_name = processor_name,
                            stream_address = stream_address,
                            chain_id = txn_pb.chain_id,
                            existing_id = chain_id,
                            "[Parser][T#{}] Stream somehow changed chain id!",
                            task_index
                        );
                        panic!(
                            "[Parser][T#{}] Stream somehow changed chain id!",
                            task_index
                        );
                    }

                    let processing_time = std::time::Instant::now();

                    let res = do_processor(
                        txn_pb,
                        processor_clone.clone(),
                        chain_id,
                        processor_name,
                        &auth_token,
                        enable_verbose_logging,
                    )
                    .await;

                    let _processed = match res {
                        Ok(versions) => {
                            PROCESSOR_SUCCESSES_COUNT
                                .with_label_values(&[processor_name])
                                .inc();
                            versions
                        },
                        Err(e) => {
                            error!(
                                processor_name = processor_name,
                                stream_address = stream_address,
                                error = ?e,
                                "[Parser][T#{}] Error processing transactions", task_index
                            );
                            PROCESSOR_ERRORS_COUNT
                                .with_label_values(&[processor_name])
                                .inc();
                            panic!(
                                "[Parser][T#{}] Error processing '{:}' transactions: {:?}",
                                task_index, processor_name, e
                            );
                        },
                    };

                    /*
                    let mut max_processing_duration_in_secs: f64 = processed.processing_duration_in_secs;
                    let mut max_db_insertion_duration_in_secs: f64 = processed.db_insertion_duration_in_secs;
                    max_processing_duration_in_secs =
                        max_processing_duration_in_secs.max(processed.processing_duration_in_secs);
                    max_db_insertion_duration_in_secs =
                        max_db_insertion_duration_in_secs.max(processed.db_insertion_duration_in_secs);
                       */

                    processor_clone
                        .update_last_processed_version(last_txn_version, last_txn_timestamp.clone())
                        .await
                        .unwrap();

                    let mut ma_locked = ma_clone.lock().await;
                    ma_locked.tick_now(last_txn_version - first_txn_version + 1);
                    let tps = (ma_locked.avg() * 1000.0) as u64;
                    info!(
                        processor_name = processor_name,
                        service_type = PROCESSOR_SERVICE_TYPE,
                        start_version = first_txn_version,
                        end_version = last_txn_version,
                        start_txn_timestamp_iso = first_txn_timestamp
                            .clone()
                            .map(|t| timestamp_to_iso(&t))
                            .unwrap_or_default(),
                        end_txn_timestamp_iso = last_txn_timestamp
                            .map(|t| timestamp_to_iso(&t))
                            .unwrap_or_default(),
                        num_of_transactions = last_txn_version - first_txn_version + 1,
                        task_count = concurrent_tasks,
                        size_in_bytes,
                        duration_in_secs = processing_time.elapsed().as_secs_f64(),
                        tps = tps,
                        bytes_per_sec = size_in_bytes / processing_time.elapsed().as_secs_f64(),
                        step = ProcessorStep::ProcessedMultipleBatches.get_step(),
                        "{}",
                        ProcessorStep::ProcessedMultipleBatches.get_label(),
                    );
                    LATEST_PROCESSED_VERSION
                        .with_label_values(&[
                            processor_name,
                            ProcessorStep::ProcessedMultipleBatches.get_step(),
                            ProcessorStep::ProcessedMultipleBatches.get_label(),
                        ])
                        .set(last_txn_version as i64);
                    // TODO: do an atomic thing, or ideally move to an async metrics collector!
                    TRANSACTION_UNIX_TIMESTAMP
                        .with_label_values(&[
                            processor_name,
                            ProcessorStep::ProcessedMultipleBatches.get_step(),
                            ProcessorStep::ProcessedMultipleBatches.get_label(),
                        ])
                        .set(
                            first_txn_timestamp
                                .map(|t| timestamp_to_unixtime(&t))
                                .unwrap_or_default(),
                        );
                    PROCESSED_BYTES_COUNT
                        .with_label_values(&[
                            processor_name,
                            ProcessorStep::ProcessedMultipleBatches.get_step(),
                            ProcessorStep::ProcessedMultipleBatches.get_label(),
                        ])
                        .inc_by(size_in_bytes as u64);
                    NUM_TRANSACTIONS_PROCESSED_COUNT
                        .with_label_values(&[
                            processor_name,
                            ProcessorStep::ProcessedMultipleBatches.get_step(),
                            ProcessorStep::ProcessedMultipleBatches.get_label(),
                        ])
                        .inc_by(last_txn_version - first_txn_version + 1);
                    MULTI_BATCH_PROCESSING_TIME_IN_SECS
                        .with_label_values(&[processor_name])
                        .set(processing_time.elapsed().as_secs_f64());
                }
            });

            tasks.push(join_handle);
        }

        // This will be forever, until all the indexer dies
        // This will be forever, until all the indexer dies
        // This will be forever, until all the indexer dies
        futures::future::try_join_all(tasks)
            .await
            .expect("[Processor] Processor tasks have died");

        // Update states depending on results of the batch processing
        // let mut processed_versions = vec![];

        // TODO: MAKE THIS GAP HANDLING CODE WORK FOR OUR NEW ASYNC WORLD!
        /*
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
            max_processing_duration_in_secs =
                max_processing_duration_in_secs.max(processing_result.processing_duration_in_secs);
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
        */
    }

    async fn run_migrations(&self) {
        use diesel::pg::PgConnection;

        println!("Running migrations: {:?}", self.postgres_connection_string);
        let mut conn =
            PgConnection::establish(&self.postgres_connection_string).expect("migrations failed!");
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
                execute_with_better_error_conn(
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

pub async fn do_processor(
    transactions_pb: TransactionsPBResponse,
    processor: Arc<Processor>,
    db_chain_id: u64,
    processor_name: &str,
    auth_token: &str,
    enable_verbose_logging: bool,
) -> Result<ProcessingResult> {
    let transactions_pb_slice = transactions_pb.transactions.as_slice();
    let first_txn = transactions_pb_slice.first().unwrap();

    let last_txn = transactions_pb_slice.last().unwrap();

    let start_version = first_txn.version;
    let end_version = last_txn.version;
    let start_txn_timestamp = first_txn.timestamp.clone();
    let end_txn_timestamp = last_txn.timestamp.clone();
    let txn_time = first_txn.timestamp.clone();

    if let Some(ref t) = txn_time {
        PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS
            .with_label_values(&[auth_token, processor_name])
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

    let processed_result = processor
        .process_transactions(
            transactions_pb.transactions,
            start_version,
            end_version,
            // TODO: Change how we fetch chain_id, ideally can be accessed by processors when they are initialized (e.g. so they can have a chain_id field set on new() funciton)
            Some(db_chain_id),
        )
        .await;
    if let Some(ref t) = txn_time {
        PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS
            .with_label_values(&[auth_token, processor_name])
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
            processor_name,
            ProcessorStep::ProcessedBatch.get_step(),
            ProcessorStep::ProcessedBatch.get_label(),
        ])
        .set(end_version as i64);
    TRANSACTION_UNIX_TIMESTAMP
        .with_label_values(&[
            processor_name,
            ProcessorStep::ProcessedBatch.get_step(),
            ProcessorStep::ProcessedBatch.get_label(),
        ])
        .set(start_txn_timestamp_unix);
    PROCESSED_BYTES_COUNT
        .with_label_values(&[
            processor_name,
            ProcessorStep::ProcessedBatch.get_step(),
            ProcessorStep::ProcessedBatch.get_label(),
        ])
        .inc_by(transactions_pb.size_in_bytes);
    NUM_TRANSACTIONS_PROCESSED_COUNT
        .with_label_values(&[
            processor_name,
            ProcessorStep::ProcessedBatch.get_step(),
            ProcessorStep::ProcessedBatch.get_label(),
        ])
        .inc_by(end_version - start_version + 1);

    if let Ok(res) = processed_result {
        SINGLE_BATCH_PROCESSING_TIME_IN_SECS
            .with_label_values(&[processor_name])
            .set(processing_duration.elapsed().as_secs_f64());
        SINGLE_BATCH_PARSING_TIME_IN_SECS
            .with_label_values(&[processor_name])
            .set(res.processing_duration_in_secs);
        SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS
            .with_label_values(&[processor_name])
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
    }
}
