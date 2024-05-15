// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::IndexerGrpcHttp2Config,
    grpc_stream::TransactionsPBResponse,
    models::{ledger_info::LedgerInfo, processor_status::ProcessorStatusQuery},
    processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        coin_processor::CoinProcessor, default_processor::DefaultProcessor,
        events_processor::EventsProcessor, fungible_asset_processor::FungibleAssetProcessor,
        monitoring_processor::MonitoringProcessor, nft_metadata_processor::NftMetadataProcessor,
        objects_processor::ObjectsProcessor, stake_processor::StakeProcessor,
        token_processor::TokenProcessor, token_v2_processor::TokenV2Processor,
        transaction_metadata_processor::TransactionMetadataProcessor,
        user_transaction_processor::UserTransactionProcessor, ProcessingResult, Processor,
        ProcessorConfig, ProcessorTrait,
    },
    schema::ledger_infos,
    transaction_filter::TransactionFilter,
    utils::{
        counters::{
            ProcessorStep, GRPC_LATENCY_BY_PROCESSOR_IN_SECS, LATEST_PROCESSED_VERSION,
            NUM_TRANSACTIONS_PROCESSED_COUNT, PB_CHANNEL_FETCH_WAIT_TIME_SECS,
            PROCESSED_BYTES_COUNT, PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS, PROCESSOR_ERRORS_COUNT,
            PROCESSOR_INVOCATIONS_COUNT, PROCESSOR_SUCCESSES_COUNT,
            SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS, SINGLE_BATCH_PARSING_TIME_IN_SECS,
            SINGLE_BATCH_PROCESSING_TIME_IN_SECS, TRANSACTION_UNIX_TIMESTAMP,
        },
        database::{execute_with_better_error_conn, new_db_pool, run_pending_migrations, PgDbPool},
        util::{time_diff_since_pb_timestamp_in_secs, timestamp_to_iso, timestamp_to_unixtime},
    },
};
use ahash::AHashMap;
use anyhow::{Context, Result};
use aptos_moving_average::MovingAverage;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use url::Url;

// this is how large the fetch queue should be. Each bucket should have a max of 80MB or so, so a batch
// of 50 means that we could potentially have at least 4.8GB of data in memory at any given time and that we should provision
// machines accordingly.
pub const BUFFER_SIZE: usize = 100;
pub const PROCESSOR_SERVICE_TYPE: &str = "processor";

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
    pub grpc_chain_id: Option<u64>,
    pub pb_channel_txn_chunk_size: usize,
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    pub enable_verbose_logging: Option<bool>,
    pub transaction_filter: TransactionFilter,
    pub grpc_response_item_timeout_in_secs: u64,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
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
        // The number of transactions per protobuf batch
        pb_channel_txn_chunk_size: usize,
        per_table_chunk_sizes: AHashMap<String, usize>,
        enable_verbose_logging: Option<bool>,
        transaction_filter: TransactionFilter,
        grpc_response_item_timeout_in_secs: u64,
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
            grpc_chain_id: None,
            pb_channel_txn_chunk_size,
            per_table_chunk_sizes,
            enable_verbose_logging,
            transaction_filter,
            grpc_response_item_timeout_in_secs,
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

        // get the chain id
        let chain_id = crate::grpc_stream::get_chain_id(
            self.indexer_grpc_data_service_address.clone(),
            self.grpc_http2_config.grpc_http2_ping_interval_in_secs(),
            self.grpc_http2_config.grpc_http2_ping_timeout_in_secs(),
            self.grpc_http2_config.grpc_connection_timeout_secs(),
            self.auth_token.clone(),
            processor_name.to_string(),
        )
        .await;
        self.check_or_update_chain_id(chain_id as i64)
            .await
            .unwrap();

        self.grpc_chain_id = Some(chain_id);

        let ending_version = self.ending_version;
        let indexer_grpc_data_service_address = self.indexer_grpc_data_service_address.clone();
        let indexer_grpc_http2_ping_interval =
            self.grpc_http2_config.grpc_http2_ping_interval_in_secs();
        let indexer_grpc_http2_ping_timeout =
            self.grpc_http2_config.grpc_http2_ping_timeout_in_secs();
        let indexer_grpc_reconnection_timeout_secs =
            self.grpc_http2_config.grpc_connection_timeout_secs();
        let pb_channel_txn_chunk_size = self.pb_channel_txn_chunk_size;

        // Create a transaction fetcher thread that will continuously fetch transactions from the GRPC stream
        // and write into a channel
        // TODO: change channel size based on number_concurrent_processing_tasks
        let (tx, receiver) = kanal::bounded_async::<TransactionsPBResponse>(BUFFER_SIZE);
        let request_ending_version = self.ending_version;
        let auth_token = self.auth_token.clone();
        let transaction_filter = self.transaction_filter.clone();
        let grpc_response_item_timeout =
            std::time::Duration::from_secs(self.grpc_response_item_timeout_in_secs);
        let fetcher_task = tokio::spawn(async move {
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
                indexer_grpc_reconnection_timeout_secs,
                grpc_response_item_timeout,
                starting_version,
                request_ending_version,
                auth_token.clone(),
                processor_name.to_string(),
                transaction_filter,
                pb_channel_txn_chunk_size,
            )
            .await
        });

        // Create a gap detector task that will panic if there is a gap in the processing
        let (gap_detector_sender, gap_detector_receiver) =
            kanal::bounded_async::<ProcessingResult>(BUFFER_SIZE);
        let gap_detection_batch_size = self.gap_detection_batch_size;
        let processor = build_processor(
            &self.processor_config,
            self.per_table_chunk_sizes.clone(),
            self.db_pool.clone(),
        );
        tokio::spawn(async move {
            crate::gap_detector::create_gap_detector_status_tracker_loop(
                gap_detector_receiver,
                processor,
                starting_version,
                gap_detection_batch_size,
            )
            .await;
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
            concurrent_tasks,
            "[Parser] Spawning concurrent parallel processor tasks",
        );

        let mut processor_tasks = vec![fetcher_task];
        for task_index in 0..concurrent_tasks {
            let join_handle = self
                .launch_processor_task(task_index, receiver.clone(), gap_detector_sender.clone())
                .await;
            processor_tasks.push(join_handle);
        }

        info!(
            processor_name = processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.as_str(),
            concurrent_tasks,
            "[Parser] Processor tasks spawned",
        );

        // Await the processor tasks: this is forever
        futures::future::try_join_all(processor_tasks)
            .await
            .expect("[Processor] Processor tasks have died");
    }

    async fn launch_processor_task(
        &self,
        task_index: usize,
        receiver: kanal::AsyncReceiver<TransactionsPBResponse>,
        gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
    ) -> JoinHandle<()> {
        let processor_name = self.processor_config.name();
        let stream_address = self.indexer_grpc_data_service_address.to_string();
        let receiver_clone = receiver.clone();
        let auth_token = self.auth_token.clone();

        // Build the processor based on the config.
        let processor = build_processor(
            &self.processor_config,
            self.per_table_chunk_sizes.clone(),
            self.db_pool.clone(),
        );

        let concurrent_tasks = self.number_concurrent_processing_tasks;

        let chain_id = self
            .grpc_chain_id
            .expect("GRPC chain ID has not been fetched yet!");
        tokio::spawn(async move {
            let task_index_str = task_index.to_string();
            let step = ProcessorStep::ProcessedBatch.get_step();
            let label = ProcessorStep::ProcessedBatch.get_label();
            let mut ma = MovingAverage::new(3000);

            loop {
                let txn_channel_fetch_latency = std::time::Instant::now();

                match fetch_transactions(
                    processor_name,
                    &stream_address,
                    receiver_clone.clone(),
                    task_index,
                )
                .await
                {
                    Ok(transactions_pb) => {
                        let size_in_bytes = transactions_pb.size_in_bytes as f64;
                        let first_txn_version = transactions_pb
                            .transactions
                            .first()
                            .map(|t| t.version)
                            .unwrap_or_default();
                        let batch_first_txn_version = transactions_pb.start_version;
                        let last_txn_version = transactions_pb
                            .transactions
                            .last()
                            .map(|t| t.version)
                            .unwrap_or_default();
                        let batch_last_txn_version = transactions_pb.end_version;
                        let start_txn_timestamp = transactions_pb.start_txn_timestamp.clone();
                        let end_txn_timestamp = transactions_pb.end_txn_timestamp.clone();

                        let start_txn_timestamp_unix = start_txn_timestamp
                            .as_ref()
                            .map(timestamp_to_unixtime)
                            .unwrap_or_default();
                        let start_txn_timestamp_iso = start_txn_timestamp
                            .as_ref()
                            .map(timestamp_to_iso)
                            .unwrap_or_default();
                        let end_txn_timestamp_iso = end_txn_timestamp
                            .as_ref()
                            .map(timestamp_to_iso)
                            .unwrap_or_default();

                        let txn_channel_fetch_latency_sec =
                            txn_channel_fetch_latency.elapsed().as_secs_f64();

                        debug!(
                            processor_name = processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            start_version = batch_first_txn_version,
                            end_version = batch_last_txn_version,
                            num_of_transactions =
                                (batch_last_txn_version - batch_first_txn_version) as i64 + 1,
                            size_in_bytes,
                            task_index,
                            duration_in_secs = txn_channel_fetch_latency_sec,
                            tps = (batch_last_txn_version as f64 - batch_first_txn_version as f64)
                                / txn_channel_fetch_latency_sec,
                            bytes_per_sec = size_in_bytes / txn_channel_fetch_latency_sec,
                            "[Parser][T#{}] Successfully fetched transactions from channel.",
                            task_index
                        );

                        // Ensure chain_id has not changed
                        if transactions_pb.chain_id != chain_id {
                            error!(
                                processor_name = processor_name,
                                stream_address = stream_address.as_str(),
                                chain_id = transactions_pb.chain_id,
                                existing_id = chain_id,
                                task_index,
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
                            transactions_pb,
                            &processor,
                            chain_id,
                            processor_name,
                            &auth_token,
                            false, // enable_verbose_logging
                        )
                        .await;

                        let processing_result = match res {
                            Ok(versions) => {
                                PROCESSOR_SUCCESSES_COUNT
                                    .with_label_values(&[processor_name])
                                    .inc();
                                versions
                            },
                            Err(e) => {
                                error!(
                                    processor_name = processor_name,
                                    stream_address = stream_address.as_str(),
                                    error = ?e,
                                    task_index,
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

                        let processing_time = processing_time.elapsed().as_secs_f64();

                        // We've processed things: do some data and metrics

                        ma.tick_now((last_txn_version - first_txn_version) + 1);
                        let tps = ma.avg().ceil() as u64;

                        let num_processed = (last_txn_version - first_txn_version) + 1;

                        debug!(
                            processor_name = processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            first_txn_version,
                            batch_first_txn_version,
                            last_txn_version,
                            batch_last_txn_version,
                            start_txn_timestamp_iso,
                            end_txn_timestamp_iso,
                            num_of_transactions = num_processed,
                            concurrent_tasks,
                            task_index,
                            size_in_bytes,
                            processing_duration_in_secs =
                                processing_result.processing_duration_in_secs,
                            db_insertion_duration_in_secs =
                                processing_result.db_insertion_duration_in_secs,
                            duration_in_secs = processing_time,
                            tps = tps,
                            bytes_per_sec = size_in_bytes / processing_time,
                            step = &step,
                            "{}",
                            label,
                        );

                        // TODO: For these three, do an atomic thing, or ideally move to an async metrics collector!
                        GRPC_LATENCY_BY_PROCESSOR_IN_SECS
                            .with_label_values(&[processor_name, &task_index_str])
                            .set(time_diff_since_pb_timestamp_in_secs(
                                end_txn_timestamp.as_ref().unwrap(),
                            ));
                        LATEST_PROCESSED_VERSION
                            .with_label_values(&[processor_name, step, label, &task_index_str])
                            .set(last_txn_version as i64);
                        TRANSACTION_UNIX_TIMESTAMP
                            .with_label_values(&[processor_name, step, label, &task_index_str])
                            .set(start_txn_timestamp_unix);

                        // Single batch metrics
                        PROCESSED_BYTES_COUNT
                            .with_label_values(&[processor_name, step, label, &task_index_str])
                            .inc_by(size_in_bytes as u64);
                        NUM_TRANSACTIONS_PROCESSED_COUNT
                            .with_label_values(&[processor_name, step, label, &task_index_str])
                            .inc_by(num_processed);

                        SINGLE_BATCH_PROCESSING_TIME_IN_SECS
                            .with_label_values(&[processor_name, &task_index_str])
                            .set(processing_time);
                        SINGLE_BATCH_PARSING_TIME_IN_SECS
                            .with_label_values(&[processor_name, &task_index_str])
                            .set(processing_result.processing_duration_in_secs);
                        SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS
                            .with_label_values(&[processor_name, &task_index_str])
                            .set(processing_result.db_insertion_duration_in_secs);

                        // Send the result to the gap detector
                        gap_detector_sender
                            .send(processing_result)
                            .await
                            .expect("[Parser] Failed to send versions to gap detector");
                    },
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = stream_address.as_str(),
                            error = ?e,
                            task_index,
                            "[Parser][T#{}] Consumer thread error fetching transactions from channel", task_index
                        );
                        break;
                    },
                }
            }
        })
    }

    // For the normal processor build we just use standard Diesel with the postgres
    // feature enabled (which uses libpq under the hood, hence why we named the feature
    // this way).
    #[cfg(feature = "libpq")]
    async fn run_migrations(&self) {
        use crate::diesel::Connection;
        use diesel::pg::PgConnection;

        info!("Running migrations: {:?}", self.postgres_connection_string);
        let mut conn =
            PgConnection::establish(&self.postgres_connection_string).expect("migrations failed!");
        run_pending_migrations(&mut conn);
    }

    // If the libpq feature isn't enabled, we use diesel async instead. This is used by
    // the CLI for the local testnet, where we cannot tolerate the libpq dependency.
    #[cfg(not(feature = "libpq"))]
    async fn run_migrations(&self) {
        use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;

        info!("Running migrations: {:?}", self.postgres_connection_string);
        let conn = self
            .db_pool
            // We need to use this since AsyncConnectionWrapper doesn't know how to
            // work with a pooled connection.
            .dedicated_connection()
            .await
            .expect("[Parser] Failed to get connection");
        // We use spawn_blocking since run_pending_migrations is a blocking function.
        tokio::task::spawn_blocking(move || {
            // This lets us use the connection like a normal diesel connection. See more:
            // https://docs.rs/diesel-async/latest/diesel_async/async_connection_wrapper/type.AsyncConnectionWrapper.html
            let mut conn: AsyncConnectionWrapper<diesel_async::AsyncPgConnection> =
                AsyncConnectionWrapper::from(conn);
            run_pending_migrations(&mut conn);
        })
        .await
        .expect("[Parser] Failed to run migrations");
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

async fn fetch_transactions(
    processor_name: &str,
    stream_address: &str,
    receiver: kanal::AsyncReceiver<TransactionsPBResponse>,
    task_index: usize,
) -> Result<TransactionsPBResponse> {
    let pb_channel_fetch_time = std::time::Instant::now();
    let txn_pb_res = receiver.recv().await;
    // Track how much time this task spent waiting for a pb bundle
    PB_CHANNEL_FETCH_WAIT_TIME_SECS
        .with_label_values(&[processor_name, &task_index.to_string()])
        .set(pb_channel_fetch_time.elapsed().as_secs_f64());

    match txn_pb_res {
        Ok(txn_pb) => Ok(txn_pb),
        Err(_e) => {
            error!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = stream_address,
                "[Parser][T#{}] Consumer thread receiver channel closed.",
                task_index
            );
            Err(anyhow::anyhow!(
                "[Parser][T#{}] Consumer thread receiver channel closed.",
                task_index
            ))
        },
    }
}

pub async fn do_processor(
    transactions_pb: TransactionsPBResponse,
    processor: &Processor,
    db_chain_id: u64,
    processor_name: &str,
    auth_token: &str,
    enable_verbose_logging: bool,
) -> Result<ProcessingResult> {
    // We use the value passed from the `transactions_pb` as it may have been filtered
    let start_version = transactions_pb.start_version;
    let end_version = transactions_pb.end_version;

    // Fake this as it's possible we have filtered out all of the txns in this batch
    if transactions_pb.transactions.is_empty() {
        return Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs: 0.0,
            db_insertion_duration_in_secs: 0.0,
            last_transaction_timestamp: transactions_pb.end_txn_timestamp,
        });
    }

    let txn_time = transactions_pb.start_txn_timestamp;

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

    let processed_result = processor
        .process_transactions(
            transactions_pb.transactions,
            start_version,
            end_version,
            Some(db_chain_id),
        )
        .await;

    if let Some(ref t) = txn_time {
        PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS
            .with_label_values(&[auth_token, processor_name])
            .set(time_diff_since_pb_timestamp_in_secs(t));
    }

    processed_result
}

/// Given a config and a db pool, build a concrete instance of a processor.
// As time goes on there might be other things that we need to provide to certain
// processors. As that happens we can revist whether this function (which tends to
// couple processors together based on their args) makes sense.
// TODO: This is not particularly easily extensible; better to refactor to use a trait, and then share one extensible config model (allowing for only one arity)
pub fn build_processor(
    config: &ProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
    db_pool: PgDbPool,
) -> Processor {
    match config {
        ProcessorConfig::AccountTransactionsProcessor => Processor::from(
            AccountTransactionsProcessor::new(db_pool, per_table_chunk_sizes),
        ),
        ProcessorConfig::AnsProcessor(config) => Processor::from(AnsProcessor::new(
            db_pool,
            config.clone(),
            per_table_chunk_sizes,
        )),
        ProcessorConfig::CoinProcessor => {
            Processor::from(CoinProcessor::new(db_pool, per_table_chunk_sizes))
        },
        ProcessorConfig::DefaultProcessor => {
            Processor::from(DefaultProcessor::new(db_pool, per_table_chunk_sizes))
        },
        ProcessorConfig::EventsProcessor => {
            Processor::from(EventsProcessor::new(db_pool, per_table_chunk_sizes))
        },
        ProcessorConfig::FungibleAssetProcessor => {
            Processor::from(FungibleAssetProcessor::new(db_pool, per_table_chunk_sizes))
        },
        ProcessorConfig::MonitoringProcessor => Processor::from(MonitoringProcessor::new(db_pool)),
        ProcessorConfig::NftMetadataProcessor(config) => {
            Processor::from(NftMetadataProcessor::new(db_pool, config.clone()))
        },
        ProcessorConfig::ObjectsProcessor(config) => Processor::from(ObjectsProcessor::new(
            db_pool,
            config.clone(),
            per_table_chunk_sizes,
        )),
        ProcessorConfig::StakeProcessor(config) => Processor::from(StakeProcessor::new(
            db_pool,
            config.clone(),
            per_table_chunk_sizes,
        )),
        ProcessorConfig::TokenProcessor(config) => Processor::from(TokenProcessor::new(
            db_pool,
            config.clone(),
            per_table_chunk_sizes,
        )),
        ProcessorConfig::TokenV2Processor(config) => Processor::from(TokenV2Processor::new(
            db_pool,
            config.clone(),
            per_table_chunk_sizes,
        )),
        ProcessorConfig::TransactionMetadataProcessor => Processor::from(
            TransactionMetadataProcessor::new(db_pool, per_table_chunk_sizes),
        ),
        ProcessorConfig::UserTransactionProcessor => Processor::from(
            UserTransactionProcessor::new(db_pool, per_table_chunk_sizes),
        ),
    }
}
