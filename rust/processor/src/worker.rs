// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    models::{ledger_info::LedgerInfo, processor_status::ProcessorStatusQuery},
    processors::{
        coin_processor::CoinTransactionProcessor,
        default_processor::DefaultTransactionProcessor,
        processor_trait::{ProcessingResult, ProcessorTrait},
        stake_processor::StakeTransactionProcessor,
        token_processor::TokenTransactionProcessor,
        Processor,
    },
    schema::ledger_infos,
    utils::{
        counters::{
            LATEST_PROCESSED_VERSION, PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS, PROCESSOR_ERRORS_COUNT,
            PROCESSOR_INVOCATIONS_COUNT, PROCESSOR_SUCCESSES_COUNT,
        },
        database::{execute_with_better_error, new_db_pool, PgDbPool},
        util::time_diff_since_pb_timestamp_in_secs,
    },
};
use anyhow::Context;
use aptos_moving_average::MovingAverage;
use aptos_protos::{
    indexer::v1::{raw_data_client::RawDataClient, GetTransactionsRequest},
    transaction::v1::Transaction,
};
use diesel::{
    pg::PgConnection,
    r2d2::{ConnectionManager, PooledConnection},
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::{StreamExt};
use futures_util::SinkExt;
use tokio::sync::mpsc::error::TryRecvError;
use std::sync::Arc;
use tracing::{error, info};

pub type PgPool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;
pub type PgPoolConnection = PooledConnection<ConnectionManager<PgConnection>>;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();
/// GRPC request metadata key for the token ID.
const GRPC_AUTH_TOKEN_HEADER: &str = "x-aptos-data-authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
// this is how large the fetch queue should be. Each bucket should have a max of 80MB or so, so a batch
// of 50 means that we could potentially have at least 4.8GB of data in memory at any given time and that we should provision
// machines accordingly.
const BUFFER_SIZE: usize = 50;
// If we don't receive any data in X seconds, we should panic.
const NO_DATA_TIMEOUT_SECS: i64 = 5;
// How long to sleep when we get no data b/c we're at the head
const SLEEP_MS: u64 = 50;

const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 20; // 20MB

pub struct Worker {
    pub db_pool: PgDbPool,
    pub processor_name: String,
    pub postgres_connection_string: String,
    pub indexer_grpc_data_service_address: String,
    pub indexer_grpc_http2_ping_interval: std::time::Duration,
    pub indexer_grpc_http2_ping_timeout: std::time::Duration,
    pub auth_token: String,
    pub starting_version: Option<u64>,
    pub ending_version: Option<u64>,
    pub number_concurrent_processing_tasks: usize,
    pub ans_address: Option<String>,
    pub nft_points_contract: Option<String>,
}

impl Worker {
    pub async fn new(
        processor_name: String,
        postgres_connection_string: String,
        indexer_grpc_data_service_address: String,
        indexer_grpc_http2_ping_interval: std::time::Duration,
        indexer_grpc_http2_ping_timeout: std::time::Duration,
        auth_token: String,
        starting_version: Option<u64>,
        ending_version: Option<u64>,
        number_concurrent_processing_tasks: Option<usize>,
        ans_address: Option<String>,
        nft_points_contract: Option<String>,
    ) -> Self {
        info!(processor_name = processor_name, "[Parser] Kicking off");

        info!(
            processor_name = processor_name,
            "[Parser] Creating connection pool"
        );
        let conn_pool =
            new_db_pool(&postgres_connection_string).expect("Failed to create connection pool");
        info!(
            processor_name = processor_name,
            "[Parser] Finish creating the connection pool"
        );
        let number_concurrent_processing_tasks = number_concurrent_processing_tasks.unwrap_or(10);
        Self {
            db_pool: conn_pool,
            processor_name,
            postgres_connection_string,
            indexer_grpc_data_service_address,
            indexer_grpc_http2_ping_interval,
            indexer_grpc_http2_ping_timeout,
            starting_version,
            ending_version,
            auth_token,
            number_concurrent_processing_tasks,
            ans_address,
            nft_points_contract,
        }
    }

    /// This is the main logic of the processor. We will do a few large parts:
    /// 1. Connect to GRPC and handling all the stuff before starting the stream such as diesel migration
    /// 2. Start a thread specifically to fetch data from GRPC. We will keep a buffer of X batches of transactions
    /// 3. Start a loop to consume from the buffer. We will have Y threads to process the transactions in parallel. (Y should be less than X for obvious reasons)
    ///   * Note that the batches will be sequential so we won't have problems with gaps
    /// 4. We will keep track of the last processed version and monitoring things like TPS
    pub async fn run(&mut self) {
        let processor_name = self.processor_name.clone();

        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_address.clone(),
            "[Parser] Connecting to GRPC endpoint",
        );

        let channel = tonic::transport::Channel::from_shared(format!(
            "http://{}",
            self.indexer_grpc_data_service_address.clone()
        ))
        .expect("[Parser] Endpoint is not a valid URI")
        .http2_keep_alive_interval(self.indexer_grpc_http2_ping_interval)
        .keep_alive_timeout(self.indexer_grpc_http2_ping_timeout);

        let mut rpc_client = match RawDataClient::connect(channel).await {
            Ok(client) => client
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(MAX_RESPONSE_SIZE)
                .max_encoding_message_size(MAX_RESPONSE_SIZE),
            Err(e) => {
                error!(
                    processor_name = processor_name,
                    stream_address = self.indexer_grpc_data_service_address.clone(),
                    error = ?e,
                    "[Parser] Error connecting to grpc_stream"
                );
                panic!();
            },
        };
        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_address.clone(),
            "[Parser] Connected to GRPC endpoint",
        );

        info!(
            processor_name = processor_name,
            "[Parser] Running migrations"
        );
        self.run_migrations();
        info!(
            processor_name = processor_name,
            "[Parser] Finished migrations"
        );

        let starting_version_from_db = self
            .get_start_version()
            .expect("[Parser] Database error when getting starting version")
            .unwrap_or_else(|| {
                info!(
                    processor_name = processor_name,
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
            stream_address = self.indexer_grpc_data_service_address.clone(),
            final_start_version = starting_version,
            start_version_from_config = self.starting_version,
            start_version_from_db = starting_version_from_db,
            "[Parser] Making request to GRPC endpoint",
        );

        let request = grpc_request_builder(
            starting_version,
            self.ending_version
                .map(|v| (v as i64 - starting_version as i64 + 1) as u64),
            self.auth_token.clone(),
            self.processor_name.clone(),
        );

        let mut resp_stream = rpc_client
            .get_transactions(request)
            .await
            .expect("[Parser] Failed to get grpc response. Is the server running?")
            .into_inner();

        let concurrent_tasks = self.number_concurrent_processing_tasks;
        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_address.clone(),
            starting_version = starting_version,
            concurrent_tasks = concurrent_tasks,
            "[Parser] Successfully connected to GRPC endpoint. Now instantiating processor",
        );

        // Instantiates correct processor based on config
        let processor_enum = Processor::from_string(&processor_name);
        let processor: Arc<dyn ProcessorTrait> = match processor_enum {
            Processor::CoinProcessor => {
                Arc::new(CoinTransactionProcessor::new(self.db_pool.clone()))
            },
            Processor::DefaultProcessor => {
                Arc::new(DefaultTransactionProcessor::new(self.db_pool.clone()))
            },
            Processor::TokenProcessor => Arc::new(TokenTransactionProcessor::new(
                self.db_pool.clone(),
                self.ans_address.clone(),
                self.nft_points_contract.clone(),
            )),
            Processor::StakeProcessor => {
                Arc::new(StakeTransactionProcessor::new(self.db_pool.clone()))
            },
        };
        let processor_name = processor.name();

        // This is the moving average that we use to calculate TPS
        let mut ma = MovingAverage::new(100_000);
        info!(processor_name = processor_name, "[Parser] Starting stream");
        let mut batch_start_version = starting_version;

        let ending_version = self.ending_version;
        let indexer_grpc_data_service_address = self.indexer_grpc_data_service_address.clone();
        // Create a transaction fetcher thread that will continuously fetch transactions from the GRPC stream
        // and write into a channel
        // The each item will be (chain_id, batch of transactions)
        let (tx, mut receiver) = tokio::sync::mpsc::channel::<(u64, Vec<Transaction>)>(BUFFER_SIZE);
        tokio::spawn(async move {
            info!(
                processor_name = processor_name,
                ending_version = ending_version,
                batch_start_version = batch_start_version,
                "[Parser] Starting fetcher thread"
            );
            // Gets a batch of transactions from the stream. Batch size is set in the grpc server.
            // The number of batches depends on our config
            // There could be several special scenarios:
            // 1. If we lose the connection, we will stop fetching and let the consumer panic.
            // 2. If we specified an end version and we hit that, we will stop fetching.
            while let Some(current_item) = resp_stream.next().await {
                let current_instant = std::time::Instant::now();
                match current_item {
                    Ok(r) => {
                        let start_version = r.transactions.as_slice().first().unwrap().version;
                        let end_version = r.transactions.as_slice().last().unwrap().version;
                        info!(
                            processor_name = processor_name,
                            start_version = start_version,
                            end_version = end_version,
                            time_elapsed_ms = current_instant.elapsed().as_millis(),
                            "[Parser] Received chunk of transactions."
                        );
                        let chain_id = r
                            .chain_id
                            .expect("[Parser] Chain Id doesn't exist.")
                            as u64;
                        match tx.send((chain_id, r.transactions))
                            .await {
                            Ok(()) => {},
                            Err(e) => {
                                error!(
                                    processor_name = processor_name,
                                    stream_address = indexer_grpc_data_service_address,
                                    error = ?e,
                                    "[Parser] Error sending datastream response to channel."
                                );
                                // Note, we don't necessarily want to panic right away because there might be still things in the
                                // channel that we want to process. Let's panic in the consumer side instead, say if there has not
                                // been any data in the channel for a while.
                                break;
                            },
                        }
                    },
                    Err(rpc_error) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = indexer_grpc_data_service_address,
                            error = ?rpc_error,
                            "[Parser] Error receiving datastream response."
                        );
                        // Note, we don't necessarily want to panic right away because there might be still things in the
                        // channel that we want to process. Let's panic in the consumer side instead, say if there has not
                        // been any data in the channel for a while.
                        break;
                    },
                }
            };
            // All senders are dropped here; channel is closed.
            info!("[Parser] The stream is ended; it's a finite stream.")
        });
        // This is the consumer side of the channel. These are the major states:
        // 1. We're backfilling so we should expect many concurrent threads to process transactions
        // 2. We're caught up so we should expect a single thread to process transactions
        // 3. We have received either an empty batch or a batch with a gap. We should panic.
        // 4. We have not received anything in X seconds, we should panic.
        // 5. If it's the wrong chain, panic.
        let mut db_chain_id = None;
        loop {
            let mut transactions_batches = vec![];
            let mut last_fetched_version = batch_start_version - 1;
            for _ in 0..concurrent_tasks {
                match receiver.try_recv() {
                    Ok((chain_id, transactions)) => {
                        if let Some(existing_id) = db_chain_id {
                            if chain_id != existing_id {
                                error!(
                                    processor_name = processor_name,
                                    stream_address = self.indexer_grpc_data_service_address.clone(),
                                    chain_id = chain_id,
                                    existing_id = existing_id,
                                    "[Parser] Stream somehow changed chain id!",
                                );
                                panic!("[Parser] Stream somehow changed chain id!");
                            }
                        } else {
                            db_chain_id = Some(
                                self.check_or_update_chain_id(chain_id as i64)
                                    .await
                                    .unwrap(),
                            );
                        }
                        let current_fetched_version =
                            transactions.as_slice().first().unwrap().version;
                        if last_fetched_version + 1 != current_fetched_version {
                            error!(
                                batch_start_version = batch_start_version,
                                last_fetched_version = last_fetched_version,
                                current_fetched_version = current_fetched_version,
                                "[Parser] Received batch with gap from GRPC stream"
                            );
                            panic!("[Parser] Received batch with gap from GRPC stream");
                        }
                        last_fetched_version = transactions.as_slice().last().unwrap().version;
                        transactions_batches.push(transactions);
                    },
                    // Channel is empty and send is not drpped which we definitely expect. Wait for a bit and continue polling.
                    Err(TryRecvError::Empty) => {
                        break;
                    },
                    // This happens when the channel is closed. We should panic.
                    Err(TryRecvError::Disconnected) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = self.indexer_grpc_data_service_address.clone(),
                            "[Parser] Channel closed; stream ended."
                        );
                        panic!("[Parser] Channel closed");
                    },
                }
            }

            // Process the transactions in parallel
            let mut tasks = vec![];
            if transactions_batches.is_empty() {
                // If we get an empty batch, we want to skip and continue polling.
                info!(
                    batch_start_version = batch_start_version,
                    last_fetched_version = last_fetched_version,
                    sleep_ms = SLEEP_MS,
                    "[Parser] No more data in channel"
                );
                tokio::time::sleep(std::time::Duration::from_millis(SLEEP_MS)).await;
                continue;
            }
            for transactions in transactions_batches {
                let processor_clone = processor.clone();
                let auth_token = self.auth_token.clone();
                let task = tokio::spawn(async move {
                    let start_version = transactions.as_slice().first().unwrap().version;
                    let end_version = transactions.as_slice().last().unwrap().version;
                    let txn_time = transactions.as_slice().first().unwrap().timestamp.clone();
                    if let Some(ref t) = txn_time {
                        PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS
                            .with_label_values(&[auth_token.as_str(), processor_name])
                            .set(time_diff_since_pb_timestamp_in_secs(t));
                    }
                    PROCESSOR_INVOCATIONS_COUNT
                        .with_label_values(&[processor_name])
                        .inc();
                    let processed_result = processor_clone
                        .process_transactions(transactions, start_version, end_version)
                        .await;
                    if let Some(ref t) = txn_time {
                        PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS
                            .with_label_values(&[auth_token.as_str(), processor_name])
                            .set(time_diff_since_pb_timestamp_in_secs(t));
                    }
                    processed_result
                });
                tasks.push(task);
            }

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
                            stream_address = self.indexer_grpc_data_service_address.clone(),
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
            processed_versions.sort();
            let mut prev_start = None;
            let mut prev_end = None;
            let processed_versions_sorted = processed_versions.clone();
            for (start, end) in processed_versions {
                if prev_start.is_none() {
                    prev_start = Some(start);
                    prev_end = Some(end);
                } else {
                    if prev_end.unwrap() + 1 != start {
                        error!(
                            processor_name = processor_name,
                            stream_address = self.indexer_grpc_data_service_address.clone(),
                            processed_versions = processed_versions_sorted
                                .iter()
                                .map(|(s, e)| format!("{}-{}", s, e))
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
            let batch_start = processed_versions_sorted.first().unwrap().0;
            let batch_end = processed_versions_sorted.last().unwrap().1;
            batch_start_version = batch_end + 1;

            LATEST_PROCESSED_VERSION
                .with_label_values(&[processor_name])
                .set(batch_end as i64);
            processor
                .update_last_processed_version(batch_end)
                .await
                .unwrap();

            ma.tick_now(batch_end - batch_start + 1);
            info!(
                processor_name = processor_name,
                start_version = batch_start,
                end_version = batch_end,
                batch_size = batch_end - batch_start + 1,
                tps = (ma.avg() * 1000.0) as u64,
                "[Parser] Processed transactions.",
            );
        }
    }

    fn run_migrations(&self) {
        let _ = &self
            .db_pool
            .get()
            .expect("[Parser] Could not get connection for migrations")
            .run_pending_migrations(MIGRATIONS)
            .expect("[Parser] migrations failed!");
    }

    /// Gets the start version for the processor. If not found, start from 0.
    pub fn get_start_version(&self) -> anyhow::Result<Option<u64>> {
        let mut conn = self.db_pool.get()?;

        match ProcessorStatusQuery::get_by_processor(&self.processor_name, &mut conn)? {
            Some(status) => Ok(Some(status.last_success_version as u64 + 1)),
            None => Ok(None),
        }
    }

    /// Verify the chain id from GRPC against the database.
    pub async fn check_or_update_chain_id(&self, grpc_chain_id: i64) -> anyhow::Result<u64> {
        info!(
            processor_name = self.processor_name.as_str(),
            "[Parser] Checking if chain id is correct"
        );
        let mut conn = self.db_pool.get()?;

        let maybe_existing_chain_id = LedgerInfo::get(&mut conn)?.map(|li| li.chain_id);

        match maybe_existing_chain_id {
            Some(chain_id) => {
                anyhow::ensure!(chain_id == grpc_chain_id, "[Parser] Wrong chain detected! Trying to index chain {} now but existing data is for chain {}", grpc_chain_id, chain_id);
                info!(
                    processor_name = self.processor_name.as_str(),
                    chain_id = chain_id,
                    "[Parser] Chain id matches! Continue to index...",
                );
                Ok(chain_id as u64)
            },
            None => {
                info!(
                    processor_name = self.processor_name.as_str(),
                    chain_id = grpc_chain_id,
                    "[Parser] Adding chain id to db, continue to index.."
                );
                execute_with_better_error(
                    &mut conn,
                    diesel::insert_into(ledger_infos::table).values(LedgerInfo {
                        chain_id: grpc_chain_id,
                    }),
                    None,
                )
                .context(r#"[Parser] Error updating chain_id!"#)
                .map(|_| grpc_chain_id as u64)
            },
        }
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
    request
        .metadata_mut()
        .insert(GRPC_AUTH_TOKEN_HEADER, grpc_auth_token.parse().unwrap());
    request
        .metadata_mut()
        .insert(GRPC_REQUEST_NAME_HEADER, processor_name.parse().unwrap());
    request
}
