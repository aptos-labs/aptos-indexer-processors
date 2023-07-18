// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::utils::util::time_diff_since_pb_timestamp_in_secs;
use crate::{
    models::{ledger_info::LedgerInfo, processor_status::ProcessorStatusQuery},
    processors::{
        coin_processor::CoinTransactionProcessor,
        default_processor::DefaultTransactionProcessor,
        default_processor_bq::DefaultTransactionProcessorBq,
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
        database::{execute_with_better_error, new_db_pool},
    },
};
use anyhow::Context;
use aptos_moving_average::MovingAverage;
use aptos_protos::indexer::v1::{
    raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse,
};
use diesel::{
    pg::PgConnection,
    r2d2::{ConnectionManager, PooledConnection},
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures::StreamExt;
use std::sync::Arc;
use tracing::{error, info};

pub type PgPool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;
pub type PgDbPool = Arc<PgPool>;
pub type PgPoolConnection = PooledConnection<ConnectionManager<PgConnection>>;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();
const BLOB_STORAGE_SIZE: usize = 1000;
/// GRPC request metadata key for the token ID.
const GRPC_AUTH_TOKEN_HEADER: &str = "x-aptos-data-authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";

pub struct Worker {
    pub db_pool: PgDbPool,
    pub processor_name: String,
    pub postgres_connection_string: String,
    pub indexer_grpc_data_service_addresss: String,
    pub indexer_grpc_http2_ping_interval: std::time::Duration,
    pub indexer_grpc_http2_ping_timeout: std::time::Duration,
    pub auth_token: String,
    pub starting_version: Option<u64>,
    pub number_concurrent_processing_tasks: usize,
    pub ans_address: Option<String>,
    pub bigquery_project_id: Option<String>,
    pub bigquery_dataset_name: Option<String>,
}

impl Worker {
    pub async fn new(
        processor_name: String,
        postgres_connection_string: String,
        indexer_grpc_data_service_addresss: String,
        indexer_grpc_http2_ping_interval: std::time::Duration,
        indexer_grpc_http2_ping_timeout: std::time::Duration,
        auth_token: String,
        starting_version: Option<u64>,
        number_concurrent_processing_tasks: Option<usize>,
        ans_address: Option<String>,
        bigquery_project_id: Option<String>,
        bigquery_dataset_name: Option<String>,
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
            indexer_grpc_data_service_addresss,
            indexer_grpc_http2_ping_interval,
            indexer_grpc_http2_ping_timeout,
            starting_version,
            auth_token,
            number_concurrent_processing_tasks,
            ans_address,
            bigquery_project_id,
            bigquery_dataset_name,
        }
    }

    pub async fn run(&self) {
        let processor_name = self.processor_name.clone();

        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_addresss.clone(),
            "[Parser] Connecting to GRPC endpoint",
        );

        let channel = tonic::transport::Channel::from_shared(format!(
            "http://{}",
            self.indexer_grpc_data_service_addresss.clone()
        ))
        .expect("[indexer grpc] Endpoint is not a valid URI")
        .http2_keep_alive_interval(self.indexer_grpc_http2_ping_interval)
        .keep_alive_timeout(self.indexer_grpc_http2_ping_timeout);

        let mut rpc_client = match RawDataClient::connect(channel).await {
            Ok(client) => client,
            Err(e) => {
                error!(
                    processor_name = processor_name,
                    stream_address = self.indexer_grpc_data_service_addresss.clone(),
                    error = ?e,
                    "[Parser] Error connecting to grpc_stream"
                );
                panic!();
            }
        };
        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_addresss.clone(),
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
            .expect("Database error when getting starting version")
            .unwrap_or_else(|| {
                info!(
                    processor_name = processor_name,
                    "No starting version from db so starting from version 0"
                );
                0
            });

        let starting_version = match self.starting_version {
            None => starting_version_from_db,
            Some(version) => version,
        };

        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_addresss.clone(),
            final_start_version = starting_version,
            start_version_from_config = self.starting_version,
            start_version_from_db = starting_version_from_db,
            "[Parser] Making request to GRPC endpoint",
        );

        let request = grpc_request_builder(
            starting_version,
            self.auth_token.clone(),
            self.processor_name.clone(),
        );

        let mut resp_stream = rpc_client
            .get_transactions(request)
            .await
            .expect("Failed to get grpc response. Is the server running?")
            .into_inner();

        let concurrent_tasks = self.number_concurrent_processing_tasks;
        info!(
            processor_name = processor_name,
            stream_address = self.indexer_grpc_data_service_addresss.clone(),
            starting_version = starting_version,
            concurrent_tasks = concurrent_tasks,
            "[Parser] Successfully connected to GRPC endpoint. Now instantiating processor",
        );

        // Instantiates correct processor based on config
        let processor_enum = Processor::from_string(&processor_name);
        let processor: Arc<dyn ProcessorTrait> = match processor_enum {
            Processor::CoinProcessor => {
                Arc::new(CoinTransactionProcessor::new(self.db_pool.clone()))
            }
            Processor::DefaultProcessor => {
                Arc::new(DefaultTransactionProcessor::new(self.db_pool.clone()))
            }
            Processor::TokenProcessor => Arc::new(TokenTransactionProcessor::new(
                self.db_pool.clone(),
                self.ans_address.clone(),
            )),
            Processor::StakeProcessor => {
                Arc::new(StakeTransactionProcessor::new(self.db_pool.clone()))
            }
            Processor::DefaultProcessorBQ => Arc::new(
                DefaultTransactionProcessorBq::new(
                    self.db_pool.clone(),
                    self.bigquery_project_id.as_ref().unwrap().clone(),
                    self.bigquery_dataset_name.as_ref().unwrap().clone(),
                )
                .await,
            ),
        };
        let processor_name = processor.name();

        let mut ma = MovingAverage::new(10_000);
        info!(processor_name = processor_name, "[Parser] Starting stream");
        match resp_stream.next().await {
            Some(Ok(r)) => {
                self.validate_grpc_chain_id(r)
                    .await
                    .expect("Invalid grpc response with INIT frame.");
            }
            _ => {
                error!(
                    processor_name = processor_name,
                    "[Parser] Error receiving datastream response"
                );
                panic!();
            }
        }
        let mut batch_start_version = starting_version;
        loop {
            let mut transactions_batches = vec![];
            // Gets a batch of transactions from the stream. Batch size is set in the grpc server.
            // The number of batches depends on our config
            for _ in 0..concurrent_tasks {
                let next_stream = match resp_stream.next().await {
                    Some(Ok(r)) => r,
                    _ => {
                        error!(
                            processor_name = processor_name,
                            "[Parser] Error receiving datastream response; reconnecting..."
                        );
                        // If we get an error, we need to reconnect to the stream.
                        let request = grpc_request_builder(
                            batch_start_version,
                            self.auth_token.clone(),
                            self.processor_name.clone(),
                        );
                        resp_stream = rpc_client
                            .get_transactions(request)
                            .await
                            .expect("Failed to get grpc response. Is the server running?")
                            .into_inner();
                        transactions_batches.clear();
                        continue;
                    }
                };
                let transactions = next_stream.transactions;

                let current_batch_size = transactions.len();
                if current_batch_size == 0 {
                    error!(
                        batch_start_version = batch_start_version,
                        "[Indexer Parser] Received empty batch from GRPC stream"
                    );
                    panic!();
                }
                transactions_batches.push(transactions);
                // If it is a partial batch, then skip polling and head to process it first.
                if current_batch_size < BLOB_STORAGE_SIZE {
                    break;
                }
            }

            // Process the transactions in parallel
            let mut tasks = vec![];
            for transactions in transactions_batches {
                let processor_clone = processor.clone();
                let auth_token = self.auth_token.clone();
                let task = tokio::spawn(async move {
                    let start_version = transactions.as_slice().first().unwrap().version;
                    let end_version = transactions.as_slice().last().unwrap().version;
                    let txn_time = transactions.as_slice().first().unwrap().timestamp.clone();
                    info!(
                        request_token = auth_token,
                        processor_name = processor_name,
                        start_version = start_version,
                        end_version = end_version,
                        "[Parser] Received chunk of transactions."
                    );
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
                    }
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = self.indexer_grpc_data_service_addresss.clone(),
                            error = ?e,
                            "[Parser] Error processing transactions"
                        );
                        PROCESSOR_ERRORS_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        panic!();
                    }
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
                            stream_address = self.indexer_grpc_data_service_addresss.clone(),
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
            .expect("Could not get connection for migrations")
            .run_pending_migrations(MIGRATIONS)
            .expect("migrations failed!");
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
            "Checking if chain id is correct"
        );
        let mut conn = self.db_pool.get()?;

        let maybe_existing_chain_id = LedgerInfo::get(&mut conn)?.map(|li| li.chain_id);

        match maybe_existing_chain_id {
            Some(chain_id) => {
                anyhow::ensure!(chain_id == grpc_chain_id, "Wrong chain detected! Trying to index chain {} now but existing data is for chain {}", grpc_chain_id, chain_id);
                info!(
                    processor_name = self.processor_name.as_str(),
                    chain_id = chain_id,
                    "Chain id matches! Continue to index...",
                );
                Ok(chain_id as u64)
            }
            None => {
                info!(
                    processor_name = self.processor_name.as_str(),
                    chain_id = grpc_chain_id,
                    "Adding chain id to db, continue to index.."
                );
                execute_with_better_error(
                    &mut conn,
                    diesel::insert_into(ledger_infos::table).values(LedgerInfo {
                        chain_id: grpc_chain_id,
                    }),
                    None,
                )
                .context(r#"Error updating chain_id!"#)
                .map(|_| grpc_chain_id as u64)
            }
        }
    }

    /// GRPC validation
    pub async fn validate_grpc_chain_id(
        &self,
        response: TransactionsResponse,
    ) -> anyhow::Result<()> {
        let grpc_chain_id = response
            .chain_id
            .ok_or_else(|| anyhow::Error::msg("Chain Id doesn't exist."))?;
        let _chain_id = self.check_or_update_chain_id(grpc_chain_id as i64).await?;
        Ok(())
    }
}

pub fn grpc_request_builder(
    starting_version: u64,
    grpc_auth_token: String,
    processor_name: String,
) -> tonic::Request<GetTransactionsRequest> {
    let mut request = tonic::Request::new(GetTransactionsRequest {
        starting_version: Some(starting_version),
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
