use crate::{
    config::TransactionStreamConfig,
    utils::{
        counters::{
            ProcessorStep, LATEST_PROCESSED_VERSION, NUM_TRANSACTIONS_PROCESSED_COUNT,
            PROCESSED_BYTES_COUNT, TRANSACTION_UNIX_TIMESTAMP,
        },
        util::{timestamp_to_iso, timestamp_to_unixtime},
    },
};
use anyhow::{anyhow, Result};
use aptos_moving_average::MovingAverage;
use aptos_protos::{
    indexer::v1::{raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse},
    transaction::v1::Transaction,
    util::timestamp::Timestamp,
};
use futures_util::StreamExt;
use prost::Message;
use std::time::Duration;
use tokio::time::timeout;
use tonic::{Response, Streaming};
use tracing::{error, info};

/// GRPC request metadata key for the token ID.
const GRPC_API_GATEWAY_API_KEY_HEADER: &str = "authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
/// GRPC connection id
const GRPC_CONNECTION_ID: &str = "x-aptos-connection-id";
/// We will try to reconnect to GRPC 5 times in case upstream connection is being updated
pub const RECONNECTION_MAX_RETRIES: u64 = 5;
/// 256MB
pub const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 256;

const PROCESSOR_SERVICE_TYPE: &str = "processor";

#[derive(Clone)]
pub struct TransactionsPBResponse {
    pub transactions: Vec<Transaction>,
    pub chain_id: u64,
    // We put start/end versions here as filtering means there are potential "gaps" here now
    pub start_version: u64,
    pub end_version: u64,
    pub start_txn_timestamp: Option<Timestamp>,
    pub end_txn_timestamp: Option<Timestamp>,
    pub size_in_bytes: u64,
}

pub fn grpc_request_builder(
    starting_version: u64,
    transactions_count: Option<u64>,
    grpc_auth_token: String,
    request_name_header: String,
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
    request.metadata_mut().insert(
        GRPC_REQUEST_NAME_HEADER,
        request_name_header.parse().unwrap(),
    );
    request
}

pub async fn get_stream(
    transaction_stream_config: TransactionStreamConfig,
) -> Result<Response<Streaming<TransactionsResponse>>> {
    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        "[Parser] Setting up rpc channel"
    );

    let channel = tonic::transport::Channel::from_shared(
        transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
    )
    .expect(
        "[Parser] Failed to build GRPC channel, perhaps because the data service URL is invalid",
    )
    .http2_keep_alive_interval(transaction_stream_config.indexer_grpc_http2_ping_interval())
    .keep_alive_timeout(transaction_stream_config.indexer_grpc_http2_ping_timeout());

    // If the scheme is https, add a TLS config.
    let channel = if transaction_stream_config
        .indexer_grpc_data_service_address
        .scheme()
        == "https"
    {
        let config = tonic::transport::channel::ClientTlsConfig::new();
        channel
            .tls_config(config)
            .expect("[Parser] Failed to create TLS config")
    } else {
        channel
    };

    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        "[Parser] Setting up GRPC client"
    );

    // TODO: move this to a config file
    // Retry this connection a few times before giving up
    let mut connect_retries = 0;
    let res = loop {
        let res = timeout(
            transaction_stream_config.indexer_grpc_reconnection_timeout(),
            RawDataClient::connect(channel.clone()),
        )
        .await;
        match res {
            Ok(connect_res) => match connect_res {
                Ok(client) => break Ok(client),
                Err(e) => {
                    error!(
                        stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                        start_version = transaction_stream_config.starting_version,
                        end_version = transaction_stream_config.request_ending_version,
                        error = ?e,
                        "[Parser] Error connecting to GRPC client"
                    );
                    connect_retries += 1;
                    if connect_retries >= RECONNECTION_MAX_RETRIES {
                        break Err(anyhow!("Error connecting to GRPC client").context(e));
                    }
                },
            },
            Err(e) => {
                error!(
                    stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retries = connect_retries,
                    error = ?e,
                    "[Parser] Timed out connecting to GRPC client"
                );
                connect_retries += 1;
                if connect_retries >= RECONNECTION_MAX_RETRIES {
                    break Err(anyhow!("Timed out connecting to GRPC client"));
                }
            },
        }
    };

    let raw_data_client = res?;

    let mut rpc_client = raw_data_client
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_RESPONSE_SIZE)
        .max_encoding_message_size(MAX_RESPONSE_SIZE);

    let count = transaction_stream_config
        .request_ending_version
        .map(|v| (v as i64 - transaction_stream_config.starting_version as i64 + 1) as u64);

    info!(
        stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        num_of_transactions = ?count,
        "[Parser] Setting up GRPC stream",
    );

    // TODO: move this to a config file
    // Retry this connection a few times before giving up
    let mut connect_retries = 0;
    loop {
        let timeout_res = timeout(
            transaction_stream_config.indexer_grpc_reconnection_timeout(),
            async {
                let request = grpc_request_builder(
                    transaction_stream_config.starting_version,
                    count,
                    transaction_stream_config.auth_token.clone(),
                    transaction_stream_config.request_name_header.clone(),
                );
                rpc_client.get_transactions(request).await
            },
        )
        .await;
        match timeout_res {
            Ok(response_res) => match response_res {
                Ok(response) => break Ok(response),
                Err(e) => {
                    error!(
                        stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                        start_version = transaction_stream_config.starting_version,
                        end_version = transaction_stream_config.request_ending_version,
                        error = ?e,
                        "[Parser] Error making grpc request. Retrying..."
                    );
                    connect_retries += 1;
                    if connect_retries >= RECONNECTION_MAX_RETRIES {
                        break Err(anyhow!("Error making grpc request").context(e));
                    }
                },
            },
            Err(e) => {
                error!(
                    stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retries = connect_retries,
                    error = ?e,
                    "[Parser] Timeout making grpc request. Retrying...",
                );
                connect_retries += 1;
                if connect_retries >= RECONNECTION_MAX_RETRIES {
                    break Err(anyhow!("Timeout making grpc request").context(e));
                }
            },
        }
    }
}

pub async fn get_chain_id(transaction_stream_config: TransactionStreamConfig) -> Result<u64> {
    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        "[Parser] Connecting to GRPC stream to get chain id",
    );

    let transaction_stream_config_for_chain_id = TransactionStreamConfig {
        starting_version: 1,
        request_ending_version: Some(2),
        ..transaction_stream_config.clone()
    };
    let response = get_stream(transaction_stream_config_for_chain_id).await?;
    let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        connection_id, "[Parser] Successfully connected to GRPC stream to get chain id",
    );

    match resp_stream.next().await {
        Some(Ok(r)) => match r.chain_id {
            Some(chain_id) => Ok(chain_id),
            None => {
                error!(
                    stream_address = transaction_stream_config
                        .indexer_grpc_data_service_address
                        .to_string(),
                    connection_id, "[Parser] Chain Id doesn't exist."
                );
                Err(anyhow!("Chain Id doesn't exist"))
            },
        },
        Some(Err(rpc_error)) => {
            error!(
                stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                connection_id,
                error = ?rpc_error,
                "[Parser] Error receiving datastream response for chain id"
            );
            Err(anyhow!("Error receiving datastream response for chain id").context(rpc_error))
        },
        None => {
            error!(
                stream_address = transaction_stream_config
                    .indexer_grpc_data_service_address
                    .to_string(),
                connection_id, "[Parser] Stream ended before getting response fo for chain id"
            );
            Err(anyhow!("Stream ended before getting response for chain id"))
        },
    }
}

pub struct TransactionStream {
    transaction_stream_config: TransactionStreamConfig,
    processor_name: String,
    stream: Streaming<TransactionsResponse>,
    connection_id: String,
    next_version_to_fetch: u64,
    reconnection_retries: u64,
    last_fetched_version: i64,
    fetch_ma: MovingAverage,
}

impl TransactionStream {
    pub async fn new(
        transaction_stream_config: TransactionStreamConfig,
        processor_name: String,
    ) -> Result<Self> {
        let (stream, connection_id) = Self::init_stream(transaction_stream_config.clone()).await?;
        Ok(Self {
            transaction_stream_config: transaction_stream_config.clone(),
            processor_name,
            stream,
            connection_id,
            next_version_to_fetch: transaction_stream_config.starting_version,
            reconnection_retries: 0,
            last_fetched_version: transaction_stream_config.starting_version as i64 - 1,
            fetch_ma: MovingAverage::new(3000),
        })
    }

    async fn init_stream(
        transaction_stream_config: TransactionStreamConfig,
    ) -> Result<(Streaming<TransactionsResponse>, String)> {
        info!(
            stream_address = transaction_stream_config
                .indexer_grpc_data_service_address
                .to_string(),
            start_version = transaction_stream_config.starting_version,
            end_version = transaction_stream_config.request_ending_version,
            "[Parser] Connecting to GRPC stream",
        );
        let resp_stream = get_stream(transaction_stream_config.clone()).await?;
        let connection_id = match resp_stream.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        info!(
            stream_address = transaction_stream_config
                .indexer_grpc_data_service_address
                .to_string(),
            connection_id = connection_id,
            start_version = transaction_stream_config.starting_version,
            end_version = transaction_stream_config.request_ending_version,
            "[Parser] Successfully connected to GRPC stream",
        );
        Ok((resp_stream.into_inner(), connection_id))
    }

    /// Gets a batch of transactions from the stream. Batch size is set in the grpc server.
    /// The number of batches depends on our config
    /// There could be several special scenarios:
    /// 1. If we lose the connection, we will try reconnecting X times within Y seconds before crashing.
    /// 2. If we specified an end version and we hit that, we will stop fetching, but we will make sure that
    ///    all existing transactions are processed
    ///
    /// Returns
    /// - true if should continue fetching
    /// - false if we reached the end of the stream or there is an error and the loop should stop
    pub async fn get_next_transaction_batch_with_reconnect(
        &mut self,
    ) -> Result<TransactionsPBResponse> {
        let grpc_channel_recv_latency = std::time::Instant::now();

        let txn_pb_res = match tokio::time::timeout(
            self.transaction_stream_config
                .indexer_grpc_response_item_timeout(),
            self.stream.next(),
        )
        .await
        {
            // Received datastream response
            Ok(response) => {
                match response {
                    Some(Ok(r)) => {
                        self.reconnection_retries = 0;
                        let start_version = r.transactions.as_slice().first().unwrap().version;
                        let start_txn_timestamp =
                            r.transactions.as_slice().first().unwrap().timestamp.clone();
                        let end_version = r.transactions.as_slice().last().unwrap().version;
                        let end_txn_timestamp =
                            r.transactions.as_slice().last().unwrap().timestamp.clone();

                        self.next_version_to_fetch = end_version + 1;

                        let size_in_bytes = r.encoded_len() as u64;
                        let chain_id: u64 = r.chain_id.expect("[Parser] Chain Id doesn't exist.");
                        let num_txns = r.transactions.len();
                        let duration_in_secs = grpc_channel_recv_latency.elapsed().as_secs_f64();
                        self.fetch_ma.tick_now(num_txns as u64);

                        // Filter out the txns we don't care about
                        // r.transactions
                        //     .retain(|txn| self.transaction_filter.include(txn));

                        // let num_txn_post_filter = r.transactions.len();
                        // let num_filtered_txns = num_txns - num_txn_post_filter;
                        let step = ProcessorStep::ReceivedTxnsFromGrpc.get_step();
                        let label = ProcessorStep::ReceivedTxnsFromGrpc.get_label();

                        info!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self
                                .transaction_stream_config
                                .indexer_grpc_data_service_address
                                .to_string(),
                            connection_id = self.connection_id,
                            start_version,
                            end_version,
                            start_txn_timestamp_iso = start_txn_timestamp
                                .as_ref()
                                .map(timestamp_to_iso)
                                .unwrap_or_default(),
                            end_txn_timestamp_iso = end_txn_timestamp
                                .as_ref()
                                .map(timestamp_to_iso)
                                .unwrap_or_default(),
                            num_of_transactions = end_version - start_version + 1,
                            // num_filtered_txns,
                            size_in_bytes,
                            duration_in_secs,
                            tps = self.fetch_ma.avg().ceil() as u64,
                            bytes_per_sec = size_in_bytes as f64 / duration_in_secs,
                            step,
                            "{}",
                            label,
                        );

                        if self.last_fetched_version + 1 != start_version as i64 {
                            error!(
                                batch_start_version = self.last_fetched_version + 1,
                                self.last_fetched_version,
                                current_fetched_version = start_version,
                                "[Parser] Received batch with gap from GRPC stream"
                            );
                            panic!("[Parser] Received batch with gap from GRPC stream");
                        }
                        self.last_fetched_version = end_version as i64;

                        LATEST_PROCESSED_VERSION
                            .with_label_values(&[&self.processor_name, step, label, "-"])
                            .set(end_version as i64);
                        TRANSACTION_UNIX_TIMESTAMP
                            .with_label_values(&[&self.processor_name, step, label, "-"])
                            .set(
                                start_txn_timestamp
                                    .as_ref()
                                    .map(timestamp_to_unixtime)
                                    .unwrap_or_default(),
                            );
                        PROCESSED_BYTES_COUNT
                            .with_label_values(&[&self.processor_name, step, label, "-"])
                            .inc_by(size_in_bytes);
                        NUM_TRANSACTIONS_PROCESSED_COUNT
                            .with_label_values(&[&self.processor_name, step, label, "-"])
                            .inc_by(end_version - start_version + 1);

                        let txn_pb = TransactionsPBResponse {
                            transactions: r.transactions,
                            chain_id,
                            start_version,
                            end_version,
                            start_txn_timestamp,
                            end_txn_timestamp,
                            size_in_bytes,
                        };

                        Ok(txn_pb)
                    },
                    // Error receiving datastream response
                    Some(Err(rpc_error)) => {
                        tracing::warn!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self.transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                            self.connection_id,
                            start_version = self.transaction_stream_config.starting_version,
                            end_version = self.transaction_stream_config.request_ending_version,
                            error = ?rpc_error,
                            "[Parser] Error receiving datastream response."
                        );
                        Err(anyhow!("Error receiving datastream response"))
                    },
                    // Stream is finished
                    None => {
                        tracing::warn!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self
                                .transaction_stream_config
                                .indexer_grpc_data_service_address
                                .to_string(),
                            connection_id = self.connection_id,
                            start_version = self.transaction_stream_config.starting_version,
                            end_version = self.transaction_stream_config.request_ending_version,
                            "[Parser] Stream ended."
                        );
                        Err(anyhow!("Stream ended"))
                    },
                }
            },
            // Timeout receiving datastream response
            Err(e) => {
                tracing::warn!(
                    processor_name = self.processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = self.transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                    connection_id = self.connection_id,
                    start_version = self.transaction_stream_config.starting_version,
                    end_version = self.transaction_stream_config.request_ending_version,
                    error = ?e,
                    "[Parser] Timeout receiving datastream response."
                );
                Err(anyhow!("Timeout receiving datastream response"))
            },
        };
        txn_pb_res
    }

    /// Helper function to signal that we've fetched all the transactions up to the ending version that was requested.
    pub fn is_end_of_stream(&self) -> bool {
        if let Some(ending_version) = self.transaction_stream_config.request_ending_version {
            self.next_version_to_fetch > ending_version
        } else {
            false
        }
    }

    pub async fn reconnect_to_grpc(&mut self) -> Result<()> {
        // Sleep for 100ms between reconnect tries
        // TODO: Turn this into exponential backoff
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.reconnection_retries >= RECONNECTION_MAX_RETRIES {
            error!(
                processor_name = self.processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = self
                    .transaction_stream_config
                    .indexer_grpc_data_service_address
                    .to_string(),
                "[Parser] Reconnected more than 100 times. Will not retry.",
            );
            panic!("[Parser] Reconnected more than 100 times. Will not retry.")
        }
        self.reconnection_retries += 1;
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self
                .transaction_stream_config
                .indexer_grpc_data_service_address
                .to_string(),
            starting_version = self.next_version_to_fetch,
            ending_version = self.transaction_stream_config.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Parser] Reconnecting to GRPC stream"
        );
        let response = get_stream(self.transaction_stream_config.clone()).await?;
        let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        self.connection_id = connection_id;
        self.stream = response.into_inner();
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self
                .transaction_stream_config
                .indexer_grpc_data_service_address
                .to_string(),
            connection_id = self.connection_id,
            starting_version = self.next_version_to_fetch,
            ending_version = self.transaction_stream_config.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Parser] Successfully reconnected to GRPC stream"
        );
        Ok(())
    }

    pub async fn get_chain_id(self) -> Result<u64> {
        get_chain_id(self.transaction_stream_config).await
    }
}
