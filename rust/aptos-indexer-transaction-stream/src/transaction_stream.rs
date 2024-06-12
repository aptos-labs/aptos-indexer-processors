use crate::utils::{
    counters::{
        ProcessorStep, FETCHER_THREAD_CHANNEL_SIZE, LATEST_PROCESSED_VERSION,
        NUM_TRANSACTIONS_FILTERED_OUT_COUNT, NUM_TRANSACTIONS_PROCESSED_COUNT,
        PROCESSED_BYTES_COUNT, TRANSACTION_UNIX_TIMESTAMP,
    },
    util::{timestamp_to_iso, timestamp_to_unixtime},
};
use aptos_moving_average::MovingAverage;
use aptos_protos::{
    indexer::v1::{raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse},
    transaction::v1::Transaction,
    util::timestamp::Timestamp,
};
use bigdecimal::Zero;
use futures_util::StreamExt;
use itertools::Itertools;
use kanal::AsyncSender;
use prost::Message;
use std::time::Duration;
use tokio::time::timeout;
use tonic::{Response, Streaming};
use tracing::{debug, error, info};
use transaction_filter::transaction_filter::TransactionFilter;
use url::Url;

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
    indexer_grpc_reconnection_timeout_secs: Duration,
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

    // TODO: move this to a config file
    // Retry this connection a few times before giving up
    let mut connect_retries = 0;
    let connect_res = loop {
        let res = timeout(
            indexer_grpc_reconnection_timeout_secs,
            RawDataClient::connect(channel.clone()),
        )
        .await;
        match res {
            Ok(client) => break Ok(client),
            Err(e) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    start_version = starting_version,
                    end_version = ending_version,
                    retries = connect_retries,
                    error = ?e,
                    "[Parser] Error connecting to GRPC client"
                );
                connect_retries += 1;
                if connect_retries >= RECONNECTION_MAX_RETRIES {
                    break Err(e);
                }
            },
        }
    }
    .expect("[Parser] Timeout connecting to GRPC server");

    let mut rpc_client = match connect_res {
        Ok(client) => client
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .send_compressed(tonic::codec::CompressionEncoding::Zstd)
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

    // TODO: move this to a config file
    // Retry this connection a few times before giving up
    let mut connect_retries = 0;
    let stream_res = loop {
        let timeout_res = timeout(indexer_grpc_reconnection_timeout_secs, async {
            let request = grpc_request_builder(
                starting_version,
                count,
                auth_token.clone(),
                processor_name.clone(),
            );
            rpc_client.get_transactions(request).await
        })
        .await;
        match timeout_res {
            Ok(client) => break Ok(client),
            Err(e) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    start_version = starting_version,
                    end_version = ending_version,
                    retries = connect_retries,
                    error = ?e,
                    "[Parser] Timeout making grpc request. Retrying...",
                );
                connect_retries += 1;
                if connect_retries >= RECONNECTION_MAX_RETRIES {
                    break Err(e);
                }
            },
        }
    }
    .expect("[Parser] Timed out making grpc request after max retries.");

    match stream_res {
        Ok(stream) => stream,
        Err(e) => {
            error!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                start_version = starting_version,
                ending_version = ending_version,
                error = ?e,
                "[Parser] Failed to get grpc response. Is the server running?"
            );
            panic!("[Parser] Failed to get grpc response. Is the server running?");
        },
    }
}

pub async fn get_chain_id(
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    indexer_grpc_reconnection_timeout_secs: Duration,
    auth_token: String,
    processor_name: String,
) -> u64 {
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        "[Parser] Connecting to GRPC stream to get chain id",
    );
    let response = get_stream(
        indexer_grpc_data_service_address.clone(),
        indexer_grpc_http2_ping_interval,
        indexer_grpc_http2_ping_timeout,
        indexer_grpc_reconnection_timeout_secs,
        1,
        Some(2),
        auth_token.clone(),
        processor_name.to_string(),
    )
    .await;
    let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = indexer_grpc_data_service_address.to_string(),
        connection_id,
        "[Parser] Successfully connected to GRPC stream to get chain id",
    );

    match resp_stream.next().await {
        Some(Ok(r)) => r.chain_id.expect("[Parser] Chain Id doesn't exist."),
        Some(Err(rpc_error)) => {
            error!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                error = ?rpc_error,
                "[Parser] Error receiving datastream response for chain id"
            );
            panic!("[Parser] Error receiving datastream response for chain id");
        },
        None => {
            error!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                "[Parser] Stream ended before getting response fo for chain id"
            );
            panic!("[Parser] Stream ended before getting response fo for chain id");
        },
    }
}

pub struct TransactionStream {
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    indexer_grpc_reconnection_timeout_secs: Duration,
    indexer_grpc_response_item_timeout_secs: Duration,
    starting_version: u64,
    request_ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
    transaction_filter: TransactionFilter,
    pb_channel_txn_chunk_size: usize,
    resp_stream: Option<Streaming<TransactionsResponse>>,
    connection_id: Option<String>,
    next_version_to_fetch: u64,
    reconnection_retries: u64,
    last_fetched_version: i64,
    fetch_ma: MovingAverage,
}

pub struct TransactionStreamOutput {
    pub transactions: Vec<TransactionsPBResponse>,
    pub should_continue_fetching: bool,
}

impl TransactionStream {
    pub fn new(
        indexer_grpc_data_service_address: Url,
        indexer_grpc_http2_ping_interval: Duration,
        indexer_grpc_http2_ping_timeout: Duration,
        indexer_grpc_reconnection_timeout_secs: Duration,
        indexer_grpc_response_item_timeout_secs: Duration,
        starting_version: u64,
        request_ending_version: Option<u64>,
        auth_token: String,
        processor_name: String,
        transaction_filter: TransactionFilter,
        pb_channel_txn_chunk_size: usize,
    ) -> Self {
        Self {
            indexer_grpc_data_service_address,
            indexer_grpc_http2_ping_interval,
            indexer_grpc_http2_ping_timeout,
            indexer_grpc_reconnection_timeout_secs,
            indexer_grpc_response_item_timeout_secs,
            starting_version,
            request_ending_version,
            auth_token,
            processor_name,
            transaction_filter,
            pb_channel_txn_chunk_size,
            resp_stream: None,
            connection_id: None,
            next_version_to_fetch: starting_version,
            reconnection_retries: 0,
            last_fetched_version: starting_version as i64 - 1,
            fetch_ma: MovingAverage::new(3000),
        }
    }

    async fn init_stream(&mut self) {
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.to_string(),
            start_version = self.starting_version,
            end_version = self.request_ending_version,
            "[Parser] Connecting to GRPC stream",
        );
        let response = get_stream(
            self.indexer_grpc_data_service_address.clone(),
            self.indexer_grpc_http2_ping_interval,
            self.indexer_grpc_http2_ping_timeout,
            self.indexer_grpc_reconnection_timeout_secs,
            self.starting_version,
            self.request_ending_version,
            self.auth_token.clone(),
            self.processor_name.to_string(),
        )
        .await;
        let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        self.connection_id = Some(connection_id);
        self.resp_stream = Some(response.into_inner());
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.to_string(),
            connection_id = self.connection_id,
            start_version = self.starting_version,
            end_version = self.request_ending_version,
            "[Parser] Successfully connected to GRPC stream",
        );
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
    pub async fn get_next_transaction_batch(&mut self) -> TransactionStreamOutput {
        let grpc_channel_recv_latency = std::time::Instant::now();
        let mut transaction_pb_response = vec![];

        let is_success = match tokio::time::timeout(
            self.indexer_grpc_response_item_timeout_secs,
            self.resp_stream
                .as_mut()
                .expect("[Parser] GRPC stream is not initialized. Did you call init_stream?")
                .next(),
        )
        .await
        {
            // Received datastream response
            Ok(response) => {
                match response {
                    Some(Ok(mut r)) => {
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
                        r.transactions
                            .retain(|txn| self.transaction_filter.include(txn));

                        let num_txn_post_filter = r.transactions.len();
                        let num_filtered_txns = num_txns - num_txn_post_filter;
                        let step = ProcessorStep::ReceivedTxnsFromGrpc.get_step();
                        let label = ProcessorStep::ReceivedTxnsFromGrpc.get_label();

                        info!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self.indexer_grpc_data_service_address.to_string(),
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
                            num_filtered_txns,
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

                        //potentially break txn_pb into many `TransactionsPBResponse` that are each `pb_channel_txn_chunk_size` txns max in size
                        if num_txn_post_filter < self.pb_channel_txn_chunk_size {
                            // We only need to send one; avoid the chunk/clone
                            let txn_pb = TransactionsPBResponse {
                                transactions: r.transactions,
                                chain_id,
                                start_version,
                                end_version,
                                start_txn_timestamp,
                                end_txn_timestamp,
                                size_in_bytes,
                            };
                            transaction_pb_response.push(txn_pb);
                        } else {
                            // We are breaking down a big batch into small batches; this involves an iterator
                            let average_size_in_bytes = size_in_bytes / num_txns as u64;

                            let pb_txn_chunks: Vec<Vec<Transaction>> = r
                                .transactions
                                .into_iter()
                                .chunks(self.pb_channel_txn_chunk_size)
                                .into_iter()
                                .map(|chunk| chunk.collect())
                                .collect();
                            for txns in pb_txn_chunks {
                                let size_in_bytes = average_size_in_bytes * txns.len() as u64;
                                let txn_pb = TransactionsPBResponse {
                                    transactions: txns,
                                    chain_id,
                                    start_version,
                                    end_version,
                                    // TODO: this is only for gap checker + filtered txns, but this is wrong
                                    start_txn_timestamp: start_txn_timestamp.clone(),
                                    end_txn_timestamp: end_txn_timestamp.clone(),
                                    size_in_bytes,
                                };
                                transaction_pb_response.push(txn_pb);
                            }
                        }

                        NUM_TRANSACTIONS_FILTERED_OUT_COUNT
                            .with_label_values(&[&self.processor_name])
                            .inc_by(num_filtered_txns as u64);
                        true
                    },
                    // Error receiving datastream response
                    Some(Err(rpc_error)) => {
                        tracing::warn!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self.indexer_grpc_data_service_address.to_string(),
                            self.connection_id,
                            start_version = self.starting_version,
                            end_version = self.request_ending_version,
                            error = ?rpc_error,
                            "[Parser] Error receiving datastream response."
                        );
                        false
                    },
                    // Stream is finished
                    None => {
                        tracing::warn!(
                            processor_name = self.processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            stream_address = self.indexer_grpc_data_service_address.to_string(),
                            connection_id = self.connection_id,
                            start_version = self.starting_version,
                            end_version = self.request_ending_version,
                            "[Parser] Stream ended."
                        );
                        false
                    },
                }
            },
            // Timeout receiving datastream response
            Err(e) => {
                tracing::warn!(
                    processor_name = self.processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = self.indexer_grpc_data_service_address.to_string(),
                    connection_id = self.connection_id,
                    start_version = self.starting_version,
                    end_version = self.request_ending_version,
                    error = ?e,
                    "[Parser] Timeout receiving datastream response."
                );
                false
            },
        };
        // Check if we're at the end of the stream
        let is_end = if let Some(ending_version) = self.request_ending_version {
            self.next_version_to_fetch > ending_version
        } else {
            false
        };

        if is_end {
            info!(
                processor_name = self.processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = self.indexer_grpc_data_service_address.to_string(),
                connection_id = self.connection_id,
                ending_version = self.request_ending_version,
                next_version_to_fetch = self.next_version_to_fetch,
                "[Parser] Reached ending version.",
            );

            TransactionStreamOutput {
                transactions: transaction_pb_response,
                should_continue_fetching: false,
            }
        } else {
            // The rest is to see if we need to reconnect
            if !is_success {
                self.reconnect_to_grpc().await;
            }

            TransactionStreamOutput {
                transactions: transaction_pb_response,
                should_continue_fetching: true,
            }
        }
    }

    async fn reconnect_to_grpc(&mut self) {
        // Sleep for 100ms between reconnect tries
        // TODO: Turn this into exponential backoff
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.reconnection_retries >= RECONNECTION_MAX_RETRIES {
            error!(
                processor_name = self.processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = self.indexer_grpc_data_service_address.to_string(),
                "[Parser] Reconnected more than 100 times. Will not retry.",
            );
            panic!("[Parser] Reconnected more than 100 times. Will not retry.")
        }
        self.reconnection_retries += 1;
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.to_string(),
            starting_version = self.next_version_to_fetch,
            ending_version = self.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Parser] Reconnecting to GRPC stream"
        );
        let response = get_stream(
            self.indexer_grpc_data_service_address.clone(),
            self.indexer_grpc_http2_ping_interval,
            self.indexer_grpc_http2_ping_timeout,
            self.indexer_grpc_reconnection_timeout_secs,
            self.next_version_to_fetch,
            self.request_ending_version,
            self.auth_token.clone(),
            self.processor_name.to_string(),
        )
        .await;
        let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        self.connection_id = Some(connection_id);
        self.resp_stream = Some(response.into_inner());
        info!(
            processor_name = self.processor_name,
            service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = self.indexer_grpc_data_service_address.to_string(),
            connection_id = self.connection_id,
            starting_version = self.next_version_to_fetch,
            ending_version = self.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Parser] Successfully reconnected to GRPC stream"
        );
    }

    pub async fn get_chain_id(self) -> u64 {
        get_chain_id(
            self.indexer_grpc_data_service_address,
            self.indexer_grpc_http2_ping_interval,
            self.indexer_grpc_http2_ping_timeout,
            self.indexer_grpc_reconnection_timeout_secs,
            self.auth_token,
            self.processor_name,
        )
        .await
    }
}

pub async fn create_fetcher_loop(
    txn_sender: AsyncSender<TransactionsPBResponse>,
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    indexer_grpc_reconnection_timeout_secs: Duration,
    indexer_grpc_response_item_timeout_secs: Duration,
    starting_version: u64,
    request_ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
    transaction_filter: TransactionFilter,
    // The number of transactions per protobuf batch
    pb_channel_txn_chunk_size: usize,
) {
    let mut transaction_stream = TransactionStream::new(
        indexer_grpc_data_service_address.clone(),
        indexer_grpc_http2_ping_interval,
        indexer_grpc_http2_ping_timeout,
        indexer_grpc_reconnection_timeout_secs,
        indexer_grpc_response_item_timeout_secs,
        starting_version,
        request_ending_version,
        auth_token,
        processor_name.clone(),
        transaction_filter,
        pb_channel_txn_chunk_size,
    );

    transaction_stream.init_stream().await;
    let mut send_ma = MovingAverage::new(3000);

    loop {
        let transactions_output = transaction_stream.get_next_transaction_batch().await;
        let start_version = transactions_output
            .transactions
            .first()
            .unwrap()
            .transactions
            .first()
            .unwrap()
            .version;
        let end_version = transactions_output
            .transactions
            .last()
            .unwrap()
            .transactions
            .last()
            .unwrap()
            .version;
        let size_in_bytes = transactions_output
            .transactions
            .iter()
            .map(|txn_pb| txn_pb.size_in_bytes)
            .sum::<u64>();

        let txn_channel_send_latency = std::time::Instant::now();
        for txn_pb in transactions_output.transactions {
            match txn_sender.send(txn_pb).await {
                Ok(()) => {},
                Err(e) => {
                    error!(
                        processor_name = processor_name,
                        stream_address = indexer_grpc_data_service_address.to_string(),
                        error = ?e,
                        "[Parser] Error sending GRPC response to channel."
                    );
                    panic!("[Parser] Error sending GRPC response to channel.")
                },
            }
        }

        // Log channel send metrics
        let duration_in_secs = txn_channel_send_latency.elapsed().as_secs_f64();
        let num_txns = end_version - start_version + 1;

        send_ma.tick_now(num_txns as u64);
        let tps = send_ma.avg().ceil() as u64;
        let bytes_per_sec = size_in_bytes as f64 / duration_in_secs;
        let channel_size = txn_sender.len();
        debug!(
            processor_name.service_type = PROCESSOR_SERVICE_TYPE,
            stream_address = indexer_grpc_data_service_address.to_string(),
            start_version,
            end_version,
            channel_size,
            size_in_bytes,
            duration_in_secs,
            bytes_per_sec,
            tps,
            "[Parser] Successfully sent transactions to channel."
        );
        FETCHER_THREAD_CHANNEL_SIZE
            .with_label_values(&[&processor_name])
            .set(channel_size as i64);

        // Handle reaching end of stream
        if !transactions_output.should_continue_fetching {
            // Wait for the fetched transactions to finish processing before closing the channel
            loop {
                let channel_size = txn_sender.len();
                info!(
                    processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    channel_size,
                    "[Parser] Waiting for channel to be empty"
                );
                if channel_size.is_zero() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(
                processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = indexer_grpc_data_service_address.to_string(),
                "[Parser] Transaction fetcher send channel is closed."
            );
            break;
        }
    }
}
