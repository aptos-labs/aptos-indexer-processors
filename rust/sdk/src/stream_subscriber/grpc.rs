// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ChannelHandle, StartError, StreamSubscriberTrait, TransactionsPBResponse};
use crate::{
    counters::{
        ProcessorStep, FETCHER_THREAD_CHANNEL_SIZE, LATEST_PROCESSED_VERSION,
        NUM_TRANSACTIONS_PROCESSED_COUNT, PROCESSED_BYTES_COUNT, TRANSACTION_UNIX_TIMESTAMP,
    },
    utils::{timestamp_to_iso, timestamp_to_unixtime},
};
use aptos_protos::indexer::v1::{
    raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse,
};
use futures::StreamExt;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::time::Duration;
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

/// This is how large the fetch queue should be. Each bucket should have a max of 80MB
/// or so, so a batch of 50 means that we could potentially have at least 4.8GB of data
/// in memory at any given time and that we should provision machines accordingly.
const BUFFER_SIZE: usize = 50;

/// 20MB
const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 20;

/// We will try to reconnect to GRPC once every X seconds if we get disconnected, before crashing
/// We define short connection issue as < 10 seconds so adding a bit of a buffer here
const MIN_SEC_BETWEEN_GRPC_RECONNECTS: u64 = 15;

/// We will try to reconnect to GRPC 5 times in case upstream connection is being updated
const RECONNECTION_MAX_RETRIES: u64 = 5;

const PROCESSOR_SERVICE_TYPE: &str = "processor";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GrpcStreamSubscriberConfig {
    /// The address of the Transaction Stream Service.
    pub grpc_data_service_address: Url,

    #[serde(flatten)]
    pub grpc_http2_config: IndexerGrpcHttp2Config,

    /// The token to use for authentication with the txn stream service GRPC endpoint.
    pub auth_token: String,

    /// If set, the processor will stop processing at this version.
    pub ending_version: Option<u64>,
}

#[derive(Debug)]
pub struct GrpcStreamSubscriber {
    pub config: GrpcStreamSubscriberConfig,
    pub processor_name: String,
    pub starting_version: u64,
}

pub async fn get_stream(
    grpc_data_service_address: Url,
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
        stream_address = grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        "[Parser] Setting up rpc channel"
    );

    let channel = tonic::transport::Channel::from_shared(
        grpc_data_service_address.to_string(),
    )
    .expect(
        "[Parser] Failed to build GRPC channel, perhaps because the data service URL is invalid",
    )
    .http2_keep_alive_interval(indexer_grpc_http2_ping_interval)
    .keep_alive_timeout(indexer_grpc_http2_ping_timeout);

    // If the scheme is https, add a TLS config.
    let channel = if grpc_data_service_address.scheme() == "https" {
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
        stream_address = grpc_data_service_address.to_string(),
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
                stream_address = grpc_data_service_address.to_string(),
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
        stream_address = grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        num_of_transactions = ?count,
        "[Parser] Setting up GRPC stream",
    );
    let request = grpc_request_builder(starting_version, count, auth_token, &processor_name);
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
    grpc_data_service_address: Url,
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
    let mut last_reconnection_time: Option<std::time::Instant> = None;
    let mut reconnection_retries = 0;
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        stream_address = grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = request_ending_version,
        "[Parser] Connecting to GRPC stream",
    );
    let mut response = get_stream(
        grpc_data_service_address.clone(),
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
        stream_address = grpc_data_service_address.to_string(),
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
                    stream_address = grpc_data_service_address.to_string(),
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
                            stream_address = grpc_data_service_address.to_string(),
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
                    stream_address = grpc_data_service_address.to_string(),
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
                    stream_address = grpc_data_service_address.to_string(),
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
                    stream_address = grpc_data_service_address.to_string(),
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
                stream_address = grpc_data_service_address.to_string(),
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
                    stream_address = grpc_data_service_address.to_string(),
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
                stream_address = grpc_data_service_address.to_string(),
                connection_id,
                "[Parser] The stream is ended."
            );
            break;
        } else {
            // The rest is to see if we need to reconnect
            if is_success {
                continue;
            }
            if let Some(lrt) = last_reconnection_time {
                let elapsed: u64 = lrt.elapsed().as_secs();
                if reconnection_retries >= RECONNECTION_MAX_RETRIES
                    && elapsed < MIN_SEC_BETWEEN_GRPC_RECONNECTS
                {
                    error!(
                        processor_name = processor_name,
                        service_type = PROCESSOR_SERVICE_TYPE,
                        stream_address = grpc_data_service_address.to_string(),
                        seconds_since_last_retry = elapsed,
                        "[Parser] Recently reconnected. Will not retry.",
                    );
                    panic!("[Parser] Recently reconnected. Will not retry.")
                }
            }
            reconnection_retries += 1;
            last_reconnection_time = Some(std::time::Instant::now());
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                stream_address = grpc_data_service_address.to_string(),
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Reconnecting to GRPC stream"
            );
            response = get_stream(
                grpc_data_service_address.clone(),
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
                stream_address = grpc_data_service_address.to_string(),
                connection_id,
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Successfully reconnected to GRPC stream"
            );
        }
    }
}

pub fn grpc_request_builder(
    starting_version: u64,
    transactions_count: Option<u64>,
    grpc_auth_token: String,
    processor_name: &str,
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

/// TODO document how the stream works when it encounters errors, what it returns, etc.
#[async_trait::async_trait]
impl StreamSubscriberTrait for GrpcStreamSubscriber {
    fn start(&self) -> Result<ChannelHandle, StartError> {
        let grpc_data_service_address = self.config.grpc_data_service_address.clone();
        let auth_token = self.config.auth_token.clone();

        let processor_name = self.processor_name.clone();

        let starting_version = self.starting_version;
        let ending_version = self.config.ending_version;

        let indexer_grpc_http2_ping_interval = self
            .config
            .grpc_http2_config
            .grpc_http2_ping_interval_in_secs();
        let indexer_grpc_http2_ping_timeout = self
            .config
            .grpc_http2_config
            .grpc_http2_ping_timeout_in_secs();

        let (tx, receiver) = tokio::sync::mpsc::channel::<TransactionsPBResponse>(BUFFER_SIZE);

        let join_handle = tokio::spawn(async move {
            info!(
                processor_name = processor_name,
                service_type = PROCESSOR_SERVICE_TYPE,
                start_version = starting_version,
                end_version = ending_version,
                "[Parser] Starting fetcher thread"
            );
            create_fetcher_loop(
                tx,
                grpc_data_service_address,
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                starting_version,
                ending_version,
                auth_token,
                processor_name.to_string(),
                starting_version,
            )
            .await
        });

        Ok(ChannelHandle {
            join_handle,
            receiver,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct IndexerGrpcHttp2Config {
    /// Indexer GRPC http2 ping interval in seconds.
    /// Tonic ref: https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html#method.http2_keep_alive_interval
    indexer_grpc_http2_ping_interval_in_secs: u64,

    /// Indexer GRPC http2 ping timeout in seconds.
    indexer_grpc_http2_ping_timeout_in_secs: u64,
}

impl IndexerGrpcHttp2Config {
    pub fn grpc_http2_ping_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_interval_in_secs)
    }

    pub fn grpc_http2_ping_timeout_in_secs(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_timeout_in_secs)
    }
}

impl Default for IndexerGrpcHttp2Config {
    fn default() -> Self {
        Self {
            indexer_grpc_http2_ping_interval_in_secs: 30,
            indexer_grpc_http2_ping_timeout_in_secs: 10,
        }
    }
}
