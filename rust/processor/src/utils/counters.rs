// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge, register_gauge_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Gauge, GaugeVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec,
};

pub enum ProcessorStep {
    ReceivedTxnsFromGrpc,
    // Received transactions from GRPC. Sending transactions to channel.
    ProcessedBatch,
    // Processor finished processing one batch of transaction
    ProcessedMultipleBatches, // Processor finished processing multiple batches of transactions
}

impl ProcessorStep {
    pub fn get_step(&self) -> &'static str {
        match self {
            ProcessorStep::ReceivedTxnsFromGrpc => "1",
            ProcessorStep::ProcessedBatch => "2",
            ProcessorStep::ProcessedMultipleBatches => "3",
        }
    }

    pub fn get_label(&self) -> &'static str {
        match self {
            ProcessorStep::ReceivedTxnsFromGrpc => {
                "[Parser] Received transactions from GRPC. Sending transactions to channel."
            },
            ProcessorStep::ProcessedBatch => {
                "[Parser] Processor finished processing one batch of transaction"
            },
            ProcessorStep::ProcessedMultipleBatches => {
                "[Parser] Processor finished processing multiple batches of transactions"
            },
        }
    }
}

/// Data latency when processor receives transactions.
pub static PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_data_receive_latency_in_secs",
        "Data latency when processor receives transactions",
        &["request_token", "processor_name"]
    )
    .unwrap()
});

/// Data latency when processor finishes processing transactions.
pub static PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_data_processed_latency_in_secs",
        "Data latency when processor finishes processing transactions",
        &["request_token", "processor_name"]
    )
    .unwrap()
});

/// Number of times a given processor has been invoked
pub static PROCESSOR_INVOCATIONS_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_invocation_count",
        "Number of times a given processor has been invoked",
        &["processor_name"]
    )
    .unwrap()
});

/// Number of times any given processor has raised an error
pub static PROCESSOR_ERRORS_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_errors",
        "Number of times any given processor has raised an error",
        &["processor_name"]
    )
    .unwrap()
});

/// Number of times any given processor has completed successfully
pub static PROCESSOR_SUCCESSES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_success_count",
        "Number of times a given processor has completed successfully",
        &["processor_name"]
    )
    .unwrap()
});

/// Number of times the connection pool has timed out when trying to get a connection
pub static UNABLE_TO_GET_CONNECTION_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_connection_pool_err",
        "Number of times the connection pool has timed out when trying to get a connection"
    )
    .unwrap()
});

/// Number of times the connection pool got a connection
pub static GOT_CONNECTION_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_connection_pool_ok",
        "Number of times the connection pool got a connection"
    )
    .unwrap()
});

#[allow(dead_code)]
/// Number of times the indexer has been unable to fetch a transaction. Ideally zero.
pub static UNABLE_TO_FETCH_TRANSACTION: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_unable_to_fetch_transaction_count",
        "Number of times the indexer has been unable to fetch a transaction"
    )
    .unwrap()
});

#[allow(dead_code)]
/// Number of times the indexer has been able to fetch a transaction
pub static FETCHED_TRANSACTION: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_fetched_transaction_count",
        "Number of times the indexer has been able to fetch a transaction"
    )
    .unwrap()
});

/// Max version processed
pub static LATEST_PROCESSED_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_processor_latest_version",
        "Latest version a processor has fully consumed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Count of bytes processed.
pub static PROCESSED_BYTES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_processed_bytes_count",
        "Count of bytes processed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// The amount of time that a task spent waiting for a protobuf bundle of transactions
pub static PB_CHANNEL_FETCH_WAIT_TIME_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_pb_channel_fetch_wait_time_secs",
        "Count of bytes processed",
        &["processor_name", "task_index"]
    )
    .unwrap()
});

/// Count of transactions processed.
pub static NUM_TRANSACTIONS_PROCESSED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_num_transactions_processed_count",
        "Number of transactions processed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Count of transactions filtered out
pub static NUM_TRANSACTIONS_FILTERED_OUT_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_num_transactions_filtered_out_count",
        "Number of transactions filtered out",
        &["processor_name"]
    )
    .unwrap()
});

/// Size of the channel containing transactions fetched from GRPC, waiting to be processed
pub static FETCHER_THREAD_CHANNEL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_processor_fetcher_thread_channel_size",
        "Size of the fetcher thread channel",
        &["processor_name"]
    )
    .unwrap()
});

/// Overall processing time for a single batch of transactions (per task)
pub static SINGLE_BATCH_PROCESSING_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_processing_time_in_secs",
        "Time taken to process a single batch of transactions",
        &["processor_name", "task_index"]
    )
    .unwrap()
});

/// Parsing time for a single batch of transactions
pub static SINGLE_BATCH_PARSING_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_parsing_time_in_secs",
        "Time taken to parse a single batch of transactions",
        &["processor_name", "task_index"]
    )
    .unwrap()
});

/// DB insertion time for a single batch of transactions
pub static SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_db_insertion_time_in_secs",
        "Time taken to insert to DB for a single batch of transactions",
        &["processor_name", "task_index"]
    )
    .unwrap()
});

/// Transaction timestamp in unixtime
pub static TRANSACTION_UNIX_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_transaction_unix_timestamp",
        "Transaction timestamp in unixtime",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Data gap warnings
pub static PROCESSOR_DATA_GAP_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!("indexer_processor_data_gap_count", "Data gap count", &[
        "processor_name"
    ])
    .unwrap()
});

/// GRPC latency.
pub static GRPC_LATENCY_BY_PROCESSOR_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_grpc_latency_in_secs",
        "GRPC latency observed by processor",
        &["processor_name", "task_index"]
    )
    .unwrap()
});

/// Processor unknown type count.
pub static PROCESSOR_UNKNOWN_TYPE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_unknown_type_count",
        "Processor unknown type count, e.g., comptaibility issues",
        &["model_name"]
    )
    .unwrap()
});

/// Indexer gRPC to Processor 1 serve latency
pub static GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "indexer_grpc_to_processor_1_serve_latency_in_secs",
        "Indexer gRPC to Processor 1 serve latency"
    )
    .unwrap()
});

/// Last value in cache
pub static LAST_TRANSACTION_VERSION_IN_CACHE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "indexer_last_transaction_version_in_cache",
        "Last value in cache"
    )
    .unwrap()
});
