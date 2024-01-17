// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter, register_int_counter_vec, GaugeVec, IntCounter,
    IntCounterVec,
};

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

/// Overall processing time for multiple (n = number_concurrent_processing_tasks) batch of transactions
pub static MULTI_BATCH_PROCESSING_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_multi_batch_processing_time_in_secs",
        "Time taken to process multiple batches of transactions",
        &["processor_name"]
    )
    .unwrap()
});

/// Overall processing time for a single batch of transactions
pub static SINGLE_BATCH_PROCESSING_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_processing_time_in_secs",
        "Time taken to process a single batch of transactions",
        &["processor_name"]
    )
    .unwrap()
});

/// Parsing time for a single batch of transactions
pub static SINGLE_BATCH_PARSING_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_parsing_time_in_secs",
        "Time taken to parse a single batch of transactions",
        &["processor_name"]
    )
    .unwrap()
});

/// DB insertion time for a single batch of transactions
pub static SINGLE_BATCH_DB_INSERTION_TIME_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_single_batch_db_insertion_time_in_secs",
        "Time taken to insert to DB for a single batch of transactions",
        &["processor_name"]
    )
    .unwrap()
});
