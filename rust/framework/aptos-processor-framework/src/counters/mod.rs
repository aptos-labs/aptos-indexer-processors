// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter_vec, register_int_gauge_vec, GaugeVec, IntCounterVec,
    IntGaugeVec,
};

/// Data latency when processor receives transactions.
pub static PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_data_receive_latency_in_secs",
        "Data latency when processor receives transactions",
        &["processor_name"]
    )
    .unwrap()
});

/// Data latency when processor finishes processing transactions.
pub static PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_data_processed_latency_in_secs",
        "Data latency when processor finishes processing transactions",
        &["processor_name"]
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

/// Latest version processed
pub static LATEST_PROCESSED_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_processor_latest_version",
        "Latest version a processor has fully consumed",
        &["processor_name"]
    )
    .unwrap()
});

/// Count of bytes received from the txn stream service
pub static RECEIVED_BYTES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_received_bytes_count",
        "Count of bytes received from the txn stream service",
        &["processor_name"]
    )
    .unwrap()
});
