// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter_vec, register_int_gauge_vec, GaugeVec, IntCounterVec,
    IntGaugeVec,
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

pub const TRANSACTION_STREAM_METRICS_PREFIX: &str = "transaction_stream_";

/// These metrics are temporary (suffixed with _temp to avoid conflict with metrics in processor crate)
/// They're only defined in this crate for backwards compatibility before we migrate over to
/// using the instrumentation provided by SDK

/// Max version processed
pub static LATEST_PROCESSED_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        format!("{}_latest_version", TRANSACTION_STREAM_METRICS_PREFIX),
        "Latest version a processor has fully consumed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Count of bytes processed.
pub static PROCESSED_BYTES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        format!(
            "{}_processed_bytes_count",
            TRANSACTION_STREAM_METRICS_PREFIX
        ),
        "Count of bytes processed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Count of transactions processed.
pub static NUM_TRANSACTIONS_PROCESSED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        format!(
            "{}_num_transactions_processed_count",
            TRANSACTION_STREAM_METRICS_PREFIX
        ),
        "Number of transactions processed",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});

/// Transaction timestamp in unixtime
pub static TRANSACTION_UNIX_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        format!(
            "{}_transaction_unix_timestamp",
            TRANSACTION_STREAM_METRICS_PREFIX
        ),
        "Transaction timestamp in unixtime",
        &["processor_name", "step", "message", "task_index"]
    )
    .unwrap()
});
