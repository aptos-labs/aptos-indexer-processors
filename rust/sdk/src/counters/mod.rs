// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter, register_int_counter_vec, register_int_gauge_vec,
    GaugeVec, IntCounter, IntCounterVec, IntGaugeVec,
};

pub enum ProcessorStep {
    ReceivedTxnsFromGrpc, // Received transactions from GRPC. Sending transactions to channel.
    ProcessedBatch,       // Processor finished processing one batch of transaction
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
        &["processor_name", "step", "message"]
    )
    .unwrap()
});

/// Count of bytes processed.
pub static PROCESSED_BYTES_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_processed_bytes_count",
        "Count of bytes processed",
        &["processor_name", "step", "message"]
    )
    .unwrap()
});

/// Count of transactions processed.
pub static NUM_TRANSACTIONS_PROCESSED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processor_num_transactions_processed_count",
        "Number of transactions processed",
        &["processor_name", "step", "message"]
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

/// Transaction timestamp in unixtime
pub static TRANSACTION_UNIX_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processor_transaction_unix_timestamp",
        "Transaction timestamp in unixtime",
        &["processor_name", "step", "message"]
    )
    .unwrap()
});
