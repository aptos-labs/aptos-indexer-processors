// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter, register_int_counter_vec, register_int_gauge_vec,
    GaugeVec, IntCounter, IntCounterVec, IntGaugeVec,
};

pub enum ProcessorStep {
    TryingToFetchTxnsFromGrpc, // Trying to fetch transaction from GRPC.
    ReceivedTxnsFromGrpc,      // Received transactions from GRPC. Sending transactions to channel.
    ProcessedBatch,            // Processor finished processing one batch of transaction
    ProcessedMultipleBatches,  // Processor finished processing multiple batches of transactions
}

impl ProcessorStep {
    pub fn get_step(&self) -> &'static str {
        match self {
            ProcessorStep::TryingToFetchTxnsFromGrpc => "0",
            ProcessorStep::ReceivedTxnsFromGrpc => "1",
            ProcessorStep::ProcessedBatch => "2",
            ProcessorStep::ProcessedMultipleBatches => "3",
        }
    }

    pub fn get_label(&self) -> &'static str {
        match self {
            ProcessorStep::TryingToFetchTxnsFromGrpc => {
                "[Parser] Trying to fetch transactions from GRPC."
            },
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

/// Number of times the indexer has been unable to fetch a transaction. Ideally zero.
pub static UNABLE_TO_FETCH_TRANSACTION: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_unable_to_fetch_transaction_count",
        "Number of times the indexer has been unable to fetch a transaction"
    )
    .unwrap()
});

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
