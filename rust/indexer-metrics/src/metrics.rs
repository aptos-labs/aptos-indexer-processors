// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge_vec, register_int_counter_vec, register_int_gauge_vec, GaugeVec, IntCounterVec,
    IntGaugeVec,
};

/// Task failure count.
pub static TASK_FAILURE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_metrics_task_failure_count",
        "Task failure count from indexer metrics service",
        &["task_name", "chain_name"],
    )
    .unwrap()
});

pub static HASURA_API_LATEST_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_metrics_hasura_latest_version",
        "Processor latest version measured from indexer metrics service",
        &["processor_name", "chain_name"],
    )
    .unwrap()
});

pub static HASURA_API_LATEST_VERSION_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_metrics_hasura_latest_version_timestamp_secs",
        "Processor latest timestamp (unix timestamp) measured from indexer metrics service",
        &["processor_name", "chain_name"],
    )
    .unwrap()
});

pub static HASURA_API_LATEST_TRANSACTION_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_metrics_hasura_latest_transaction_timestamp_secs",
        "Latest transaction timestamp (unix timestamp), i.e., block timestamp.",
        &["processor_name", "chain_name"],
    )
    .unwrap()
});

pub static HASURA_API_LATEST_TRANSACTION_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_metrics_hasura_latest_transaction_latency_secs",
        "Latest transaction e2e latency, from block timestamp to insertion time of db.",
        &["processor_name", "chain_name"],
    )
    .unwrap()
});

pub static PFN_LEDGER_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_metrics_pfn_ledger_version",
        "Ledger latest version measured from indexer metrics service",
        &["chain_name"],
    )
    .unwrap()
});

pub static PFN_LEDGER_TIMESTAMP: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_metrics_pfn_ledger_timestamp_secs",
        "Ledger latest timestamp (unix timestamp) measured from indexer metrics service",
        &["chain_name"],
    )
    .unwrap()
});
