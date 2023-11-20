// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{
    register_gauge, register_int_counter_vec, register_int_gauge, register_int_gauge_vec, Gauge,
    IntCounterVec, IntGauge, IntGaugeVec,
};

/// Task failure count.
pub static TASK_FAILURE_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "indexer_processors_post_processing_task_failure_count",
        "Task failure count.",
        &["task_name"],
    )
    .unwrap()
});

pub static HASURA_API_LATEST_VERSION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!("hasura_api_latest_version", "Processor latest version", &[
        "processor_name"
    ],)
    .unwrap()
});

pub static HASURA_API_LATEST_VERSION_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "hasura_api_latest_version_timestamp",
        "Processor latest timestamp (unix timestamp)",
        &["processor_name"],
    )
    .unwrap()
});

pub static PFN_LEDGER_VERSION: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("pfn_ledger_version_from_indexer", "Ledger latest version",).unwrap()
});

pub static PFN_LEDGER_TIMESTAMP: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "pfn_ledger_timestamp_from_indexer",
        "Ledger latest timestamp (unix timestamp)",
    )
    .unwrap()
});
