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
        "indexer_processors_post_processing_task_failure_count",
        "Task failure count.",
        &["task_name"],
    )
    .unwrap()
});

// API last update time latency to current time in seconds.
pub static HASURA_API_LAST_UPDATED_TIME_LATENCY_IN_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "indexer_processors_hasura_api_last_updated_time_latency_in_secs",
        "Processor last update time latency to current time in seconds.",
        &["processor_name"],
    )
    .unwrap()
});

// Processor latest version latency to fullnode latest version.
pub static HASURA_API_LATEST_VERSION_LATENCY: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_processors_hasura_api_latest_version_latency",
        "Processor latest version latency to fullnode latest version.",
        &["processor_name"],
    )
    .unwrap()
});
