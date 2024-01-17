// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, IntCounter};

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
