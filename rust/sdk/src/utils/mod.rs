// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "counters")]
use aptos_protos::util::timestamp::Timestamp;

/// Convert the protobuf Timestamp to epcoh time in seconds.
#[cfg(feature = "counters")]
pub fn time_diff_since_pb_timestamp_in_secs(timestamp: &Timestamp) -> f64 {
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!")
        .as_secs_f64();
    let transaction_time = timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9;
    current_timestamp - transaction_time
}
