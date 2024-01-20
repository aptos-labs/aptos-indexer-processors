// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::util::timestamp::Timestamp;

// 9999-12-31 23:59:59, this is the max supported by Google BigQuery
pub const MAX_TIMESTAMP_SECS: i64 = 253_402_300_799;

pub fn parse_timestamp(ts: &Timestamp, version: i64) -> chrono::NaiveDateTime {
    let final_ts = if ts.seconds >= MAX_TIMESTAMP_SECS {
        Timestamp {
            seconds: MAX_TIMESTAMP_SECS,
            nanos: 0,
        }
    } else {
        ts.clone()
    };
    chrono::NaiveDateTime::from_timestamp_opt(final_ts.seconds, final_ts.nanos as u32)
        .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

pub fn parse_timestamp_secs(ts: u64, version: i64) -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::from_timestamp_opt(
        std::cmp::min(ts, MAX_TIMESTAMP_SECS as u64) as i64,
        0,
    )
    .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

/// Convert the protobuf timestamp to ISO format
pub fn timestamp_to_iso(timestamp: &Timestamp) -> String {
    let dt = parse_timestamp(timestamp, 0);
    dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

/// Convert the protobuf timestamp to unixtime
pub fn timestamp_to_unixtime(timestamp: &Timestamp) -> f64 {
    timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_timestamp() {
        let ts = parse_timestamp(
            &Timestamp {
                seconds: 1649560602,
                nanos: 0,
            },
            1,
        );
        assert_eq!(ts.timestamp(), 1649560602);
        assert_eq!(ts.year(), 2022);

        let ts2 = parse_timestamp_secs(600000000000000, 2);
        assert_eq!(ts2.year(), 9999);

        let ts3 = parse_timestamp_secs(1659386386, 2);
        assert_eq!(ts3.timestamp(), 1659386386);
    }
}
