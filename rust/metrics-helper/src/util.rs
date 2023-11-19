// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use reqwest::Client;
use serde::{Deserialize, Deserializer};
use std::{str::FromStr, time::Duration};
use tokio::time::{error::Elapsed, timeout};

/// Deserialize from string to type T
pub fn deserialize_from_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    use serde::de::Error;

    let s = <String>::deserialize(deserializer)?;
    s.parse::<T>().map_err(D::Error::custom)
}

pub async fn fetch_url_with_timeout(
    url: &str,
    timeout_ms: u64,
) -> Result<Result<reqwest::Response, reqwest::Error>, Elapsed> {
    let client = Client::new();

    // Set the timeout duration
    let timeout_duration = Duration::from_millis(timeout_ms);

    // Use tokio::time::timeout to set a timeout for the request
    timeout(timeout_duration, client.get(url).send()).await
}
