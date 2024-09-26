// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use chrono::NaiveDateTime;
use clap::Parser;
use indexer_metrics::{
    metrics::{
        HASURA_API_LATEST_TRANSACTION_LATENCY_IN_SECS, HASURA_API_LATEST_TRANSACTION_TIMESTAMP,
        HASURA_API_LATEST_VERSION, HASURA_API_LATEST_VERSION_TIMESTAMP, PFN_LEDGER_TIMESTAMP,
        PFN_LEDGER_VERSION, TASK_FAILURE_COUNT,
    },
    util::{deserialize_from_string, fetch_processor_status_with_timeout, get_url_with_timeout},
};
use serde::{Deserialize, Serialize};
use server_framework::{RunnableConfig, ServerArgs};
use tokio::time::Duration;

const QUERY_TIMEOUT_MS: u64 = 500;
const MIN_TIME_QUERIES_MS: u64 = 500;
const MICROSECONDS_MULTIPLIER: f64 = 1_000_000.0;

#[derive(Debug, Deserialize, Serialize)]
struct FullnodeResponse {
    #[serde(deserialize_with = "deserialize_from_string")]
    ledger_version: u64,
    #[serde(deserialize_with = "deserialize_from_string")]
    ledger_timestamp: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessorStatus {
    processor: String,
    last_updated: NaiveDateTime,
    last_success_version: u64,
    last_transaction_timestamp: Option<NaiveDateTime>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessorsResponseInner {
    processor_status: Vec<ProcessorStatus>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessorsResponse {
    data: ProcessorsResponseInner,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostProcessorConfig {
    pub hasura_graphql_endpoint: Option<String>,
    pub fullnode_rest_api_endpoint: Option<String>,
    pub chain_name: String,
}

#[async_trait::async_trait]
impl RunnableConfig for PostProcessorConfig {
    async fn run(&self) -> Result<()> {
        let mut tasks = vec![];
        let hasura_graphql_endpoint = self.hasura_graphql_endpoint.clone();
        let fullnode_rest_api_endpoint = self.fullnode_rest_api_endpoint.clone();
        let chain_name = self.chain_name.clone();

        if let Some(endpoint) = hasura_graphql_endpoint {
            tasks.push(tokio::spawn(start_processor_status_fetch(
                endpoint,
                chain_name.clone(),
            )));
        }
        if let Some(fullnode) = fullnode_rest_api_endpoint {
            tasks.push(tokio::spawn(start_fn_fetch(fullnode, chain_name)));
        }

        let _ = futures::future::join_all(tasks).await;
        unreachable!("All tasks should run forever");
    }

    fn get_server_name(&self) -> String {
        "idxbg".to_string()
    }
}

#[allow(clippy::needless_return)]
#[tokio::main]
async fn main() -> Result<()> {
    let args = ServerArgs::parse();
    args.run::<PostProcessorConfig>(tokio::runtime::Handle::current())
        .await
}

async fn start_fn_fetch(url: String, chain_name: String) {
    loop {
        let result = get_url_with_timeout(&url, QUERY_TIMEOUT_MS).await;
        let time_now = tokio::time::Instant::now();

        // Handle the result
        match result {
            Ok(Ok(response)) => match response.json::<FullnodeResponse>().await {
                Ok(resp) => {
                    tracing::info!(url = &url, response = ?resp, "Request succeeded");
                    PFN_LEDGER_VERSION
                        .with_label_values(&[&chain_name])
                        .set(resp.ledger_version as i64);
                    PFN_LEDGER_TIMESTAMP
                        .with_label_values(&[&chain_name])
                        .set(resp.ledger_timestamp as f64 / MICROSECONDS_MULTIPLIER);
                },
                Err(err) => {
                    tracing::error!(url = &url, error = ?err, "Parsing error");
                    TASK_FAILURE_COUNT
                        .with_label_values(&["fullnode", &chain_name])
                        .inc();
                },
            },
            Ok(Err(err)) => {
                // Request encountered an error within the timeout
                tracing::error!(url = &url, error = ?err, "Request error");
                TASK_FAILURE_COUNT
                    .with_label_values(&["fullnode", &chain_name])
                    .inc();
            },
            Err(_) => {
                // Request timed out
                tracing::error!(url = &url, "Request timed out");
                TASK_FAILURE_COUNT
                    .with_label_values(&["fullnode", &chain_name])
                    .inc();
            },
        }
        let elapsed = time_now.elapsed().as_millis() as u64;
        // Sleep for a max of 500ms between queries
        if elapsed < MIN_TIME_QUERIES_MS {
            tokio::time::sleep(Duration::from_millis(MIN_TIME_QUERIES_MS - elapsed)).await;
        }
    }
}

async fn start_processor_status_fetch(url: String, chain_name: String) {
    loop {
        let result = fetch_processor_status_with_timeout(&url, QUERY_TIMEOUT_MS).await;
        let time_now = tokio::time::Instant::now();

        // Handle the result
        match result {
            Ok(Ok(response)) => match response.json::<ProcessorsResponse>().await {
                Ok(resp) => {
                    tracing::info!(url = &url, response = ?resp, "Request succeeded");
                    // Process the data as needed
                    let system_time_now = chrono::Utc::now().naive_utc();
                    for processor in resp.data.processor_status {
                        // If the last_transaction_timestamp is None, then the processor has not processed any transactions.
                        // Skip.
                        let last_transaction_timestamp = match processor.last_transaction_timestamp
                        {
                            Some(timestamp) => timestamp,
                            None => continue,
                        };
                        HASURA_API_LATEST_VERSION
                            .with_label_values(&[&processor.processor, &chain_name])
                            .set(processor.last_success_version as i64);
                        HASURA_API_LATEST_VERSION_TIMESTAMP
                            .with_label_values(&[&processor.processor, &chain_name])
                            .set(processor.last_updated.and_utc().timestamp_micros() as f64 * 1e-6);
                        HASURA_API_LATEST_TRANSACTION_TIMESTAMP
                            .with_label_values(&[&processor.processor, &chain_name])
                            .set(
                                last_transaction_timestamp.and_utc().timestamp_micros() as f64
                                    * 1e-6,
                            );
                        let latency = system_time_now - last_transaction_timestamp;
                        HASURA_API_LATEST_TRANSACTION_LATENCY_IN_SECS
                            .with_label_values(&[&processor.processor, &chain_name])
                            .set(latency.num_milliseconds() as f64 * 1e-3);
                    }
                },
                Err(err) => {
                    tracing::error!(url = &url, error = ?err, "Parsing error");
                    // Increment failure count or other error handling
                },
            },
            Ok(Err(err)) => {
                // Request encountered an error within the timeout
                tracing::error!(url = &url, error = ?err, "Request error");
                // Increment failure count or other error handling
            },
            Err(_) => {
                // Request timed out
                tracing::error!(url = &url, "Request timed out");
                // Increment failure count or other error handling
            },
        }

        let elapsed = time_now.elapsed().as_millis() as u64;
        // Sleep for a max of 500ms between queries
        if elapsed < MIN_TIME_QUERIES_MS {
            tokio::time::sleep(Duration::from_millis(MIN_TIME_QUERIES_MS - elapsed)).await;
        }
    }
}

#[allow(clippy::needless_return)]
#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_response_parsing() {
        let response_str = r#"
        {
            "data": {
                "processor_status": [
                {
                    "processor": "token_processor",
                    "last_updated": "2024-07-02T17:23:50.47637",
                    "last_success_version": 1010349813,
                    "last_transaction_timestamp": "2024-07-02T17:23:49.595574"
                }
                ]
            }
        }
        "#;
        let resp: ProcessorsResponse = serde_json::from_str(response_str).unwrap();
        assert_eq!(resp.data.processor_status.len(), 1);
        assert!(resp.data.processor_status[0]
            .last_transaction_timestamp
            .is_some());
        assert_eq!(resp.data.processor_status[0].processor, "token_processor");
        let expected_last_updated =
            NaiveDateTime::parse_from_str("2024-07-02T17:23:50.47637", "%Y-%m-%dT%H:%M:%S%.f")
                .unwrap();
        assert_eq!(
            resp.data.processor_status[0].last_updated,
            expected_last_updated
        );
        let timestamp = resp.data.processor_status[0]
            .last_transaction_timestamp
            .unwrap();
        let actual_datetime =
            NaiveDateTime::parse_from_str("2024-07-02T17:23:49.595574", "%Y-%m-%dT%H:%M:%S%.f")
                .unwrap();
        assert_eq!(timestamp, actual_datetime);
    }

    #[tokio::test]
    async fn test_response_parsing_with_null() {
        let response_str = r#"
        {
            "data": {
                "processor_status": [
                {
                    "processor": "token_processor",
                    "last_updated": "2024-07-02T17:23:50.47637",
                    "last_success_version": 1010349813,
                    "last_transaction_timestamp": null
                }
                ]
            }
        }
        "#;
        let resp: ProcessorsResponse = serde_json::from_str(response_str).unwrap();
        assert_eq!(resp.data.processor_status.len(), 1);
        assert_eq!(resp.data.processor_status[0].processor, "token_processor");
        let expected_last_updated =
            NaiveDateTime::parse_from_str("2024-07-02T17:23:50.47637", "%Y-%m-%dT%H:%M:%S%.f")
                .unwrap();
        assert_eq!(
            resp.data.processor_status[0].last_updated,
            expected_last_updated
        );
        assert!(resp.data.processor_status[0]
            .last_transaction_timestamp
            .is_none());
    }
}
