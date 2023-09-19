// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::metrics::{
    HASURA_API_LAST_UPDATED_TIME_LATENCY_IN_SECS, HASURA_API_LATEST_VERSION_LATENCY,
};
use anyhow::Result;
use chrono::NaiveDateTime;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::info;

const PROCESSOR_STATUS_CHECKER_WAIT_TIME_IN_SECS: u64 = 10;

pub struct ProcessorStatusChecker {
    pub hasura_rest_api_endpoint: String,
    pub fullnode_rest_api_endpoint: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessorStatusResponse {
    processor_status: Vec<ProcessorStatus>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProcessorStatus {
    pub processor: String,
    pub last_updated: String,
    pub last_success_version: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct FullnodeResponse {
    chain_id: u8,
    epoch: String,
    ledger_version: String,
    oldest_ledger_version: String,
    ledger_timestamp: String,
    node_role: String,
    oldest_block_height: String,
    block_height: String,
    git_hash: String,
}

impl ProcessorStatusChecker {
    pub fn new(hasura_rest_api_endpoint: String, fullnode_rest_api_endpoint: String) -> Self {
        Self {
            hasura_rest_api_endpoint,
            fullnode_rest_api_endpoint,
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let processor_latest_version_map = handle_hasura_response(
                self.hasura_rest_api_endpoint.clone(),
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!(e = ?e, "Failed to get processor status response from hasura");
                panic!();
            });

            let fullnode_latest_version =
                handle_fullnode_api_response(self.fullnode_rest_api_endpoint.clone())
                    .await
                    .unwrap_or_else(|e| {
                        tracing::error!(e = ?e, "Failed to get fullnode response from fullnode");
                        panic!();
                    });
            for processor_latest_version in processor_latest_version_map {
                let latency = fullnode_latest_version - processor_latest_version.1;
                HASURA_API_LATEST_VERSION_LATENCY
                    .with_label_values(&[processor_latest_version.0.as_str()])
                    .set(latency);
            }
            tokio::time::sleep(Duration::from_secs(
                PROCESSOR_STATUS_CHECKER_WAIT_TIME_IN_SECS,
            ))
            .await;
        }
    }
}

async fn handle_hasura_response(hasura_endpoint: String) -> Result<HashMap<String, i64>> {
    let endpoint = hasura_endpoint.clone();
    info!("Connecting to hasura endpoint: {}", endpoint);
    let client = reqwest::Client::new();
    let result = client.get(endpoint).send().await?;
    let processor_status_response_result = result.json::<ProcessorStatusResponse>().await;
    let processor_status_response = match processor_status_response_result {
        Ok(processor_status_response) => processor_status_response,
        Err(e) => {
            anyhow::bail!("Failed to handle hasura api response: {:?}", e);
        },
    };

    let mut processor_latest_version_map = HashMap::new();

    for processor_status in processor_status_response.processor_status {
        let last_updated_time = NaiveDateTime::parse_from_str(
            processor_status.last_updated.as_str(),
            "%Y-%m-%dT%H:%M:%S%.f",
        )
        .unwrap();
        let current_time = SystemTime::now();
        let latency = current_time.duration_since(UNIX_EPOCH)?.as_secs_f64()
            - last_updated_time
                .signed_duration_since(NaiveDateTime::from_timestamp_opt(0, 0).unwrap())
                .to_std()?
                .as_secs_f64();
        HASURA_API_LAST_UPDATED_TIME_LATENCY_IN_SECS
            .with_label_values(&[processor_status.processor.as_str()])
            .set(latency);
        processor_latest_version_map.insert(
            processor_status.processor,
            processor_status.last_success_version,
        );
    }
    Ok(processor_latest_version_map)
}

async fn handle_fullnode_api_response(fullnode_endpoint: String) -> Result<i64> {
    let endpoint = fullnode_endpoint.clone();
    info!("Connecting to fullnode endpoint: {}", endpoint);
    let client = reqwest::Client::new();
    let result = client.get(endpoint).send().await?;
    let fullnode_response_result = result.json::<FullnodeResponse>().await;
    match fullnode_response_result {
        Ok(fullnode_response) => Ok(fullnode_response.ledger_version.parse::<i64>().unwrap()),
        Err(e) => anyhow::bail!("Failed to handle fullnode api response: {:?}", e),
    }
}
