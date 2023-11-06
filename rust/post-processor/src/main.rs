// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use post_processor::{
    metrics::TASK_FAILURE_COUNT, processor_status_checker::ProcessorStatusChecker,
};
use serde::{Deserialize, Serialize};
use server_framework::{RunnableConfig, ServerArgs};
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorStatusCheckerConfig {
    pub hasura_rest_api_endpoint: String,
    pub fullnode_rest_api_endpoint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostProcessorConfig {
    pub processor_status_checker_config: Option<ProcessorStatusCheckerConfig>,
}

#[async_trait::async_trait]
impl RunnableConfig for PostProcessorConfig {
    async fn run(&self) -> Result<()> {
        let mut tasks = vec![];

        if let Some(config) = &self.processor_status_checker_config {
            tasks.push(tokio::spawn({
                let config = config.clone();
                async move {
                    let checker = ProcessorStatusChecker::new(
                        config.hasura_rest_api_endpoint.clone(),
                        config.fullnode_rest_api_endpoint.clone(),
                    );
                    info!("Starting ProcessorStatusChecker");
                    if let Err(err) = checker.run().await {
                        tracing::error!("ProcessorStatusChecker failed: {:?}", err);
                        TASK_FAILURE_COUNT
                            .with_label_values(&["processor_status_checker"])
                            .inc();
                    }
                }
            }))
        }

        let _ = futures::future::join_all(tasks).await;
        unreachable!("All tasks should run forever");
    }

    fn get_server_name(&self) -> String {
        "idxbg".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = ServerArgs::parse();
    args.run::<PostProcessorConfig>(tokio::runtime::Handle::current())
        .await
}
