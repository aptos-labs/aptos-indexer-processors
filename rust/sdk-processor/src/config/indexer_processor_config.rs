// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{db_config::DbConfig, processor_config::ProcessorConfig};
use crate::processors::{
    events_processor::EventsProcessor, fungible_asset_processor::FungibleAssetProcessor,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    traits::processor_trait::ProcessorTrait,
};
use aptos_indexer_processor_sdk_server_framework::RunnableConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerProcessorConfig {
    pub processor_config: ProcessorConfig,
    pub transaction_stream_config: TransactionStreamConfig,
    pub db_config: DbConfig,
    pub backfill_config: Option<BackfillConfig>,
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerProcessorConfig {
    async fn run(&self) -> Result<()> {
        match self.processor_config {
            ProcessorConfig::EventsProcessor(_) => {
                let events_processor = EventsProcessor::new(self.clone()).await?;
                events_processor.run_processor().await
            },
            ProcessorConfig::FungibleAssetProcessor(_) => {
                let fungible_asset_processor = FungibleAssetProcessor::new(self.clone()).await?;
                fungible_asset_processor.run_processor().await
            },
            ProcessorConfig::AnsProcessor(_) => {
                // let ans_processor = AnsProcessor::new(self.clone()).await?;
                // ans_processor.run_processor().await
                Ok(())
            },
        }
    }

    fn get_server_name(&self) -> String {
        // Get the part before the first _ and trim to 12 characters.
        let before_underscore = self
            .processor_config
            .name()
            .split('_')
            .next()
            .unwrap_or("unknown");
        before_underscore[..before_underscore.len().min(12)].to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillConfig {
    pub backfill_alias: String,
}
