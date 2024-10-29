// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{db_config::DbConfig, processor_config::ProcessorConfig};
use crate::processors::{
    account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
    default_processor::DefaultProcessor, events_processor::EventsProcessor,
    fungible_asset_processor::FungibleAssetProcessor, stake_processor::StakeProcessor,
    token_v2_processor::TokenV2Processor,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    traits::processor_trait::ProcessorTrait,
};
use aptos_indexer_processor_sdk_server_framework::RunnableConfig;
use serde::{Deserialize, Serialize};

pub const QUERY_DEFAULT_RETRIES: u32 = 5;
pub const QUERY_DEFAULT_RETRY_DELAY_MS: u64 = 500;

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
            ProcessorConfig::AccountTransactionsProcessor(_) => {
                let acc_txns_processor = AccountTransactionsProcessor::new(self.clone()).await?;
                acc_txns_processor.run_processor().await
            },
            ProcessorConfig::AnsProcessor(_) => {
                let ans_processor = AnsProcessor::new(self.clone()).await?;
                ans_processor.run_processor().await
            },
            ProcessorConfig::DefaultProcessor(_) => {
                let default_processor = DefaultProcessor::new(self.clone()).await?;
                default_processor.run_processor().await
            },
            ProcessorConfig::EventsProcessor(_) => {
                let events_processor = EventsProcessor::new(self.clone()).await?;
                events_processor.run_processor().await
            },
            ProcessorConfig::FungibleAssetProcessor(_) => {
                let fungible_asset_processor = FungibleAssetProcessor::new(self.clone()).await?;
                fungible_asset_processor.run_processor().await
            },
            ProcessorConfig::StakeProcessor(_) => {
                let stake_processor = StakeProcessor::new(self.clone()).await?;
                stake_processor.run_processor().await
            },
            ProcessorConfig::TokenV2Processor(_) => {
                let token_v2_processor = TokenV2Processor::new(self.clone()).await?;
                token_v2_processor.run_processor().await
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
