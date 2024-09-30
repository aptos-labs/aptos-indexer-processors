use crate::{
    db::common::models::events_models::events::EventModel,
    processors::{
        events_processor::EventsProcessorConfig,
        fungible_asset_processor::FungibleAssetProcessorConfig,
    },
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use processor::{
    db::common::models::{
        coin_models::coin_supply::CoinSupply,
        fungible_asset_models::{
            v2_fungible_asset_activities::FungibleAssetActivity,
            v2_fungible_asset_balances::{
                CurrentFungibleAssetBalance, CurrentUnifiedFungibleAssetBalance,
                FungibleAssetBalance,
            },
            v2_fungible_metadata::FungibleAssetMetadataModel,
        },
    },
    processors::fungible_asset_processor::insert_current_fungible_asset_balances_query,
    schema,
};
use tracing::debug;

pub struct FungibleAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: FungibleAssetProcessorConfig,
}

impl FungibleAssetStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: FungibleAssetProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for FungibleAssetStorer {
    type Input = (
        Vec<FungibleAssetActivity>,
        Vec<FungibleAssetMetadataModel>,
        Vec<FungibleAssetBalance>,
        Vec<CurrentFungibleAssetBalance>,
        Vec<CurrentUnifiedFungibleAssetBalance>,
        Vec<CoinSupply>,
    );
    type Output = (
        Vec<FungibleAssetActivity>,
        Vec<FungibleAssetMetadataModel>,
        Vec<FungibleAssetBalance>,
        Vec<CurrentFungibleAssetBalance>,
        Vec<CurrentUnifiedFungibleAssetBalance>,
        Vec<CoinSupply>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        items: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (_, _, _, current_fungible_asset_balances, _, _) = &items.data[0];

        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_fungible_asset_balances_query,
            current_fungible_asset_balances,
            get_config_table_chunk_size::<CurrentFungibleAssetBalance>(
                "current_fungible_asset_balances",
                &self.processor_config.per_table_chunk_sizes,
            ),
        )
        .await;
        match execute_res {
            Ok(_) => {
                debug!(
                    "FA version {} stored successfully",
                    items
                        .get_versions()
                        .iter()
                        .map(|(x, y)| format!("[{}, {}]", x, y))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                Ok(Some(items))
            },
            Err(e) => Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store events versions {}: {:?}",
                    items
                        .get_versions()
                        .iter()
                        .map(|(x, y)| format!("[{}, {}]", x, y))
                        .collect::<Vec<_>>()
                        .join(", "),
                    e,
                ),
                // TODO: fix it with a debug_query.
                query: None,
            }),
        }
    }
}

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}
