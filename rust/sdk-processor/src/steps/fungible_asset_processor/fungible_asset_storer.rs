use crate::{
    db::common::models::events_models::events::EventModel,
    processors::events_processor::EventsProcessorConfig,
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
    schema,
};
use tracing::debug;

pub struct FungibleAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: EventsProcessorConfig,
}

impl FungibleAssetStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: EventsProcessorConfig) -> Self {
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
        events: TransactionContext<(
            Vec<FungibleAssetActivity>,
            Vec<FungibleAssetMetadataModel>,
            Vec<FungibleAssetBalance>,
            Vec<CurrentFungibleAssetBalance>,
            Vec<CurrentUnifiedFungibleAssetBalance>,
            Vec<CoinSupply>,
        )>,
    ) -> Result<Option<TransactionContext<EventModel>>, ProcessorError> {

        // match execute_res {
        //     Ok(_) => {
        //         debug!(
        //             "Events version [{}, {}] stored successfully",
        //             events.start_version, events.end_version
        //         );
        //         Ok(Some(events))
        //     },
        //     Err(e) => Err(ProcessorError::DBStoreError {
        //         message: format!(
        //             "Failed to store events versions {} to {}: {:?}",
        //             events.start_version, events.end_version, e,
        //         ),
        //     }),
        // }
    }
}

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}
