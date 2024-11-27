use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::fungible_asset_models::raw_v2_fungible_asset_activities::FungibleAssetActivityConvertible,
        parquet::models::fungible_asset_models::parquet_v2_fungible_asset_activities::FungibleAssetActivity,
    },
    processors::fungible_asset_processor::parse_v2_coin,
    worker::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetFungibleAssetExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetFungibleAssetExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (
            raw_fungible_asset_activities,
            _raw_fungible_asset_metadata,
            _raw_fungible_asset_balances,
            _raw_current_fungible_asset_balances,
            _raw_current_unified_fungible_asset_balances,
            _raw_coin_supply,
        ) = parse_v2_coin(&transactions.data).await;

        let parquet_fungible_asset_activities: Vec<FungibleAssetActivity> =
            raw_fungible_asset_activities
                .into_iter()
                .map(FungibleAssetActivity::from_raw)
                .collect();

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(
            " - V2FungibleAssetActivity: {}",
            parquet_fungible_asset_activities.len()
        );

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::FUNGIBLE_ASSET_ACTIVITIES,
            ParquetTypeEnum::FungibleAssetActivity,
            ParquetTypeStructs::FungibleAssetActivity(parquet_fungible_asset_activities),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetFungibleAssetExtractor {}

impl NamedStep for ParquetFungibleAssetExtractor {
    fn name(&self) -> String {
        "ParquetFungibleAssetExtractor".to_string()
    }
}
