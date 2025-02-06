use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::fungible_asset_models::{
            raw_v2_fungible_asset_activities::FungibleAssetActivityConvertible,
            raw_v2_fungible_asset_balances::FungibleAssetBalanceConvertible,
            raw_v2_fungible_asset_to_coin_mappings::{
                FungibleAssetToCoinMappings, RawFungibleAssetToCoinMapping,
            },
            raw_v2_fungible_metadata::FungibleAssetMetadataConvertible,
        },
        parquet::models::fungible_asset_models::{
            parquet_v2_fungible_asset_activities::FungibleAssetActivity,
            parquet_v2_fungible_asset_balances::FungibleAssetBalance,
            parquet_v2_fungible_metadata::FungibleAssetMetadataModel,
        },
    },
    processors::fungible_asset_processor::{get_fa_to_coin_mapping, parse_v2_coin},
    utils::{database::ArcDbPool, table_flags::TableFlags},
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetFungibleAssetExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
    pub fa_to_coin_mapping: FungibleAssetToCoinMappings,
}

impl ParquetFungibleAssetExtractor {
    pub fn new(opt_in_tables: TableFlags) -> Self {
        Self {
            opt_in_tables,
            fa_to_coin_mapping: AHashMap::new(),
        }
    }

    pub async fn bootstrap_fa_to_coin_mapping(&mut self, db_pool: ArcDbPool) -> anyhow::Result<()> {
        let mut conn = db_pool.get().await?;
        let mapping = RawFungibleAssetToCoinMapping::get_all_mappings(&mut conn).await;
        self.fa_to_coin_mapping = mapping;
        Ok(())
    }
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
        // get the new fa_to_coin_mapping from the transactions
        let new_fa_to_coin_mapping = get_fa_to_coin_mapping(&transactions.data).await;
        // Merge the mappings
        self.fa_to_coin_mapping.extend(new_fa_to_coin_mapping);

        let (
            raw_fungible_asset_activities,
            raw_fungible_asset_metadata,
            raw_fungible_asset_balances,
            _,
            _raw_coin_supply,
            _raw_fa_to_coin_mappings,
        ) = parse_v2_coin(&transactions.data, Some(&self.fa_to_coin_mapping)).await;

        let parquet_fungible_asset_activities: Vec<FungibleAssetActivity> =
            raw_fungible_asset_activities
                .into_iter()
                .map(FungibleAssetActivity::from_raw)
                .collect();

        let parquet_fungible_asset_metadata: Vec<FungibleAssetMetadataModel> =
            raw_fungible_asset_metadata
                .into_iter()
                .map(FungibleAssetMetadataModel::from_raw)
                .collect();

        let parquet_fungible_asset_balances: Vec<FungibleAssetBalance> =
            raw_fungible_asset_balances
                .into_iter()
                .map(FungibleAssetBalance::from_raw)
                .collect();

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(
            " - V2FungibleAssetActivity: {}",
            parquet_fungible_asset_activities.len()
        );
        debug!(
            " - V2FungibleAssetMetadata: {}",
            parquet_fungible_asset_metadata.len()
        );
        debug!(
            " - V2FungibleAssetBalance: {}",
            parquet_fungible_asset_balances.len()
        );

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [
            (
                TableFlags::FUNGIBLE_ASSET_ACTIVITIES,
                ParquetTypeEnum::FungibleAssetActivities,
                ParquetTypeStructs::FungibleAssetActivity(parquet_fungible_asset_activities),
            ),
            (
                TableFlags::FUNGIBLE_ASSET_METADATA,
                ParquetTypeEnum::FungibleAssetMetadata,
                ParquetTypeStructs::FungibleAssetMetadata(parquet_fungible_asset_metadata),
            ),
            (
                TableFlags::FUNGIBLE_ASSET_BALANCES,
                ParquetTypeEnum::FungibleAssetBalances,
                ParquetTypeStructs::FungibleAssetBalance(parquet_fungible_asset_balances),
            ),
        ];

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
