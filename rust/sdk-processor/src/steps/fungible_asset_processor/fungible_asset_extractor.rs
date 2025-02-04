use crate::utils::database::ArcDbPool;
use ahash::AHashMap;
use anyhow::Result;
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
            raw_v2_fungible_asset_balances::{
                CurrentUnifiedFungibleAssetBalanceConvertible, FungibleAssetBalanceConvertible,
            },
            raw_v2_fungible_asset_to_coin_mappings::{
                FungibleAssetToCoinMappingConvertible, FungibleAssetToCoinMappings,
                RawFungibleAssetToCoinMapping,
            },
            raw_v2_fungible_metadata::FungibleAssetMetadataConvertible,
        },
        postgres::models::{
            coin_models::coin_supply::CoinSupply,
            fungible_asset_models::{
                v2_fungible_asset_activities::FungibleAssetActivity,
                v2_fungible_asset_balances::{
                    CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
                },
                v2_fungible_asset_to_coin_mappings::FungibleAssetToCoinMapping,
                v2_fungible_metadata::FungibleAssetMetadataModel,
            },
        },
    },
    processors::fungible_asset_processor::{get_fa_to_coin_mapping, parse_v2_coin},
};

/// Extracts fungible asset events, metadata, balances, and v1 supply from transactions
pub struct FungibleAssetExtractor
where
    Self: Sized + Send + 'static,
{
    pub fa_to_coin_mapping: FungibleAssetToCoinMappings,
}

impl FungibleAssetExtractor {
    pub fn new() -> Self {
        Self {
            fa_to_coin_mapping: AHashMap::new(),
        }
    }

    pub async fn bootstrap_fa_to_coin_mapping(&mut self, db_pool: ArcDbPool) -> Result<()> {
        tracing::info!("Started bootstrapping fungible asset to coin mapping");
        let start = std::time::Instant::now();
        let mut conn = db_pool.get().await?;
        let mapping = RawFungibleAssetToCoinMapping::get_all_mappings(&mut conn).await;
        self.fa_to_coin_mapping = mapping;
        tracing::info!(
            item_count = self.fa_to_coin_mapping.len(),
            duration_ms = start.elapsed().as_millis(),
            "Finished bootstrapping fungible asset to coin mapping"
        );
        Ok(())
    }
}

impl Default for FungibleAssetExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Processable for FungibleAssetExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<FungibleAssetActivity>,
        Vec<FungibleAssetMetadataModel>,
        Vec<FungibleAssetBalance>,
        (
            Vec<CurrentUnifiedFungibleAssetBalance>,
            Vec<CurrentUnifiedFungibleAssetBalance>,
        ),
        Vec<CoinSupply>,
        Vec<FungibleAssetToCoinMapping>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<FungibleAssetActivity>,
                Vec<FungibleAssetMetadataModel>,
                Vec<FungibleAssetBalance>,
                (
                    Vec<CurrentUnifiedFungibleAssetBalance>,
                    Vec<CurrentUnifiedFungibleAssetBalance>,
                ),
                Vec<CoinSupply>,
                Vec<FungibleAssetToCoinMapping>,
            )>,
        >,
        ProcessorError,
    > {
        let new_fa_to_coin_mapping = get_fa_to_coin_mapping(&transactions.data).await;
        // Merge the mappings
        self.fa_to_coin_mapping.extend(new_fa_to_coin_mapping);
        let (
            raw_fungible_asset_activities,
            raw_fungible_asset_metadata,
            raw_fungible_asset_balances,
            (raw_current_unified_fab_v1, raw_current_unified_fab_v2),
            coin_supply,
            fa_to_coin_mappings,
        ) = parse_v2_coin(&transactions.data, Some(&self.fa_to_coin_mapping)).await;

        let postgres_fungible_asset_activities: Vec<FungibleAssetActivity> =
            raw_fungible_asset_activities
                .into_iter()
                .map(FungibleAssetActivity::from_raw)
                .collect();

        let postgres_fungible_asset_metadata: Vec<FungibleAssetMetadataModel> =
            raw_fungible_asset_metadata
                .into_iter()
                .map(FungibleAssetMetadataModel::from_raw)
                .collect();

        let postgres_fungible_asset_balances: Vec<FungibleAssetBalance> =
            raw_fungible_asset_balances
                .into_iter()
                .map(FungibleAssetBalance::from_raw)
                .collect();

        let postgres_current_unified_fab_v1: Vec<CurrentUnifiedFungibleAssetBalance> =
            raw_current_unified_fab_v1
                .into_iter()
                .map(CurrentUnifiedFungibleAssetBalance::from_raw)
                .collect();
        let postgres_current_unified_fab_v2: Vec<CurrentUnifiedFungibleAssetBalance> =
            raw_current_unified_fab_v2
                .into_iter()
                .map(CurrentUnifiedFungibleAssetBalance::from_raw)
                .collect();
        let postgres_fa_to_coin_mappings: Vec<FungibleAssetToCoinMapping> = fa_to_coin_mappings
            .into_iter()
            .map(FungibleAssetToCoinMapping::from_raw)
            .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_fungible_asset_activities,
                postgres_fungible_asset_metadata,
                postgres_fungible_asset_balances,
                (
                    postgres_current_unified_fab_v1,
                    postgres_current_unified_fab_v2,
                ),
                coin_supply,
                postgres_fa_to_coin_mappings,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for FungibleAssetExtractor {}

impl NamedStep for FungibleAssetExtractor {
    fn name(&self) -> String {
        "FungibleAssetExtractor".to_string()
    }
}
