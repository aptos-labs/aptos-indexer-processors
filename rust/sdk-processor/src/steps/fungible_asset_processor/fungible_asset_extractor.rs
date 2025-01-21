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
                CurrentFungibleAssetBalanceConvertible,
                CurrentUnifiedFungibleAssetBalanceConvertible, FungibleAssetBalanceConvertible,
            },
            raw_v2_fungible_metadata::FungibleAssetMetadataConvertible,
        },
        postgres::models::{
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
    },
    processors::fungible_asset_processor::parse_v2_coin,
};

/// Extracts fungible asset events, metadata, balances, and v1 supply from transactions
pub struct FungibleAssetExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for FungibleAssetExtractor {
    type Input = Vec<Transaction>;
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
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<FungibleAssetActivity>,
                Vec<FungibleAssetMetadataModel>,
                Vec<FungibleAssetBalance>,
                Vec<CurrentFungibleAssetBalance>,
                Vec<CurrentUnifiedFungibleAssetBalance>,
                Vec<CoinSupply>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            raw_fungible_asset_activities,
            raw_fungible_asset_metadata,
            raw_fungible_asset_balances,
            raw_current_fungible_asset_balances,
            raw_current_unified_fungible_asset_balances,
            coin_supply,
        ) = parse_v2_coin(&transactions.data).await;

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

        let postgres_current_fungible_asset_balances: Vec<CurrentFungibleAssetBalance> =
            raw_current_fungible_asset_balances
                .into_iter()
                .map(CurrentFungibleAssetBalance::from_raw)
                .collect();

        let postgres_current_unified_fungible_asset_balances: Vec<
            CurrentUnifiedFungibleAssetBalance,
        > = raw_current_unified_fungible_asset_balances
            .into_iter()
            .map(CurrentUnifiedFungibleAssetBalance::from_raw)
            .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_fungible_asset_activities,
                postgres_fungible_asset_metadata,
                postgres_fungible_asset_balances,
                postgres_current_fungible_asset_balances,
                postgres_current_unified_fungible_asset_balances,
                coin_supply,
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
