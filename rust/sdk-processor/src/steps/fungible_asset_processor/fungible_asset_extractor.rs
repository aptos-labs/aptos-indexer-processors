use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::postgres::models::{
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
            fungible_asset_activities,
            fungible_asset_metadata,
            fungible_asset_balances,
            current_fungible_asset_balances,
            current_unified_fungible_asset_balances,
            coin_supply,
        ) = parse_v2_coin(&transactions.data).await;

        Ok(Some(TransactionContext {
            data: (
                fungible_asset_activities,
                fungible_asset_metadata,
                fungible_asset_balances,
                current_fungible_asset_balances,
                current_unified_fungible_asset_balances,
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
