use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
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
    processors::fungible_asset_processor::parse_v2_coin,
};

pub struct FungibleAssetExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for FungibleAssetExtractor {
    type Input = Transaction;
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
        transactions: TransactionContext<Transaction>,
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
            data: vec![(
                fungible_asset_activities,
                fungible_asset_metadata,
                fungible_asset_balances,
                current_fungible_asset_balances,
                current_unified_fungible_asset_balances,
                coin_supply,
            )],
            start_version: transactions.start_version,
            end_version: transactions.end_version,
            start_transaction_timestamp: transactions.start_transaction_timestamp,
            end_transaction_timestamp: transactions.end_transaction_timestamp,
            total_size_in_bytes: transactions.total_size_in_bytes,
        }))
    }
}

impl AsyncStep for FungibleAssetExtractor {}

impl NamedStep for FungibleAssetExtractor {
    fn name(&self) -> String {
        "FungibleAssetExtractor".to_string()
    }
}
