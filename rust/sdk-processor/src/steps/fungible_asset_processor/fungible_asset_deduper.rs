use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{
        async_step::AsyncRunType, AsyncStep, NamedStep, PollableAsyncRunType, PollableAsyncStep,
        Processable,
    },
    types::transaction_context::{Context, TransactionContext},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::associations::Identifiable;
use processor::db::common::models::{
    coin_models::coin_supply::CoinSupply,
    fungible_asset_models::{
        v2_fungible_asset_activities::FungibleAssetActivity,
        v2_fungible_asset_balances::{
            CurrentFungibleAssetBalance, CurrentFungibleAssetBalancePK,
            CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
        },
        v2_fungible_metadata::FungibleAssetMetadataModel,
    },
};
use std::{collections::BTreeSet, time::Duration};

pub struct FungibleAssetDeduper
where
    Self: Sized + Send + 'static,
{
    // The duration to collect and dedup data before releasing it
    poll_interval: Duration,
    merged_cfab: AHashMap<CurrentFungibleAssetBalancePK, CurrentFungibleAssetBalance>,
    contexts: BTreeSet<Context>,
}

impl FungibleAssetDeduper
where
    Self: Sized + Send + 'static,
{
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            poll_interval,
            merged_cfab: AHashMap::new(),
            contexts: BTreeSet::new(),
        }
    }
}

#[async_trait]
impl Processable for FungibleAssetDeduper {
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
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        items: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (_, _, _, cfab, _, _) = &items.data[0];

        // Update transaction contexts
        for context in items.context.iter() {
            self.contexts.insert(context.clone());
        }

        // Dedup
        for balance in cfab.iter() {
            self.merged_cfab
                .insert(balance.id().to_string(), balance.clone());
        }

        Ok(None)
    }
}

#[async_trait]
impl PollableAsyncStep for FungibleAssetDeduper {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        let transaction_context = TransactionContext::new_with_contexts(
            vec![(
                vec![],
                vec![],
                vec![],
                std::mem::take(&mut self.merged_cfab)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![],
                vec![],
            )],
            self.contexts.clone(),
        );
        Ok(Some(vec![transaction_context]))
    }
}

impl NamedStep for FungibleAssetDeduper {
    fn name(&self) -> String {
        "FungibleAssetDeduper".to_string()
    }
}
