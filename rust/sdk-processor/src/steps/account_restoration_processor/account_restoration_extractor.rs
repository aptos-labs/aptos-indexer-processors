use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::db::postgres::models::account_restoration_models::{
    account_restoration_utils::parse_account_restoration_models_from_transaction,
    auth_key_account_addresses::AuthKeyAccountAddress,
    auth_key_multikey_layout::AuthKeyMultikeyLayout, public_key_auth_keys::PublicKeyAuthKey,
};
use rayon::prelude::*;

pub struct AccountRestorationExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for AccountRestorationExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<AuthKeyAccountAddress>,
        Vec<Vec<PublicKeyAuthKey>>,
        Vec<Option<AuthKeyMultikeyLayout>>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let results: Vec<_> = transactions
            .data
            .par_iter()
            .filter_map(parse_account_restoration_models_from_transaction)
            .collect();

        let (auth_key_account_addresses, multikey_outputs): (Vec<_>, Vec<_>) =
            results.into_iter().map(|(a, b, c)| (a, (b, c))).unzip();
        let (public_key_auth_keys, auth_key_multikey_layouts): (Vec<_>, Vec<_>) =
            multikey_outputs.into_iter().unzip();

        Ok(Some(TransactionContext {
            data: (
                auth_key_account_addresses,
                public_key_auth_keys,
                auth_key_multikey_layouts,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for AccountRestorationExtractor {}

impl NamedStep for AccountRestorationExtractor {
    fn name(&self) -> String {
        "AccountRestorationExtractor".to_string()
    }
}
