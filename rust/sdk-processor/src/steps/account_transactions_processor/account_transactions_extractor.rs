use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::db::postgres::models::account_transaction_models::account_transactions::AccountTransaction;
use rayon::prelude::*;

pub struct AccountTransactionsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for AccountTransactionsExtractor {
    type Input = Vec<Transaction>;
    type Output = Vec<AccountTransaction>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<AccountTransaction>>>, ProcessorError> {
        let acc_txns: Vec<AccountTransaction> = input
            .data
            .into_par_iter()
            .map(|txn| {
                let transaction_version = txn.version as i64;
                let accounts = AccountTransaction::get_accounts(&txn);
                accounts
                    .into_iter()
                    .map(|account_address| AccountTransaction {
                        transaction_version,
                        account_address,
                    })
                    .collect()
            })
            .collect::<Vec<Vec<AccountTransaction>>>()
            .into_iter()
            .flatten()
            .collect();

        Ok(Some(TransactionContext {
            data: acc_txns,
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AccountTransactionsExtractor {}

impl NamedStep for AccountTransactionsExtractor {
    fn name(&self) -> String {
        "AccountTransactionsExtractor".to_string()
    }
}
