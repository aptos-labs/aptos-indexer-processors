use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::postgres::models::user_transactions_models::{
        signatures::Signature, user_transactions::UserTransactionModel,
    },
    processors::user_transaction_processor::user_transaction_parse,
    worker::TableFlags,
};
pub struct UserTransactionExtractor
where
    Self: Sized + Send + 'static,
{
    deprecated_tables: TableFlags,
}

impl UserTransactionExtractor {
    pub fn new(deprecated_tables: TableFlags) -> Self {
        Self { deprecated_tables }
    }
}

#[async_trait]
impl Processable for UserTransactionExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<UserTransactionModel>, Vec<Signature>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<TransactionContext<(Vec<UserTransactionModel>, Vec<Signature>)>>,
        ProcessorError,
    > {
        let (user_transactions, signatures) =
            user_transaction_parse(item.data, self.deprecated_tables);
        Ok(Some(TransactionContext {
            data: (user_transactions, signatures),
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionExtractor {}

impl NamedStep for UserTransactionExtractor {
    fn name(&self) -> String {
        "UserTransactionExtractor".to_string()
    }
}
