use crate::{
    config::processor_config::DefaultProcessorConfig,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::postgres::models::account_transaction_models::account_transactions::AccountTransaction,
    processors::account_transactions_processor::insert_account_transactions_query,
};
use tracing::debug;

pub struct AccountTransactionsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
}

impl AccountTransactionsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for AccountTransactionsStorer {
    type Input = Vec<AccountTransaction>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<AccountTransaction>>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_account_transactions_query,
            &input.data,
            get_config_table_chunk_size::<AccountTransaction>(
                "account_transactions",
                &per_table_chunk_sizes,
            ),
        )
        .await;

        match res {
            Ok(_) => {
                debug!(
                    "Account transactions version [{}, {}] stored successfully",
                    input.metadata.start_version, input.metadata.end_version
                );
                Ok(Some(TransactionContext {
                    data: (),
                    metadata: input.metadata,
                }))
            },
            Err(e) => Err(ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store account transactions versions {} to {}: {:?}",
                    input.metadata.start_version, input.metadata.end_version, e,
                ),
                query: None,
            }),
        }
    }
}

impl AsyncStep for AccountTransactionsStorer {}

impl NamedStep for AccountTransactionsStorer {
    fn name(&self) -> String {
        "AccountTransactionsStorer".to_string()
    }
}
