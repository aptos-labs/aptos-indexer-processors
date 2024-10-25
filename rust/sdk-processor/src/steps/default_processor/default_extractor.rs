use crate::db::common::models::events_models::events::EventModel;
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::db::common::models::default_models::{
    block_metadata_transactions::BlockMetadataTransaction,
    move_tables::{CurrentTableItem, TableItem, TableMetadata},
};
use processor::processors::default_processor::process_transactions;
use processor::worker::TableFlags;
pub const MIN_TRANSACTIONS_PER_RAYON_JOB: usize = 64;

pub struct DefaultExtractor
where
    Self: Sized + Send + 'static,
{
    pub deprecated_table_flags: TableFlags,
}

#[async_trait]
impl Processable for DefaultExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<BlockMetadataTransaction>,
        (Vec<TableItem>, Vec<CurrentTableItem>, Vec<TableMetadata>),
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<BlockMetadataTransaction>,
                (Vec<TableItem>, Vec<CurrentTableItem>, Vec<TableMetadata>),
            )>,
        >,
        ProcessorError,
    > {
        let flags = self.deprecated_table_flags;
        let (block_metadata_transactions, (table_items, current_table_items, table_metadata)) =
            tokio::task::spawn_blocking(move || process_transactions(transactions.data, flags))
                .await
                .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        Ok(Some(TransactionContext {
            data: (
                block_metadata_transactions,
                (table_items, current_table_items, table_metadata),
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for DefaultExtractor {}

impl NamedStep for DefaultExtractor {
    fn name(&self) -> String {
        "DefaultExtractor".to_string()
    }
}
