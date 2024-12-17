use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::default_models::{
            raw_block_metadata_transactions::BlockMetadataTransactionConvertible,
            raw_current_table_items::CurrentTableItemConvertible,
            raw_table_items::TableItemConvertible, raw_table_metadata::TableMetadataConvertible,
        },
        postgres::models::default_models::{
            block_metadata_transactions::BlockMetadataTransactionModel,
            move_tables::{CurrentTableItem, TableItem, TableMetadata},
        },
    },
    processors::default_processor::process_transactions,
    utils::table_flags::TableFlags,
};

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
        Vec<BlockMetadataTransactionModel>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<BlockMetadataTransactionModel>,
                Vec<TableItem>,
                Vec<CurrentTableItem>,
                Vec<TableMetadata>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            raw_block_metadata_transactions,
            raw_table_items,
            raw_current_table_items,
            raw_table_metadata,
        ) = process_transactions(transactions.data.clone());

        let postgres_table_items: Vec<TableItem> =
            raw_table_items.iter().map(TableItem::from_raw).collect();
        let postgres_current_table_items: Vec<CurrentTableItem> = raw_current_table_items
            .iter()
            .map(CurrentTableItem::from_raw)
            .collect();
        let postgres_block_metadata_transactions: Vec<BlockMetadataTransactionModel> =
            raw_block_metadata_transactions
                .into_iter()
                .map(BlockMetadataTransactionModel::from_raw)
                .collect();
        let postgres_table_metadata = raw_table_metadata
            .iter()
            .map(TableMetadata::from_raw)
            .collect();

        Ok(Some(TransactionContext {
            data: (
                postgres_block_metadata_transactions,
                postgres_table_items,
                postgres_current_table_items,
                postgres_table_metadata,
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
