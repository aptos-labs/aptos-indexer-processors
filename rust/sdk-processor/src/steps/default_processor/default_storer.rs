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
    db::postgres::models::default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
    },
    processors::default_processor::{
        insert_block_metadata_transactions_query, insert_current_table_items_query,
        insert_table_items_query, insert_table_metadata_query,
    },
};

pub struct DefaultStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
}

impl DefaultStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: DefaultProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for DefaultStorer {
    type Input = (
        Vec<BlockMetadataTransactionModel>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    /// Processes a batch of transactions and inserts the extracted data into the database.
    ///
    /// This function takes a `TransactionContext` containing vectors of block metadata transactions,
    /// table items, current table items, and table metadata. It processes these vectors by executing
    /// database insertion operations in chunks to handle large datasets efficiently. The function
    /// uses `futures::try_join!` to run the insertion operations concurrently and ensures that all
    /// operations complete successfully.
    ///
    /// # Arguments
    ///
    /// * `input` - A `TransactionContext` containing:
    ///   * `Vec<BlockMetadataTransactionModel>` - A vector of block metadata transaction models.
    ///   * `Vec<TableItem>` - A vector of table items.
    ///   * `Vec<CurrentTableItem>` - A vector of current table items.
    ///   * `Vec<TableMetadata>` - A vector of table metadata.
    ///
    /// # Returns
    ///
    /// * `Result<Option<TransactionContext<()>>, ProcessorError>` - Returns `Ok(Some(TransactionContext))`
    ///   if all insertion operations complete successfully. Returns an error if any of the operations fail.
    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<BlockMetadataTransactionModel>,
            Vec<TableItem>,
            Vec<CurrentTableItem>,
            Vec<TableMetadata>,
        )>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (block_metadata_transactions, table_items, current_table_items, table_metadata) =
            input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();

        let bmt_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_block_metadata_transactions_query,
            &block_metadata_transactions,
            get_config_table_chunk_size::<BlockMetadataTransactionModel>(
                "block_metadata_transactions",
                &per_table_chunk_sizes,
            ),
        );

        let table_items_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_table_items_query,
            &table_items,
            get_config_table_chunk_size::<TableItem>("table_items", &per_table_chunk_sizes),
        );

        let current_table_items_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_table_items_query,
            &current_table_items,
            get_config_table_chunk_size::<CurrentTableItem>(
                "current_table_items",
                &per_table_chunk_sizes,
            ),
        );

        let table_metadata_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_table_metadata_query,
            &table_metadata,
            get_config_table_chunk_size::<TableMetadata>("table_metadata", &per_table_chunk_sizes),
        );

        futures::try_join!(
            bmt_res,
            table_items_res,
            current_table_items_res,
            table_metadata_res
        )?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for DefaultStorer {}

impl NamedStep for DefaultStorer {
    fn name(&self) -> String {
        "DefaultStorer".to_string()
    }
}
