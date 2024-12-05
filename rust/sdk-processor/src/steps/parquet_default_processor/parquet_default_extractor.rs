use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
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
        parquet::models::default_models::{
            parquet_block_metadata_transactions::BlockMetadataTransaction,
            parquet_move_tables::{CurrentTableItem, TableItem},
            parquet_table_metadata::TableMetadata,
        },
    },
    processors::{
        default_processor::process_transactions,
        parquet_processors::parquet_default_processor::process_transactions_parquet,
    },
    worker::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetDefaultExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetDefaultExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (
            raw_block_metadata_transactions,
            raw_table_items,
            raw_current_table_items,
            raw_table_metadata,
        ) = process_transactions(transactions.data.clone());

        let parquet_table_items: Vec<TableItem> =
            raw_table_items.iter().map(TableItem::from_raw).collect();
        let parquet_current_table_items: Vec<CurrentTableItem> = raw_current_table_items
            .iter()
            .map(CurrentTableItem::from_raw)
            .collect();
        let parquet_block_metadata_transactions: Vec<BlockMetadataTransaction> =
            raw_block_metadata_transactions
                .iter()
                .map(BlockMetadataTransaction::from_raw)
                .collect();
        let parquet_table_metadata: Vec<TableMetadata> = raw_table_metadata
            .iter()
            .map(TableMetadata::from_raw)
            .collect();

        let (
            (
                move_resources,
                write_set_changes,
                parquet_transactions,
                _parquet_table_items,
                move_modules,
            ),
            _transaction_version_to_struct_count,
        ) = process_transactions_parquet(transactions.data);

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(" - MoveResources: {}", move_resources.len());
        debug!(" - WriteSetChanges: {}", write_set_changes.len());
        debug!(" - ParquetTransactions: {}", parquet_transactions.len());
        debug!(" - TableItems: {}", parquet_table_items.len());
        debug!(" - MoveModules: {}", move_modules.len());
        debug!(
            " - CurrentTableItems: {}",
            parquet_current_table_items.len()
        );
        debug!(
            " - BlockMetadataTransactions: {}",
            parquet_block_metadata_transactions.len()
        );
        debug!(" - TableMetadata: {}", parquet_table_metadata.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [
            (
                TableFlags::MOVE_RESOURCES,
                ParquetTypeEnum::MoveResources,
                ParquetTypeStructs::MoveResource(move_resources),
            ),
            (
                TableFlags::WRITE_SET_CHANGES,
                ParquetTypeEnum::WriteSetChanges,
                ParquetTypeStructs::WriteSetChange(write_set_changes),
            ),
            (
                TableFlags::TRANSACTIONS,
                ParquetTypeEnum::Transactions,
                ParquetTypeStructs::Transaction(parquet_transactions),
            ),
            (
                TableFlags::TABLE_ITEMS,
                ParquetTypeEnum::TableItems,
                ParquetTypeStructs::TableItem(parquet_table_items),
            ),
            (
                TableFlags::MOVE_MODULES,
                ParquetTypeEnum::MoveModules,
                ParquetTypeStructs::MoveModule(move_modules),
            ),
            (
                TableFlags::CURRENT_TABLE_ITEMS,
                ParquetTypeEnum::CurrentTableItems,
                ParquetTypeStructs::CurrentTableItem(parquet_current_table_items),
            ),
            (
                TableFlags::BLOCK_METADATA_TRANSACTIONS,
                ParquetTypeEnum::BlockMetadataTransactions,
                ParquetTypeStructs::BlockMetadataTransaction(parquet_block_metadata_transactions),
            ),
            (
                TableFlags::TABLE_METADATAS,
                ParquetTypeEnum::TableMetadata,
                ParquetTypeStructs::TableMetadata(parquet_table_metadata),
            ),
        ];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetDefaultExtractor {}

impl NamedStep for ParquetDefaultExtractor {
    fn name(&self) -> String {
        "ParquetDefaultExtractor".to_string()
    }
}
