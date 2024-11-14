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
        common::models::default_models::raw_table_items::TableItemConvertible,
        parquet::models::default_models::parquet_move_tables::TableItem,
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
        let (_, raw_table_items, _, _) = process_transactions(transactions.data.clone());

        let parquet_table_items: Vec<TableItem> =
            raw_table_items.iter().map(TableItem::from_raw).collect();

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

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [
            (
                TableFlags::MOVE_RESOURCES,
                ParquetTypeEnum::MoveResource,
                ParquetTypeStructs::MoveResource(move_resources),
            ),
            (
                TableFlags::WRITE_SET_CHANGES,
                ParquetTypeEnum::WriteSetChange,
                ParquetTypeStructs::WriteSetChange(write_set_changes),
            ),
            (
                TableFlags::TRANSACTIONS,
                ParquetTypeEnum::Transaction,
                ParquetTypeStructs::Transaction(parquet_transactions),
            ),
            (
                TableFlags::TABLE_ITEMS,
                ParquetTypeEnum::TableItem,
                ParquetTypeStructs::TableItem(parquet_table_items),
            ),
            (
                TableFlags::MOVE_MODULES,
                ParquetTypeEnum::MoveModule,
                ParquetTypeStructs::MoveModule(move_modules),
            ),
        ];

        // Populate the map based on opt-in tables
        for (table_flag, enum_type, data) in data_types {
            add_to_map_if_opted_in_for_backfill(
                self.opt_in_tables,
                &mut map,
                table_flag,
                enum_type,
                data,
            );
        }

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
