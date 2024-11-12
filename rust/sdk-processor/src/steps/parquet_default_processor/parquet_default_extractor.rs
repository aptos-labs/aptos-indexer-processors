use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    steps::common::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::db::common::models::default_models::{
    parquet_move_modules::MoveModule,
    parquet_move_resources::MoveResource,
    parquet_move_tables::TableItem,
    parquet_transactions::TransactionModel,
    parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetDefaultExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: Option<Vec<String>>,
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
        let (move_resources, write_set_changes, parquet_transactions, table_items, move_modules) =
            process_transactions(transactions.data);

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(" - MoveResources: {}", move_resources.len());
        debug!(" - WriteSetChanges: {}", write_set_changes.len());
        debug!(" - ParquetTransactions: {}", parquet_transactions.len());
        debug!(" - TableItems: {}", table_items.len());
        debug!(" - MoveModules: {}", move_modules.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant
        let data_types = [
            (
                ParquetTypeEnum::MoveResource,
                ParquetTypeStructs::MoveResource(move_resources),
            ),
            (
                ParquetTypeEnum::WriteSetChange,
                ParquetTypeStructs::WriteSetChange(write_set_changes),
            ),
            (
                ParquetTypeEnum::Transaction,
                ParquetTypeStructs::Transaction(parquet_transactions),
            ),
            (
                ParquetTypeEnum::TableItem,
                ParquetTypeStructs::TableItem(table_items),
            ),
            (
                ParquetTypeEnum::MoveModule,
                ParquetTypeStructs::MoveModule(move_modules),
            ),
        ];

        // Populate the map based on opt-in tables
        for (enum_type, data) in data_types {
            add_to_map_if_opted_in_for_backfill(&self.opt_in_tables, &mut map, enum_type, data);
        }

        debug!(
            "Map populated with data for the following tables: {:?}",
            map.keys().collect::<Vec<_>>()
        );

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<MoveResource>,
    Vec<WriteSetChangeModel>,
    Vec<TransactionModel>,
    Vec<TableItem>,
    Vec<MoveModule>,
) {
    // this will be removed in the future.
    let mut transaction_version_to_struct_count = AHashMap::new();
    let (txns, _, write_set_changes, wsc_details) = TransactionModel::from_transactions(
        &transactions,
        &mut transaction_version_to_struct_count,
    );

    let mut move_modules = vec![];
    let mut move_resources = vec![];
    let mut table_items = vec![];

    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => {
                move_modules.push(module);
            },
            WriteSetChangeDetail::Resource(resource) => {
                move_resources.push(resource);
            },
            WriteSetChangeDetail::Table(item, _, _) => {
                table_items.push(item);
            },
        }
    }

    (
        move_resources,
        write_set_changes,
        txns,
        table_items,
        move_modules,
    )
}

impl AsyncStep for ParquetDefaultExtractor {}

impl NamedStep for ParquetDefaultExtractor {
    fn name(&self) -> String {
        "ParquetDefaultExtractor".to_string()
    }
}
