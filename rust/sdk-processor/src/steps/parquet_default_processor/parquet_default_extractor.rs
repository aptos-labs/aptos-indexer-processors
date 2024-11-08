use crate::parquet_processors::ParquetTypeEnum;
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
    parquet_transactions::{Transaction as ParquetTransaction, TransactionModel},
    parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
};
use std::collections::HashMap;
use crate::parquet_processors::ParquetTypeStructs;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetDefaultExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: Option<Vec<String>>,
}

impl ParquetDefaultExtractor {
    fn add_if_opted_in(
        &self,
        map: &mut HashMap<ParquetTypeEnum, ParquetTypeStructs>,
        enum_type: ParquetTypeEnum,
        data: ParquetTypeStructs,
    ) {
        if let Some(ref opt_in_tables) = self.opt_in_tables {
            let table_name = enum_type.to_string();
            if opt_in_tables.contains(&table_name) {
                map.insert(enum_type, data);
            }
        } else {
            // If there's no opt-in table, include all data
            map.insert(enum_type, data);
        }
    }
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
        println!("Processed data sizes:");
        println!(" - MoveResources: {}", move_resources.len());
        println!(" - WriteSetChanges: {}", write_set_changes.len());
        println!(" - ParquetTransactions: {}", parquet_transactions.len());
        println!(" - TableItems: {}", table_items.len());
        println!(" - MoveModules: {}", move_modules.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();
        // Populate the map based on opt-in tables
        self.add_if_opted_in(
            &mut map,
            ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::MoveResource(move_resources),
        );
        self.add_if_opted_in(
            &mut map,
            ParquetTypeEnum::WriteSetChange,
            ParquetTypeStructs::WriteSetChange(write_set_changes),
        );
        self.add_if_opted_in(
            &mut map,
            ParquetTypeEnum::Transaction,
            ParquetTypeStructs::Transaction(parquet_transactions),
        );
        self.add_if_opted_in(
            &mut map,
            ParquetTypeEnum::TableItem,
            ParquetTypeStructs::TableItem(table_items),
        );
        self.add_if_opted_in(
            &mut map,
            ParquetTypeEnum::MoveModule,
            ParquetTypeStructs::MoveModule(move_modules),
        );
        println!("Map populated with data for the following tables: {:?}", map.keys().collect::<Vec<_>>());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

fn clear_unselected_data(
    opt_in_tables: &Option<Vec<String>>,
    move_resources: &mut Vec<MoveResource>,
    write_set_changes: &mut Vec<WriteSetChangeModel>,
    parquet_transactions: &mut Vec<ParquetTransaction>,
    table_items: &mut Vec<TableItem>,
    move_modules: &mut Vec<MoveModule>,
) {
    if let Some(opt_in_tables) = opt_in_tables {
        // Clear vectors only if they are not included in `opt_in_tables`
        if !opt_in_tables.contains(&"move-resources".to_string()) {
            move_resources.clear();
        }
        if !opt_in_tables.contains(&"write-set-changes".to_string()) {
            write_set_changes.clear();
        }
        if !opt_in_tables.contains(&"parquet-transactions".to_string()) {
            parquet_transactions.clear();
        }
        if !opt_in_tables.contains(&"move-modules".to_string()) {
            move_modules.clear();
        }
        if !opt_in_tables.contains(&"table-items".to_string()) {
            table_items.clear();
        }
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
    let (txns, _block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(
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
