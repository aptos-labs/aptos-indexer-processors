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

pub struct ParquetDefaultExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: Option<Vec<String>>,
}

#[async_trait]
impl Processable for ParquetDefaultExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<ParquetTransaction>,
        Vec<MoveResource>,
        Vec<WriteSetChangeModel>,
        Vec<TableItem>,
        Vec<MoveModule>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<
        Option<
            TransactionContext<(
                Vec<ParquetTransaction>,
                Vec<MoveResource>,
                Vec<WriteSetChangeModel>,
                Vec<TableItem>,
                Vec<MoveModule>,
            )>,
        >,
        ProcessorError,
    > {
        let backfill_mode = self.opt_in_tables.is_some();

        let (
            mut move_resources,
            mut write_set_changes,
            mut parquet_transactions,
            mut table_items,
            mut move_modules,
        ) = process_transactions(transactions.data);

        if backfill_mode {
            clear_unselected_data(
                &self.opt_in_tables,
                &mut move_resources,
                &mut write_set_changes,
                &mut parquet_transactions,
                &mut table_items,
                &mut move_modules,
            );
        }

        Ok(Some(TransactionContext {
            data: (
                parquet_transactions,
                move_resources,
                write_set_changes,
                table_items,
                move_modules,
            ),
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
            WriteSetChangeDetail::Table(item, _current_item, _) => {
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
