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
    processors::parquet_processors::parquet_events_processor::process_transactions_parquet,
    worker::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetEventsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetEventsExtractor {
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

        let (hmap, events) = process_transactions_parquet(transactions.data);

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::MOVE_RESOURCES,
            ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::MoveResource(move_resources),
        )];

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

impl AsyncStep for ParquetEventsExtractor {}

impl NamedStep for ParquetEventsExtractor {
    fn name(&self) -> String {
        "ParquetEventsExtractor".to_string()
    }
}
