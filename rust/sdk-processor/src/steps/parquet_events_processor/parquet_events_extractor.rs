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
    processors::parquet_processors::parquet_events_processor::process_transactions_parquet,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;

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
        let (_txn_ver_map, events) = process_transactions_parquet(transactions.data);

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [(
            TableFlags::EVENTS,
            ParquetTypeEnum::Events,
            ParquetTypeStructs::Event(events),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

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
