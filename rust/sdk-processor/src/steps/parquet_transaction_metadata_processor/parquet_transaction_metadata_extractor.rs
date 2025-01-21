use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
};
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    processors::parquet_processors::parquet_transaction_metadata_processor::process_transaction,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetTransactionMetadataExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetTransactionMetadataExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();
        let write_set_size =
            process_transaction(transactions.data, &mut transaction_version_to_struct_count);

        debug!("Processed data sizes:");
        debug!(" - WriteSetSize: {}", write_set_size.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [(
            TableFlags::WRITE_SET_SIZE,
            ParquetTypeEnum::WriteSetSize,
            ParquetTypeStructs::WriteSetSize(write_set_size),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetTransactionMetadataExtractor {}

impl NamedStep for ParquetTransactionMetadataExtractor {
    fn name(&self) -> String {
        "ParquetTransactionMetadataExtractor".to_string()
    }
}
