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
    processors::parquet_processors::parquet_user_transactions_processor::process_transactions,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetUserTransactionExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetUserTransactionExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (user_txns, _) = process_transactions(transactions.data.clone()).await;

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(" - UserTransactions: {}", user_txns.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::USER_TRANSACTIONS,
            ParquetTypeEnum::UserTransactions,
            ParquetTypeStructs::UserTransaction(user_txns),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetUserTransactionExtractor {}

impl NamedStep for ParquetUserTransactionExtractor {
    fn name(&self) -> String {
        "ParquetUserTransactionExtractor".to_string()
    }
}
