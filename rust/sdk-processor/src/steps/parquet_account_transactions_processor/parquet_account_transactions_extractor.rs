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
use chrono::NaiveDateTime;
use processor::{
    db::{
        common::models::account_transaction_models::raw_account_transactions::RawAccountTransaction,
        parquet::models::account_transaction_models::parquet_account_transactions::AccountTransaction,
    },
    utils::table_flags::TableFlags,
};
use rayon::prelude::*;
use std::collections::HashMap;
use tracing::debug;
pub struct ParquetAccountTransactionsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetAccountTransactionsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let acc_txns: Vec<AccountTransaction> = transactions
            .data
            .into_par_iter()
            .map(|txn| {
                let transaction_version = txn.version as i64;
                let txn_timestamp = txn
                    .timestamp
                    .as_ref()
                    .expect("Transaction timestamp doesn't exist!")
                    .seconds;
                #[allow(deprecated)]
                let block_timestamp = NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
                    .expect("Txn Timestamp is invalid!");

                let accounts = RawAccountTransaction::get_accounts(&txn);
                accounts
                    .into_iter()
                    .map(|account_address| AccountTransaction {
                        txn_version: transaction_version,
                        account_address,
                        block_timestamp,
                    })
                    .collect()
            })
            .collect::<Vec<Vec<AccountTransaction>>>()
            .into_iter()
            .flatten()
            .collect();
        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(" - AccountTransaction: {}", acc_txns.len());

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::ACCOUNT_TRANSACTIONS,
            ParquetTypeEnum::AccountTransactions,
            ParquetTypeStructs::AccountTransaction(acc_txns),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetAccountTransactionsExtractor {}

impl NamedStep for ParquetAccountTransactionsExtractor {
    fn name(&self) -> String {
        "ParquetAccountTransactionsExtractor".to_string()
    }
}
