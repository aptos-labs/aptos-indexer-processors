// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::account_transaction_models::account_transactions::AccountTransaction,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment};
use std::fmt::Debug;
use tracing::error;

pub struct AccountTransactionsProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl AccountTransactionsProcessor {
    pub fn new(connection_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for AccountTransactionsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AccountTransactionsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    account_transactions: &[AccountTransaction],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    execute_in_chunks(
        conn.clone(),
        insert_account_transactions_query,
        account_transactions,
        get_config_table_chunk_size::<AccountTransaction>(
            "account_transactions",
            per_table_chunk_sizes,
        ),
    )
    .await?;
    Ok(())
}

fn insert_account_transactions_query(
    item_to_insert: Vec<AccountTransaction>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::account_transactions::dsl::*;

    (
        diesel::insert_into(schema::account_transactions::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, account_address))
            .do_nothing(),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for AccountTransactionsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::AccountTransactionsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut account_transactions = AHashMap::new();

        for txn in &transactions {
            account_transactions.extend(AccountTransaction::from_transaction(txn));
        }
        let mut account_transactions = account_transactions
            .into_values()
            .collect::<Vec<AccountTransaction>>();

        // Sort by PK
        account_transactions.sort_by(|a, b| {
            (&a.transaction_version, &a.account_address)
                .cmp(&(&b.transaction_version, &b.account_address))
        });

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();
        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &account_transactions,
            &self.per_table_chunk_sizes,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp,
            }),
            Err(err) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    "[Parser] Error inserting transactions to db: {:?}",
                    err
                );
                bail!(format!("Error inserting transactions to db. Processor {}. Start {}. End {}. Error {:?}", self.name(), start_version, end_version, err))
            },
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
