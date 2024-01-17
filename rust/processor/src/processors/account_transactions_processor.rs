// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessorName, ProcessorStorageTrait};
use crate::{
    models::account_transaction_models::account_transactions::AccountTransaction,
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
        PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_processor_sdk::processor::{ProcessingResult, ProcessorTrait};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::result::Error;
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

pub struct AccountTransactionsProcessor {
    connection_pool: PgDbPool,
}

impl AccountTransactionsProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
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

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    account_transactions: &[AccountTransaction],
) -> Result<(), diesel::result::Error> {
    insert_account_transactions(conn, account_transactions).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    account_transactions: Vec<AccountTransaction>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| Box::pin(insert_to_db_impl(pg_conn, &account_transactions)))
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| {
                    Box::pin(async {
                        insert_to_db_impl(pg_conn, &clean_data_for_db(account_transactions, true))
                            .await
                    })
                })
                .await
        },
    }
}

async fn insert_account_transactions(
    conn: &mut MyDbConnection,
    item_to_insert: &[AccountTransaction],
) -> Result<(), diesel::result::Error> {
    use schema::account_transactions::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), AccountTransaction::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::account_transactions::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, account_address))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
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
        _: Option<u8>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;
        let mut account_transactions = HashMap::new();

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
            &mut conn,
            self.name(),
            start_version,
            end_version,
            account_transactions,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
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
}

impl ProcessorStorageTrait for AccountTransactionsProcessor {
    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
