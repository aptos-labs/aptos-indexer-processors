// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{models::account_transaction_models::account_transactions::AccountTransaction, schema};
use ahash::AHashMap;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::query_builder::QueryFragment;

pub struct AccountTransactionsProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl std::fmt::Debug for AccountTransactionsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool().state();
        write!(
            f,
            "{:} {{ connections: {:?}  idle_connections: {:?} }}",
            self.name(),
            state.connections,
            state.idle_connections
        )
    }
}

impl AccountTransactionsProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

pub fn insert_account_transactions_query(
    items_to_insert: &[AccountTransaction],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::account_transactions::dsl::*;

    diesel::insert_into(schema::account_transactions::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, account_address))
        .do_nothing()
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
        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Finished parsing, sending to DB",
        );
        self.db_writer()
            .send_in_chunks(
                "account_transactions",
                account_transactions,
                insert_account_transactions_query,
            )
            .await;

        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_channel_insertion_duration_in_secs,
            last_transaction_timestamp,
        })
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
