// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db_writer::execute_in_chunks,
    models::user_transactions_models::{
        signatures::Signature, user_transactions::UserTransactionModel,
    },
    schema,
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, database::get_config_table_chunk_size},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use tracing::error;
use crate::db_writer::QueryWrapper;

pub struct UserTransactionProcessor {
    db_writer: crate::db_writer::DbWriter,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl std::fmt::Debug for UserTransactionProcessor {
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

impl UserTransactionProcessor {
    pub fn new(
        db_writer: crate::db_writer::DbWriter,
        per_table_chunk_sizes: AHashMap<String, usize>,
    ) -> Self {
        Self {
            db_writer,
            per_table_chunk_sizes,
        }
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    user_transactions: Vec<UserTransactionModel>,
    signatures: Vec<Signature>,
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let query_sender = db_writer.query_sender.clone();
    let ut = execute_in_chunks(
        "TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_user_transactions_query,
        user_transactions,
        get_config_table_chunk_size::<UserTransactionModel>(
            "user_transactions",
            per_table_chunk_sizes,
        ),
    );
    let is = execute_in_chunks(
        "TABLE_NAME_PLACEHOLDER",
        query_sender,
        insert_signatures_query,
        signatures,
        get_config_table_chunk_size::<Signature>("signatures", per_table_chunk_sizes),
    );

    tokio::join!(ut, is);
    Ok(())
}

fn insert_user_transactions_query(
    items_to_insert: &[UserTransactionModel],
) -> (
    QueryWrapper,
    Option<&'static str>,
) {
    use schema::user_transactions::dsl::*;
    (
        QueryWrapper::new(diesel::insert_into(schema::user_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                expiration_timestamp_secs.eq(excluded(expiration_timestamp_secs)),
                inserted_at.eq(excluded(inserted_at)),
            ))),
        None,
    )
}

fn insert_signatures_query(
    items_to_insert: &[Signature],
) -> (
    QueryWrapper,
    Option<&'static str>,
) {
    use schema::signatures::dsl::*;
    (
        QueryWrapper::new(diesel::insert_into(schema::signatures::table)
            .values(items_to_insert)
            .on_conflict((
                transaction_version,
                multi_agent_index,
                multi_sig_index,
                is_sender_primary,
            ))
            .do_nothing()),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for UserTransactionProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::UserTransactionProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut signatures = vec![];
        let mut user_transactions = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = match txn.txn_data.as_ref() {
                Some(txn_data) => txn_data,
                None => {
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["UserTransactionProcessor"])
                        .inc();
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    continue;
                }
            };
            if let TxnData::User(inner) = txn_data {
                let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                    inner,
                    txn.timestamp.as_ref().unwrap(),
                    block_height,
                    txn.epoch as i64,
                    txn_version,
                );
                signatures.extend(sigs);
                user_transactions.push(user_transaction);
            }
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            user_transactions,
            signatures,
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
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            }
        }
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
