// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessorName, ProcessorStorageTrait};
use crate::{
    models::user_transactions_models::{
        signatures::Signature, user_transactions::UserTransactionModel,
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
        PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_processor_sdk::processor::{ProcessingResult, ProcessorTrait};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods};
use field_count::FieldCount;
use std::fmt::Debug;
use tracing::error;

pub struct UserTransactionProcessor {
    connection_pool: PgDbPool,
}

impl UserTransactionProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for UserTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "UserTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    user_transactions: &[UserTransactionModel],
    signatures: &[Signature],
) -> Result<(), diesel::result::Error> {
    insert_user_transactions(conn, user_transactions).await?;
    insert_signatures(conn, signatures).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    user_transactions: Vec<UserTransactionModel>,
    signatures: Vec<Signature>,
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
        .run::<_, Error, _>(|pg_conn| {
            Box::pin(insert_to_db_impl(pg_conn, &user_transactions, &signatures))
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            let user_transactions = clean_data_for_db(user_transactions, true);
            let signatures = clean_data_for_db(signatures, true);

            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| {
                    Box::pin(async move {
                        insert_to_db_impl(pg_conn, &user_transactions, &signatures).await
                    })
                })
                .await
        },
    }
}

async fn insert_user_transactions(
    conn: &mut MyDbConnection,
    items_to_insert: &[UserTransactionModel],
) -> Result<(), diesel::result::Error> {
    use schema::user_transactions::dsl::*;
    let chunks = get_chunks(items_to_insert.len(), UserTransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::user_transactions::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(version)
                .do_update()
                .set((
                    expiration_timestamp_secs.eq(excluded(expiration_timestamp_secs)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_signatures(
    conn: &mut MyDbConnection,
    items_to_insert: &[Signature],
) -> Result<(), diesel::result::Error> {
    use schema::signatures::dsl::*;
    let chunks = get_chunks(items_to_insert.len(), Signature::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::signatures::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((
                    transaction_version,
                    multi_agent_index,
                    multi_sig_index,
                    is_sender_primary,
                ))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
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
        _: Option<u8>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;
        let mut signatures = vec![];
        let mut user_transactions = vec![];
        for txn in transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            if let TxnData::User(inner) = txn_data {
                let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                    inner,
                    &txn.timestamp.unwrap(),
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
            &mut conn,
            self.name(),
            start_version,
            end_version,
            user_transactions,
            signatures,
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
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        }
    }
}

#[async_trait]
impl ProcessorStorageTrait for UserTransactionProcessor {
    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
