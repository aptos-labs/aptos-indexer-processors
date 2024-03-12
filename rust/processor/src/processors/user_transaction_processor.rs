// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    latest_version_tracker::{PartialBatch, VersionTrackerItem},
    models::user_transactions_models::{
        signatures::Signature, user_transactions::UserTransactionModel,
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{query_builder::QueryFragment, upsert::excluded};

pub struct UserTransactionProcessor {
    db_writer: crate::db_writer::DbWriter,
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
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    user_transactions: Vec<UserTransactionModel>,
    signatures: Vec<Signature>,
) {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Finished parsing, sending to DB",
    );
    let version_tracker_item = VersionTrackerItem::PartialBatch(PartialBatch {
        start_version,
        end_version,
        last_transaction_timestamp,
    });
    let ut = db_writer.send_in_chunks(
        "user_transactions",
        user_transactions,
        insert_user_transactions_query,
        version_tracker_item.clone(),
    );
    let is = db_writer.send_in_chunks(
        "signatures",
        signatures,
        insert_signatures_query,
        version_tracker_item,
    );

    tokio::join!(ut, is);
}

pub fn insert_user_transactions_query(
    items_to_insert: &[UserTransactionModel],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::user_transactions::dsl::*;

    diesel::insert_into(crate::schema::user_transactions::table)
        .values(items_to_insert)
        .on_conflict(version)
        .do_update()
        .set((
            expiration_timestamp_secs.eq(excluded(expiration_timestamp_secs)),
            inserted_at.eq(excluded(inserted_at)),
        ))
}

pub fn insert_signatures_query(
    items_to_insert: &[Signature],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::signatures::dsl::*;

    diesel::insert_into(crate::schema::signatures::table)
        .values(items_to_insert)
        .on_conflict((
            transaction_version,
            multi_agent_index,
            multi_sig_index,
            is_sender_primary,
        ))
        .do_nothing()
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
                },
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

        insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            last_transaction_timestamp.clone(),
            user_transactions,
            signatures,
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
