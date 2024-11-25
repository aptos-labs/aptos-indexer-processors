// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::{
        common::models::default_models::{
            raw_current_table_items::{CurrentTableItemConvertible, RawCurrentTableItem},
            raw_table_items::{RawTableItem, TableItemConvertible},
        },
        postgres::models::default_models::{
            block_metadata_transactions::BlockMetadataTransactionModel,
            move_tables::{CurrentTableItem, TableItem, TableMetadata},
        },
    },
    gap_detectors::ProcessingResult,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    },
    worker::TableFlags,
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, Transaction,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tokio::join;
use tracing::error;

pub struct DefaultProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    deprecated_tables: TableFlags,
}

impl DefaultProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        per_table_chunk_sizes: AHashMap<String, usize>,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
            deprecated_tables,
        }
    }
}

impl Debug for DefaultProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    block_metadata_transactions: &[BlockMetadataTransactionModel],
    (table_items, current_table_items, table_metadata): (
        &[TableItem],
        &[CurrentTableItem],
        &[TableMetadata],
    ),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let bmt_res = execute_in_chunks(
        conn.clone(),
        insert_block_metadata_transactions_query,
        block_metadata_transactions,
        get_config_table_chunk_size::<BlockMetadataTransactionModel>(
            "block_metadata_transactions",
            per_table_chunk_sizes,
        ),
    );

    let ti_res = execute_in_chunks(
        conn.clone(),
        insert_table_items_query,
        table_items,
        get_config_table_chunk_size::<TableItem>("table_items", per_table_chunk_sizes),
    );

    let cti_res = execute_in_chunks(
        conn.clone(),
        insert_current_table_items_query,
        current_table_items,
        get_config_table_chunk_size::<CurrentTableItem>(
            "current_table_items",
            per_table_chunk_sizes,
        ),
    );

    let tm_res = execute_in_chunks(
        conn.clone(),
        insert_table_metadata_query,
        table_metadata,
        get_config_table_chunk_size::<TableMetadata>("table_metadatas", per_table_chunk_sizes),
    );

    let (bmt_res, ti_res, cti_res, tm_res) = join!(bmt_res, ti_res, cti_res, tm_res);

    for res in [bmt_res, ti_res, cti_res, tm_res] {
        res?;
    }

    Ok(())
}

pub fn insert_block_metadata_transactions_query(
    items_to_insert: Vec<BlockMetadataTransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::block_metadata_transactions::dsl::*;

    (
        diesel::insert_into(schema::block_metadata_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_nothing(),
        None,
    )
}

pub fn insert_table_items_query(
    items_to_insert: Vec<TableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::table_items::dsl::*;

    (
        diesel::insert_into(schema::table_items::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

pub fn insert_current_table_items_query(
    items_to_insert: Vec<CurrentTableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_table_items::dsl::*;

    (
        diesel::insert_into(schema::current_table_items::table)
            .values(items_to_insert)
            .on_conflict((table_handle, key_hash))
            .do_update()
            .set((
                key.eq(excluded(key)),
                decoded_key.eq(excluded(decoded_key)),
                decoded_value.eq(excluded(decoded_value)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_table_items.last_transaction_version <= excluded.last_transaction_version "),
    )
}

pub fn insert_table_metadata_query(
    items_to_insert: Vec<TableMetadata>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::table_metadatas::dsl::*;

    (
        diesel::insert_into(schema::table_metadatas::table)
            .values(items_to_insert)
            .on_conflict(handle)
            .do_nothing(),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for DefaultProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::DefaultProcessor.into()
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

        let (
            block_metadata_transactions,
            raw_table_items,
            raw_current_table_items,
            mut table_metadata,
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let mut postgres_table_items: Vec<TableItem> =
            raw_table_items.iter().map(TableItem::from_raw).collect();

        let postgres_current_table_items: Vec<CurrentTableItem> = raw_current_table_items
            .iter()
            .map(CurrentTableItem::from_raw)
            .collect();

        let flags = self.deprecated_tables;
        // TODO: remove this, since we are not going to deprecate this anytime soon?
        if flags.contains(TableFlags::TABLE_ITEMS) {
            postgres_table_items.clear();
        }
        // TODO: migrate to Parquet
        if flags.contains(TableFlags::TABLE_METADATAS) {
            table_metadata.clear();
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &block_metadata_transactions,
            (
                &postgres_table_items,
                &postgres_current_table_items,
                &table_metadata,
            ),
            &self.per_table_chunk_sizes,
        )
        .await;

        // These vectors could be super large and take a lot of time to drop, move to background to
        // make it faster.
        tokio::task::spawn(async move {
            drop(block_metadata_transactions);
            drop(postgres_table_items);
            drop(postgres_current_table_items);
            drop(table_metadata);
        });

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version,
                    end_version,
                    processing_duration_in_secs,
                    db_insertion_duration_in_secs,
                    last_transaction_timestamp,
                },
            )),
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

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

// TODO: we can further optimize this by passing in a flag to selectively parse only the required data (e.g. table_items for parquet)
/// Processes a list of transactions and extracts relevant data into different models.
///
/// This function iterates over a list of transactions, extracting block metadata transactions,
/// table items, current table items, and table metadata. It handles different types of
/// transactions and write set changes, converting them into appropriate models. The function
/// also sorts the extracted data to avoid PostgreSQL deadlocks during multi-threaded database
/// writes.
///
/// # Arguments
///
/// * `transactions` - A vector of `Transaction` objects to be processed.
///
/// # Returns
///
/// A tuple containing:
/// * `Vec<BlockMetadataTransactionModel>` - A vector of block metadata transaction models.
/// * `Vec<RawTableItem>` - A vector of table items.
/// * `Vec<CurrentTableItem>` - A vector of current table items, sorted by primary key.
/// * `Vec<TableMetadata>` - A vector of table metadata, sorted by primary key.
pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<BlockMetadataTransactionModel>,
    Vec<RawTableItem>,
    Vec<RawCurrentTableItem>,
    Vec<TableMetadata>,
) {
    let mut block_metadata_transactions = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata = AHashMap::new();

    for transaction in transactions {
        let version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let epoch = transaction.epoch as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");

        #[allow(deprecated)]
        let block_timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
            .expect("Txn Timestamp is invalid!");
        let txn_data = match transaction.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["Transaction"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };
        if let TxnData::BlockMetadata(block_metadata_txn) = txn_data {
            let bmt = BlockMetadataTransactionModel::from_bmt_transaction(
                block_metadata_txn,
                version,
                block_height,
                epoch,
                timestamp,
            );
            block_metadata_transactions.push(bmt);
        }

        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            match wsc
                .change
                .as_ref()
                .expect("WriteSetChange must have a change")
            {
                WriteSetChangeEnum::WriteTableItem(inner) => {
                    let (ti, cti) = RawTableItem::from_write_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    table_items.push(ti);
                    current_table_items.insert(
                        (cti.table_handle.clone(), cti.key_hash.clone()),
                        cti.clone(),
                    );
                    table_metadata.insert(
                        cti.table_handle.clone(),
                        TableMetadata::from_write_table_item(inner),
                    );
                },
                WriteSetChangeEnum::DeleteTableItem(inner) => {
                    let (ti, cti) = RawTableItem::from_delete_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    table_items.push(ti);
                    current_table_items
                        .insert((cti.table_handle.clone(), cti.key_hash.clone()), cti);
                },
                _ => {},
            };
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<RawCurrentTableItem>>();
    let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

    (
        block_metadata_transactions,
        table_items,
        current_table_items,
        table_metadata,
    )
}
