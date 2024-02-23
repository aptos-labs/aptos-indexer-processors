// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db_writer::execute_in_chunks,
    models::default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        transactions::TransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
    utils::database::get_config_table_chunk_size,
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use tracing::error;

pub struct DefaultProcessor {
    db_writer: crate::db_writer::DbWriter,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl std::fmt::Debug for DefaultProcessor {
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

impl DefaultProcessor {
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
    txns: Vec<TransactionModel>,
    block_metadata_transactions: Vec<BlockMetadataTransactionModel>,
    wscs: Vec<WriteSetChangeModel>,
    (move_modules, move_resources, table_items, current_table_items, table_metadata): (
        Vec<MoveModule>,
        Vec<MoveResource>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    ),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let query_sender = db_writer.query_sender.clone();
    let txns_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_transactions_query,
        txns,
        get_config_table_chunk_size::<TransactionModel>("transactions", per_table_chunk_sizes),
    );
    let bmt_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_block_metadata_transactions_query,
        block_metadata_transactions,
        get_config_table_chunk_size::<BlockMetadataTransactionModel>(
            "block_metadata_transactions",
            per_table_chunk_sizes,
        ),
    );
    let wst_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_write_set_changes_query,
        wscs,
        get_config_table_chunk_size::<WriteSetChangeModel>(
            "write_set_changes",
            per_table_chunk_sizes,
        ),
    );
    let mm_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_move_modules_query,
        move_modules,
        get_config_table_chunk_size::<MoveModule>("move_modules", per_table_chunk_sizes),
    );

    let mr_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_move_resources_query,
        move_resources,
        get_config_table_chunk_size::<MoveResource>("move_resources", per_table_chunk_sizes),
    );

    let ti_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_table_items_query,
        table_items,
        get_config_table_chunk_size::<TableItem>("table_items", per_table_chunk_sizes),
    );

    let cti_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_current_table_items_query,
        current_table_items,
        get_config_table_chunk_size::<CurrentTableItem>(
            "current_table_items",
            per_table_chunk_sizes,
        ),
    );

    let tm_res = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender,
        insert_table_metadata_query,
        table_metadata,
        get_config_table_chunk_size::<TableMetadata>("table_metadatas", per_table_chunk_sizes),
    );

    tokio::join!(txns_res, bmt_res, wst_res, mm_res, mr_res, ti_res, cti_res, tm_res);

    Ok(())
}

fn insert_transactions_query(
    items_to_insert: &[TransactionModel],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::transactions::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::transactions::table)
                .values(items_to_insert)
                .on_conflict(version)
                .do_update()
                .set((
                    inserted_at.eq(excluded(inserted_at)),
                    payload_type.eq(excluded(payload_type)),
                )),
        ),
        None,
    )
}

fn insert_block_metadata_transactions_query(
    items_to_insert: &[BlockMetadataTransactionModel],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::block_metadata_transactions::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::block_metadata_transactions::table)
                .values(items_to_insert)
                .on_conflict(version)
                .do_nothing(),
        ),
        None,
    )
}

fn insert_write_set_changes_query(
    items_to_insert: &[WriteSetChangeModel],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::write_set_changes::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::write_set_changes::table)
                .values(items_to_insert)
                .on_conflict((transaction_version, index))
                .do_nothing(),
        ),
        None,
    )
}

fn insert_move_modules_query(
    items_to_insert: &[MoveModule],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::move_modules::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::move_modules::table)
                .values(items_to_insert)
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
        ),
        None,
    )
}

fn insert_move_resources_query(
    items_to_insert: &[MoveResource],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::move_resources::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::move_resources::table)
                .values(items_to_insert)
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
        ),
        None,
    )
}

fn insert_table_items_query(
    items_to_insert: &[TableItem],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::table_items::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::table_items::table)
                .values(items_to_insert)
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
        ),
        None,
    )
}

fn insert_current_table_items_query(
    items_to_insert: &[CurrentTableItem],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::current_table_items::dsl::*;

    (
        Box::new(
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
                ))
        ),
        Some(" WHERE current_table_items.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_table_metadata_query(
    items_to_insert: &[TableMetadata],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::table_metadatas::dsl::*;

    (
        Box::new(
            diesel::insert_into(schema::table_metadatas::table)
                .values(items_to_insert)
                .on_conflict(handle)
                .do_nothing(),
        ),
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
            txns,
            block_metadata_transactions,
            write_set_changes,
            (move_modules, move_resources, table_items, current_table_items, table_metadata),
        ) = tokio::task::spawn_blocking(move || {
            let (txns, block_metadata_txns, write_set_changes, wsc_details) =
                TransactionModel::from_transactions(&transactions);
            let mut block_metadata_transactions = vec![];
            for block_metadata_txn in block_metadata_txns {
                block_metadata_transactions.push(block_metadata_txn.clone());
            }
            let mut move_modules = vec![];
            let mut move_resources = vec![];
            let mut table_items = vec![];
            let mut current_table_items = AHashMap::new();
            let mut table_metadata = AHashMap::new();
            for detail in wsc_details {
                match detail {
                    WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
                    WriteSetChangeDetail::Resource(resource) => {
                        move_resources.push(resource.clone())
                    },
                    WriteSetChangeDetail::Table(item, current_item, metadata) => {
                        table_items.push(item.clone());
                        current_table_items.insert(
                            (
                                current_item.table_handle.clone(),
                                current_item.key_hash.clone(),
                            ),
                            current_item.clone(),
                        );
                        if let Some(meta) = metadata {
                            table_metadata.insert(meta.handle.clone(), meta.clone());
                        }
                    },
                }
            }

            // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
            let mut current_table_items = current_table_items
                .into_values()
                .collect::<Vec<CurrentTableItem>>();
            let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
            // Sort by PK
            current_table_items.sort_by(|a, b| {
                (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash))
            });
            table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

            (
                txns,
                block_metadata_transactions,
                write_set_changes,
                (
                    move_modules,
                    move_resources,
                    table_items,
                    current_table_items,
                    table_metadata,
                ),
            )
        })
        .await
        .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            txns,
            block_metadata_transactions,
            write_set_changes,
            (
                move_modules,
                move_resources,
                table_items,
                current_table_items,
                table_metadata,
            ),
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
            },
        }
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
