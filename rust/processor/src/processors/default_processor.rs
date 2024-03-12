// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    latest_version_tracker::{PartialBatch, VersionTrackerItem},
    models::default_models::{
        block_metadata_transactions::{BlockMetadataTransaction, BlockMetadataTransactionModel},
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        transactions::TransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::{QueryFragment, QueryId},
};

pub struct DefaultProcessor {
    db_writer: crate::db_writer::DbWriter,
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

    let txns_res = db_writer.send_in_chunks(
        "transactions",
        txns,
        insert_tranasctions_query,
        version_tracker_item.clone(),
    );
    let bmt_res = db_writer.send_in_chunks(
        "block_metadata_transactions",
        block_metadata_transactions,
        insert_block_metadata_transactions_query,
        version_tracker_item.clone(),
    );
    let wst_res = db_writer.send_in_chunks(
        "write_set_changes",
        wscs,
        insert_write_set_changes_query,
        version_tracker_item.clone(),
    );
    let mm_res = db_writer.send_in_chunks(
        "move_modules",
        move_modules,
        insert_move_modules_query,
        version_tracker_item.clone(),
    );

    let mr_res = db_writer.send_in_chunks(
        "move_resources",
        move_resources,
        insert_move_resources_query,
        version_tracker_item.clone(),
    );

    let ti_res = db_writer.send_in_chunks(
        "table_items",
        table_items,
        insert_table_items_query,
        version_tracker_item.clone(),
    );

    let cti_res = db_writer.send_in_chunks(
        "current_table_items",
        current_table_items,
        insert_current_table_items_query,
        version_tracker_item.clone(),
    );

    let tm_res = db_writer.send_in_chunks(
        "table_metadatas",
        table_metadata,
        insert_table_metadata_query,
        version_tracker_item,
    );

    tokio::join!(txns_res, bmt_res, wst_res, mm_res, mr_res, ti_res, cti_res, tm_res);
}

pub fn insert_tranasctions_query(
    items_to_insert: &[TransactionModel],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::transactions::dsl::*;

    diesel::insert_into(schema::transactions::table)
        .values(items_to_insert)
        .on_conflict(version)
        .do_update()
        .set((
            inserted_at.eq(excluded(inserted_at)),
            payload_type.eq(excluded(payload_type)),
        ))
}

pub fn insert_block_metadata_transactions_query(
    items_to_insert: &[BlockMetadataTransactionModel],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::block_metadata_transactions::dsl::*;

    diesel::insert_into(schema::block_metadata_transactions::table)
        .values(items_to_insert)
        .on_conflict(version)
        .do_nothing()
}

pub fn insert_write_set_changes_query(
    items_to_insert: &[WriteSetChangeModel],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::write_set_changes::dsl::*;
    diesel::insert_into(schema::write_set_changes::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, index))
        .do_nothing()
}

pub fn insert_move_modules_query(
    items_to_insert: &[MoveModule],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::move_modules::dsl::*;

    diesel::insert_into(schema::move_modules::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}

pub fn insert_move_resources_query(
    items_to_insert: &[MoveResource],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::move_resources::dsl::*;

    diesel::insert_into(schema::move_resources::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}

pub fn insert_table_items_query(
    items_to_insert: &[TableItem],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::table_items::dsl::*;

    diesel::insert_into(schema::table_items::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_nothing()
}

pub fn insert_current_table_items_query(
    items_to_insert: &[CurrentTableItem],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::{diesel::query_dsl::methods::FilterDsl, schema::current_table_items::dsl::*};

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
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

pub fn insert_table_metadata_query(
    items_to_insert: &[TableMetadata],
) -> impl QueryFragment<Pg> + QueryId + Sync + Send + '_ {
    use crate::schema::table_metadatas::dsl::*;

    diesel::insert_into(schema::table_metadatas::table)
        .values(items_to_insert)
        .on_conflict(handle)
        .do_nothing()
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
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            last_transaction_timestamp.clone(),
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

fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<crate::models::default_models::transactions::Transaction>,
    Vec<BlockMetadataTransaction>,
    Vec<WriteSetChangeModel>,
    (
        Vec<MoveModule>,
        Vec<MoveResource>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    ),
) {
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
            WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
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
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
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
}
