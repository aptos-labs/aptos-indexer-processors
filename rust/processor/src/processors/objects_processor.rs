// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    models::object_models::{
        v2_object_utils::{ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata},
        v2_objects::{CurrentObject, Object},
    },
    schema,
    utils::util::standardize_address,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{
    pg::upsert::excluded, query_builder::QueryFragment, query_dsl::filter_dsl::FilterDsl,
};

pub struct ObjectsProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl std::fmt::Debug for ObjectsProcessor {
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

impl ObjectsProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

pub fn insert_objects_query(
    items_to_insert: &[Object],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::objects::dsl::*;

    diesel::insert_into(schema::objects::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, write_set_change_index))
        .do_update()
        .set(inserted_at.eq(excluded(inserted_at)))
}

pub fn insert_current_objects_query(
    items_to_insert: &[CurrentObject],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::current_objects::dsl::*;

    diesel::insert_into(schema::current_objects::table)
        .values(items_to_insert)
        .on_conflict(object_address)
        .do_update()
        .set((
            owner_address.eq(excluded(owner_address)),
            state_key_hash.eq(excluded(state_key_hash)),
            allow_ungated_transfer.eq(excluded(allow_ungated_transfer)),
            last_guid_creation_num.eq(excluded(last_guid_creation_num)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            is_deleted.eq(excluded(is_deleted)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

#[async_trait]
impl ProcessorTrait for ObjectsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ObjectsProcessor.into()
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

        let mut conn = self.get_conn().await;

        // Moving object handling here because we need a single object
        // map through transactions for lookups
        let mut all_objects = vec![];
        let mut all_current_objects = AHashMap::new();
        let mut object_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

        for txn in &transactions {
            let txn_version = txn.version as i64;
            let changes = &txn
                .info
                .as_ref()
                .unwrap_or_else(|| {
                    panic!(
                        "Transaction info doesn't exist! Transaction {}",
                        txn_version
                    )
                })
                .changes;

            // First pass to get all the object cores
            for wsc in changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(object_with_metadata) =
                        ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                    {
                        // Object core is the first struct that we need to get
                        object_metadata_helper.insert(address.clone(), ObjectAggregatedData {
                            object: object_with_metadata,
                            token: None,
                            fungible_asset_store: None,
                            // The following structs are unused in this processor
                            fungible_asset_metadata: None,
                            aptos_collection: None,
                            fixed_supply: None,
                            unlimited_supply: None,
                            concurrent_supply: None,
                            property_map: None,
                            transfer_events: vec![],
                            fungible_asset_supply: None,
                            token_identifier: None,
                        });
                    }
                }
            }

            // Second pass to construct the object data
            for (index, wsc) in changes.iter().enumerate() {
                let index: i64 = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(inner) => {
                        if let Some((object, current_object)) = &Object::from_write_resource(
                            inner,
                            txn_version,
                            index,
                            &object_metadata_helper,
                        )
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    Change::DeleteResource(inner) => {
                        // Passing all_current_objects into the function so that we can get the owner of the deleted
                        // resource if it was handled in the same batch
                        if let Some((object, current_object)) = Object::from_delete_resource(
                            inner,
                            txn_version,
                            index,
                            &all_current_objects,
                            &mut conn,
                        )
                        .await
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    _ => {},
                };
            }
        }

        // Sort by PK
        let mut all_current_objects = all_current_objects
            .into_values()
            .collect::<Vec<CurrentObject>>();
        all_current_objects.sort_by(|a, b| a.object_address.cmp(&b.object_address));

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Finished parsing, sending to DB",
        );

        let db_writer = self.db_writer();
        let io = db_writer.send_in_chunks("objects", all_objects, insert_objects_query);
        let co = db_writer.send_in_chunks(
            "current_objects",
            all_current_objects,
            insert_current_objects_query,
        );
        tokio::join!(io, co);

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
