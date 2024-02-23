// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db_writer::execute_in_chunks,
    models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::FungibleAssetActivity,
            v2_fungible_asset_utils::FungibleAssetStore,
        },
        object_models::{
            v2_object_utils::{
                ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
            },
            v2_objects::{CurrentObject, Object},
        },
        token_v2_models::v2_token_utils::TokenV2,
    },
    schema,
    utils::{database::get_config_table_chunk_size, util::standardize_address},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use tracing::error;

pub struct ObjectsProcessor {
    db_writer: crate::db_writer::DbWriter,
    per_table_chunk_sizes: AHashMap<String, usize>,
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
    (objects, current_objects): (Vec<Object>, Vec<CurrentObject>),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let query_sender = db_writer.query_sender.clone();
    let io = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender.clone(),
        insert_objects_query,
        objects,
        get_config_table_chunk_size::<Object>("objects", per_table_chunk_sizes),
    );
    let co = execute_in_chunks(
        &"TABLE_NAME_PLACEHOLDER",
        query_sender,
        insert_current_objects_query,
        current_objects,
        get_config_table_chunk_size::<FungibleAssetActivity>(
            "current_objects",
            per_table_chunk_sizes,
        ),
    );
    tokio::join!(io, co);

    Ok(())
}

fn insert_objects_query(
    items_to_insert: &[Object],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::objects::dsl::*;
    (
        Box::new(
            diesel::insert_into(schema::objects::table)
                .values(items_to_insert)
                .on_conflict((transaction_version, write_set_change_index))
                .do_update()
                .set((
                    inserted_at.eq(excluded(inserted_at)),
                    is_token.eq(excluded(is_token)),
                    is_fungible_asset.eq(excluded(is_fungible_asset)),
                )),
        ),
        None,
    )
}

fn insert_current_objects_query(
    items_to_insert: &[CurrentObject],
) -> (
    Box<impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send>,
    Option<&'static str>,
) {
    use schema::current_objects::dsl::*;
    (
        Box::new(
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
                    is_token.eq(excluded(is_token)),
                    is_fungible_asset.eq(excluded(is_fungible_asset)),
                ))
                .target,
        ),
        Some(
            " WHERE current_objects.last_transaction_version <= excluded.last_transaction_version ",
        ),
    )
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

            // Second pass to get all other structs related to the object
            for wsc in changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());

                    // Find structs related to object
                    if let Some(aggregated_data) = object_metadata_helper.get_mut(&address) {
                        if let Some(token) = TokenV2::from_write_resource(wr, txn_version).unwrap()
                        {
                            // Object is a token if it has 0x4::token::Token struct
                            aggregated_data.token = Some(token);
                        }
                        if let Some(fungible_asset_store) =
                            FungibleAssetStore::from_write_resource(wr, txn_version).unwrap()
                        {
                            // Object is a fungible asset if it has a 0x1::fungible_asset::FungibleAssetStore
                            aggregated_data.fungible_asset_store = Some(fungible_asset_store);
                        }
                    }
                }
            }

            // Third pass to construct the object data
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

        let tx_result = insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            (all_objects, all_current_objects),
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
