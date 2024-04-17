// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::object_models::{
        v2_object_utils::{ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata},
        v2_objects::{CurrentObject, Object},
    },
    schema,
    utils::{
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        util::standardize_address,
    },
    IndexerGrpcProcessorConfig,
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
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectsProcessorConfig {
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}
pub struct ObjectsProcessor {
    connection_pool: ArcDbPool,
    config: ObjectsProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl ObjectsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ObjectsProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
    ) -> Self {
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for ObjectsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "ObjectsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    (objects, current_objects): (&[Object], &[CurrentObject]),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let io = execute_in_chunks(
        conn.clone(),
        insert_objects_query,
        objects,
        get_config_table_chunk_size::<Object>("objects", per_table_chunk_sizes),
    );
    let co = execute_in_chunks(
        conn,
        insert_current_objects_query,
        current_objects,
        get_config_table_chunk_size::<CurrentObject>("current_objects", per_table_chunk_sizes),
    );
    let (io_res, co_res) = tokio::join!(io, co);
    for res in [io_res, co_res] {
        res?;
    }

    Ok(())
}

fn insert_objects_query(
    items_to_insert: Vec<Object>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::objects::dsl::*;
    (
        diesel::insert_into(schema::objects::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_update()
            .set((inserted_at.eq(excluded(inserted_at)),)),
        None,
    )
}

fn insert_current_objects_query(
    items_to_insert: Vec<CurrentObject>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_objects::dsl::*;
    (
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
            )),
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
        let query_retries = self.config.query_retries;
        let query_retry_delay_ms = self.config.query_retry_delay_ms;

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
                            query_retries,
                            query_retry_delay_ms,
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
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            (&all_objects, &all_current_objects),
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

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
