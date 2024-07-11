// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        default_models::move_resources::MoveResource,
        object_models::{
            v2_object_utils::{CurrentObjectPK, ObjectAggregatedDataMapping},
            v2_objects::{CurrentObject, Object as DefaultObject},
        },
    },
    utils::{database::DbPoolConnection, util::standardize_address},
};
use ahash::AHashMap;
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::ToPrimitive;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct Object {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: u64,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for Object {
    const TABLE_NAME: &'static str = "objects";
}

impl HasVersion for Object {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for Object {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl Object {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_metadata_mapping: &ObjectAggregatedDataMapping,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        let address = standardize_address(&write_resource.address.to_string());
        if let Some(object_aggregated_metadata) = object_metadata_mapping.get(&address) {
            // do something
            let object_with_metadata = object_aggregated_metadata.object.clone();
            let object_core = object_with_metadata.object_core;

            let untransferrable = if object_aggregated_metadata.untransferable.as_ref().is_some() {
                true
            } else {
                !object_core.allow_ungated_transfer
            };
            Ok(Some((
                Self {
                    txn_version,
                    write_set_change_index,
                    object_address: address.clone(),
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash.clone(),
                    guid_creation_num: object_core.guid_creation_num.clone().to_u64().unwrap(),
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    is_deleted: false,
                    untransferrable,
                    block_timestamp,
                },
                CurrentObject {
                    object_address: address,
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash,
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    last_guid_creation_num: object_core.guid_creation_num.clone(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    untransferrable,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where the entire object is deleted
    /// TODO: We need to detect if an object is only partially deleted
    /// using KV store
    pub async fn from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_mapping: &AHashMap<CurrentObjectPK, CurrentObject>,
        conn: &mut DbPoolConnection<'_>,
        query_retries: u32,
        query_retry_delay_ms: u64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        if delete_resource.type_str == "0x1::object::ObjectGroup" {
            let resource = MoveResource::from_delete_resource(
                delete_resource,
                0, // Placeholder, this isn't used anyway
                txn_version,
                0, // Placeholder, this isn't used anyway
            );
            let previous_object = if let Some(object) = object_mapping.get(&resource.address) {
                object.clone()
            } else {
                match DefaultObject::get_current_object(
                    conn,
                    &resource.address,
                    query_retries,
                    query_retry_delay_ms,
                )
                .await
                {
                    Ok(object) => object,
                    Err(_) => {
                        tracing::error!(
                            transaction_version = txn_version,
                            lookup_key = &resource.address,
                            "Missing current_object for object_address: {}. You probably should backfill db.",
                            resource.address,
                        );
                        return Ok(None);
                    },
                }
            };
            Ok(Some((
                Self {
                    txn_version,
                    write_set_change_index,
                    object_address: resource.address.clone(),
                    owner_address: previous_object.owner_address.clone(),
                    state_key_hash: resource.state_key_hash.clone(),
                    guid_creation_num: previous_object
                        .last_guid_creation_num
                        .clone()
                        .to_u64()
                        .unwrap(),
                    allow_ungated_transfer: previous_object.allow_ungated_transfer,
                    is_deleted: true,
                    untransferrable: previous_object.untransferrable,
                    block_timestamp,
                },
                CurrentObject {
                    object_address: resource.address,
                    owner_address: previous_object.owner_address.clone(),
                    state_key_hash: resource.state_key_hash,
                    last_guid_creation_num: previous_object.last_guid_creation_num.clone(),
                    allow_ungated_transfer: previous_object.allow_ungated_transfer,
                    last_transaction_version: txn_version,
                    is_deleted: true,
                    untransferrable: previous_object.untransferrable,
                },
            )))
        } else {
            Ok(None)
        }
    }
}
