// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_object_utils::{CurrentObjectPK, ObjectAggregatedDataMapping};
use crate::{
    db::postgres::models::default_models::move_resources::MoveResource,
    schema::current_objects,
    utils::{
        database::{DbContext, DbPoolConnection},
        util::standardize_address,
    },
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

const DELETED_RESOURCE_OWNER_ADDRESS: &str = "Unknown";

#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct RawObject {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

pub trait ObjectConvertible {
    fn from_raw(raw_item: RawObject) -> Self;
}

#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct RawCurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
    pub block_timestamp: chrono::NaiveDateTime,
}

pub trait CurrentObjectConvertible {
    fn from_raw(raw_item: RawCurrentObject) -> Self;
}

#[derive(Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct CurrentObjectQuery {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
    pub untransferrable: bool,
}

impl RawObject {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_metadata_mapping: &ObjectAggregatedDataMapping,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, RawCurrentObject)>> {
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
                    transaction_version: txn_version,
                    write_set_change_index,
                    object_address: address.clone(),
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash.clone(),
                    guid_creation_num: object_core.guid_creation_num.clone(),
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    is_deleted: false,
                    untransferrable,
                    block_timestamp,
                },
                RawCurrentObject {
                    object_address: address,
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash,
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    last_guid_creation_num: object_core.guid_creation_num.clone(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    untransferrable,
                    block_timestamp,
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
        object_mapping: &AHashMap<CurrentObjectPK, RawCurrentObject>,
        db_context: &mut Option<DbContext<'_>>,
        block_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, RawCurrentObject)>> {
        if delete_resource.type_str == "0x1::object::ObjectGroup" {
            let resource = MoveResource::from_delete_resource(
                delete_resource,
                0, // Placeholder, this isn't used anyway
                txn_version,
                0, // Placeholder, this isn't used anyway
            );
            // Add a logid here to handle None conn
            if db_context.is_none() {
                // This is a hack to prevent the program for parquet
                Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        object_address: resource.address.clone(),
                        owner_address: DELETED_RESOURCE_OWNER_ADDRESS.to_string(),
                        state_key_hash: resource.state_key_hash.clone(),
                        guid_creation_num: BigDecimal::default(),
                        allow_ungated_transfer: false,
                        is_deleted: true,
                        untransferrable: false,
                        block_timestamp,
                    },
                    RawCurrentObject {
                        object_address: resource.address.clone(),
                        owner_address: DELETED_RESOURCE_OWNER_ADDRESS.to_string(),
                        state_key_hash: resource.state_key_hash.clone(),
                        last_guid_creation_num: BigDecimal::default(),
                        allow_ungated_transfer: false,
                        last_transaction_version: txn_version,
                        is_deleted: true,
                        untransferrable: false,
                        block_timestamp,
                    },
                )))
            } else {
                let previous_object = if let Some(object) = object_mapping.get(&resource.address) {
                    object.clone()
                } else if let Some(db_context) = db_context {
                    match Self::get_current_object(
                        &mut db_context.conn,
                        &resource.address,
                        db_context.query_retries,
                        db_context.query_retry_delay_ms,
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
                } else {
                    tracing::error!(
                        transaction_version = txn_version,
                        lookup_key = &resource.address,
                        "Connection to DB is missing. You may need to investigate",
                    );
                    return Ok(None);
                };
                Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        object_address: resource.address.clone(),
                        owner_address: previous_object.owner_address.clone(),
                        state_key_hash: resource.state_key_hash.clone(),
                        guid_creation_num: previous_object.last_guid_creation_num.clone(),
                        allow_ungated_transfer: previous_object.allow_ungated_transfer,
                        is_deleted: true,
                        untransferrable: previous_object.untransferrable,
                        block_timestamp,
                    },
                    RawCurrentObject {
                        object_address: resource.address,
                        owner_address: previous_object.owner_address.clone(),
                        state_key_hash: resource.state_key_hash,
                        last_guid_creation_num: previous_object.last_guid_creation_num.clone(),
                        allow_ungated_transfer: previous_object.allow_ungated_transfer,
                        last_transaction_version: txn_version,
                        is_deleted: true,
                        untransferrable: previous_object.untransferrable,
                        block_timestamp,
                    },
                )))
            }
        } else {
            Ok(None)
        }
    }

    /// This is actually not great because object owner can change. The best we can do now though.
    /// This will loop forever until we get the object from the db
    pub async fn get_current_object(
        conn: &mut DbPoolConnection<'_>,
        object_address: &str,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<RawCurrentObject> {
        let mut tried = 0;
        while tried < query_retries {
            tried += 1;
            match CurrentObjectQuery::get_by_address(object_address, conn).await {
                Ok(res) => {
                    return Ok(RawCurrentObject {
                        object_address: res.object_address,
                        owner_address: res.owner_address,
                        state_key_hash: res.state_key_hash,
                        allow_ungated_transfer: res.allow_ungated_transfer,
                        last_guid_creation_num: res.last_guid_creation_num,
                        last_transaction_version: res.last_transaction_version,
                        is_deleted: res.is_deleted,
                        untransferrable: res.untransferrable,
                        block_timestamp: chrono::NaiveDateTime::default(), // this won't be used
                    });
                },
                Err(_) => {
                    if tried < query_retries {
                        tokio::time::sleep(std::time::Duration::from_millis(query_retry_delay_ms))
                            .await;
                    }
                },
            }
        }
        Err(anyhow::anyhow!("Failed to get object owner"))
    }
}

impl CurrentObjectQuery {
    /// TODO: Change this to a KV store
    pub async fn get_by_address(
        object_address: &str,
        conn: &mut DbPoolConnection<'_>,
    ) -> diesel::QueryResult<Self> {
        current_objects::table
            .filter(current_objects::object_address.eq(object_address))
            .first::<Self>(conn)
            .await
    }
}
