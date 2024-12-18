// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::object_models::raw_v2_objects::{
        CurrentObjectConvertible, ObjectConvertible, RawCurrentObject, RawObject,
    },
};
use allocative_derive::Allocative;
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
    pub guid_creation_num: u64, //  BigDecimal,
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

impl ObjectConvertible for Object {
    fn from_raw(raw_item: RawObject) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            object_address: raw_item.object_address,
            owner_address: raw_item.owner_address,
            state_key_hash: raw_item.state_key_hash,
            guid_creation_num: raw_item.guid_creation_num.to_u64().unwrap(),
            allow_ungated_transfer: raw_item.allow_ungated_transfer,
            is_deleted: raw_item.is_deleted,
            untransferrable: raw_item.untransferrable,
            block_timestamp: raw_item.block_timestamp,
        }
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: u64, //  BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for CurrentObject {
    const TABLE_NAME: &'static str = "objects";
}

impl HasVersion for CurrentObject {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentObject {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl CurrentObjectConvertible for CurrentObject {
    fn from_raw(raw_item: RawCurrentObject) -> Self {
        Self {
            object_address: raw_item.object_address,
            owner_address: raw_item.owner_address,
            state_key_hash: raw_item.state_key_hash,
            allow_ungated_transfer: raw_item.allow_ungated_transfer,
            last_guid_creation_num: raw_item.last_guid_creation_num.to_u64().unwrap(),
            last_transaction_version: raw_item.last_transaction_version,
            is_deleted: raw_item.is_deleted,
            untransferrable: raw_item.untransferrable,
            block_timestamp: raw_item.block_timestamp,
        }
    }
}
