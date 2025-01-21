// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::object_models::raw_v2_objects::{
        CurrentObjectConvertible, ObjectConvertible, RawCurrentObject, RawObject,
    },
    schema::{current_objects, objects},
};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = objects)]
pub struct Object {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_deleted: bool,
    pub untransferrable: bool,
}

impl ObjectConvertible for Object {
    fn from_raw(raw_item: RawObject) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            write_set_change_index: raw_item.write_set_change_index,
            object_address: raw_item.object_address,
            owner_address: raw_item.owner_address,
            state_key_hash: raw_item.state_key_hash,
            guid_creation_num: raw_item.guid_creation_num,
            allow_ungated_transfer: raw_item.allow_ungated_transfer,
            is_deleted: raw_item.is_deleted,
            untransferrable: raw_item.untransferrable,
        }
    }
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct CurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
}

impl CurrentObjectConvertible for CurrentObject {
    fn from_raw(raw_item: RawCurrentObject) -> Self {
        Self {
            object_address: raw_item.object_address,
            owner_address: raw_item.owner_address,
            state_key_hash: raw_item.state_key_hash,
            allow_ungated_transfer: raw_item.allow_ungated_transfer,
            last_guid_creation_num: raw_item.last_guid_creation_num,
            last_transaction_version: raw_item.last_transaction_version,
            is_deleted: raw_item.is_deleted,
            untransferrable: raw_item.untransferrable,
        }
    }
}
