// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_object_utils::{CurrentObjectPK, ObjectAggregatedDataMapping};
use crate::{
    db::postgres::models::default_models::move_resources::MoveResource,
    schema::{current_objects, objects},
    utils::{database::DbPoolConnection, util::standardize_address},
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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
}

pub trait ObjectConvertible {
    fn from_raw(raw_item: RawObject) -> Self;
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
pub struct RawCurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub untransferrable: bool,
}

pub trait CurrentObjectConvertible {
    fn from_raw(raw_item: RawCurrentObject) -> Self;
}
