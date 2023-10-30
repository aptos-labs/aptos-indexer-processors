// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

extern crate proc_macro;

use crate::{
    models::default_models::write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    processors::default_processor2::PGInsertable,
    utils::util::truncate_str,
};
use my_macros::PGInsertable;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub enum WriteSetChangeCockroach {
    Resource(WriteSetChangeResource),
    Module(WriteSetChangeModule),
    Table(WriteSetChangeTable),
}

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct WriteSetChangeResource {
    pub transaction_version: String,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    pub index: i64,
    pub is_deleted: bool,
    pub name: String,
    pub module: String,
    pub generic_type_params: Option<serde_json::Value>,
    pub data: Option<serde_json::Value>,
    pub state_key_hash: String,
    pub data_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct WriteSetChangeModule {
    pub transaction_version: String,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    pub index: i64,
    pub is_deleted: bool,
    pub bytecode: Option<Vec<u8>>,
    pub friends: Option<serde_json::Value>,
    pub exposed_functions: Option<serde_json::Value>,
    pub structs: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct WriteSetChangeTable {
    pub transaction_version: String,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    pub index: i64,
    pub is_deleted: bool,
    pub table_handle: String,
    pub key: String,
    pub data: Option<serde_json::Value>,
}

// p99 currently is 296 so using 300 as a safe max length
const WRITE_RESOURCE_TYPE_MAX_LENGTH: usize = 300;

impl WriteSetChangeCockroach {
    pub fn from_wscs(
        write_set_changes: Vec<WriteSetChangeModel>,
        write_set_change_details: Vec<WriteSetChangeDetail>,
    ) -> Vec<Self> {
        write_set_changes
            .iter()
            .zip(write_set_change_details.iter())
            .map(|(write_set_change, write_set_change_detail)| {
                Self::from_wsc(write_set_change, write_set_change_detail)
            })
            .collect()
    }

    fn from_wsc(
        write_set_change: &WriteSetChangeModel,
        write_set_change_detail: &WriteSetChangeDetail,
    ) -> WriteSetChangeCockroach {
        let transaction_version = write_set_change.transaction_version;
        let index = write_set_change.index;
        let hash = write_set_change.hash.clone();
        let transaction_block_height = write_set_change.transaction_block_height;
        let write_set_change_type = write_set_change.type_.to_string();
        let address = write_set_change.address.clone();

        let wsc = match write_set_change_detail {
            WriteSetChangeDetail::Module(move_module) => {
                WriteSetChangeCockroach::Module(WriteSetChangeModule {
                    transaction_version: transaction_version.to_string(),
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: move_module.is_deleted,
                    bytecode: move_module.bytecode.clone(),
                    friends: Some(serde_json::to_value(&move_module.friends).unwrap()),
                    exposed_functions: Some(
                        serde_json::to_value(&move_module.exposed_functions).unwrap(),
                    ),
                    structs: Some(serde_json::to_value(&move_module.structs).unwrap()),
                })
            },
            WriteSetChangeDetail::Resource(move_resource) => {
                WriteSetChangeCockroach::Resource(WriteSetChangeResource {
                    transaction_version: transaction_version.to_string(),
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: move_resource.is_deleted,
                    name: move_resource.name.clone(),
                    module: move_resource.module.clone(),
                    generic_type_params: Some(
                        serde_json::to_value(&move_resource.generic_type_params).unwrap(),
                    ),
                    data: Some(serde_json::to_value(&move_resource.data).unwrap()),
                    state_key_hash: move_resource.state_key_hash.clone(),
                    data_type: Some(truncate_str(
                        &move_resource.type_.clone(),
                        WRITE_RESOURCE_TYPE_MAX_LENGTH,
                    )),
                })
            },
            WriteSetChangeDetail::Table(table_item, _, table_metadata) => {
                let decoded_key = serde_json::to_value(&table_item.decoded_key).unwrap();
                let decoded_value = Some(serde_json::to_value(&table_item.decoded_value).unwrap());
                let key_type = table_metadata
                    .as_ref()
                    .map(|metadata| metadata.key_type.clone());
                let value_type = table_metadata
                    .as_ref()
                    .map(|metadata| metadata.value_type.clone());
                let data: Option<serde_json::Value> =
                    match (decoded_key, decoded_value, key_type, value_type) {
                        (decoded_key, Some(decoded_value), key_type, value_type) => Some(json!({
                            "decoded_key": decoded_key,
                            "decoded_value": decoded_value,
                            "key_type": key_type,
                            "value_type": value_type,
                        })),
                        _ => None,
                    };

                WriteSetChangeCockroach::Table(WriteSetChangeTable {
                    transaction_version: transaction_version.to_string(),
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: table_item.is_deleted,
                    table_handle: table_item.table_handle.clone(),
                    key: table_item.key.clone(),
                    data,
                })
            },
        };

        wsc
    }
}
