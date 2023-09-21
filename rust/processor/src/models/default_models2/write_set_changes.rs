// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

extern crate proc_macro;

use crate::{models::default_models::write_set_changes::{WriteSetChangeModel, WriteSetChangeDetail}, processors::default_processor2::PGInsertable};
use serde::{Deserialize, Serialize};
use my_macros::PGInsertable;

pub enum WriteSetChangeCockroach {
    Resource(WriteSetChangeResource),
    Module(WriteSetChangeModule),
    Table(WriteSetChangeTable),
}

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct WriteSetChangeResource {
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    pub index: i64,
    pub is_deleted: bool,
    pub name: String,
    pub module: String,
    pub generic_type_params: Option<serde_json::Value>,
    pub data:  Option<serde_json::Value>,
    pub state_key_hash: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct WriteSetChangeModule {
    pub transaction_version: i64,
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
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    pub index: i64,
    pub is_deleted: bool,
    pub table_handle: String,
    pub key: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub key_type: Option<String>,
    pub value_type: Option<String>,
}

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
                    transaction_version,
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: move_module.is_deleted,
                    bytecode: move_module.bytecode.clone(),
                    friends: Some(serde_json::to_value(&move_module.friends).unwrap()),
                    exposed_functions: Some(serde_json::to_value(&move_module.exposed_functions).unwrap()),
                    structs: Some(serde_json::to_value(&move_module.structs).unwrap()),
                })
            }
            WriteSetChangeDetail::Resource(move_resource) => {
                WriteSetChangeCockroach::Resource(WriteSetChangeResource {
                    transaction_version,
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: move_resource.is_deleted,
                    name: move_resource.name.clone(),
                    module: move_resource.module.clone(),
                    generic_type_params: Some(serde_json::to_value(&move_resource.generic_type_params).unwrap()),
                    data: Some(serde_json::to_value(&move_resource.data).unwrap()),
                    state_key_hash: move_resource.state_key_hash.clone(),
                })
            }
            WriteSetChangeDetail::Table(table_item, _, table_metadata) => {
                WriteSetChangeCockroach::Table(WriteSetChangeTable {
                    transaction_version,
                    transaction_block_height,
                    hash,
                    write_set_change_type,
                    address,
                    index,
                    is_deleted: table_item.is_deleted,
                    table_handle: table_item.table_handle.clone(),
                    key: table_item.key.clone(),
                    decoded_key: serde_json::to_value(&table_item.decoded_key).unwrap(),
                    decoded_value: Some(serde_json::to_value(&table_item.decoded_value).unwrap()),
                    key_type: table_metadata.as_ref().map(|metadata| metadata.key_type.clone()),
                    value_type: table_metadata.as_ref().map(|metadata| metadata.value_type.clone()),
                })
            }
        };
    
        wsc
    }
    
}
