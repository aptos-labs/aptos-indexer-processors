// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    utils::util::standardize_address,
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{
    DeleteModule, MoveModule as MoveModulePB, MoveModuleBytecode, WriteModule,
};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct MoveModule {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub block_height: i64,
    pub name: String,
    pub address: String,
    pub bytecode: Option<Vec<u8>>,
    pub exposed_functions: Option<String>,
    pub friends: Option<String>,
    pub structs: Option<String>,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for MoveModule {
    const TABLE_NAME: &'static str = "move_modules";
}

impl HasVersion for MoveModule {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for MoveModule {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MoveModuleByteCodeParsed {
    pub address: String,
    pub name: String,
    pub bytecode: Vec<u8>,
    pub exposed_functions: String,
    pub friends: String,
    pub structs: String,
}

impl MoveModule {
    pub fn from_write_module(
        write_module: &WriteModule,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        let parsed_data = Self::convert_move_module_bytecode(write_module.data.as_ref().unwrap());
        Self {
            txn_version,
            write_set_change_index,
            block_height,
            // TODO: remove the useless_asref lint when new clippy nighly is released.
            #[allow(clippy::useless_asref)]
            name: parsed_data
                .clone()
                .map(|d| d.name.clone())
                .unwrap_or_default(),
            address: standardize_address(&write_module.address.to_string()),
            bytecode: parsed_data.clone().map(|d| d.bytecode.clone()),
            exposed_functions: parsed_data.clone().map(|d| d.exposed_functions.clone()),
            friends: parsed_data.clone().map(|d| d.friends.clone()),
            structs: parsed_data.map(|d| d.structs.clone()),
            is_deleted: false,
            block_timestamp,
        }
    }

    pub fn from_delete_module(
        delete_module: &DeleteModule,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        Self {
            txn_version,
            block_height,
            write_set_change_index,
            // TODO: remove the useless_asref lint when new clippy nighly is released.
            #[allow(clippy::useless_asref)]
            name: delete_module
                .module
                .clone()
                .map(|d| d.name.clone())
                .unwrap_or_default(),
            address: standardize_address(&delete_module.address.to_string()),
            bytecode: None,
            exposed_functions: None,
            friends: None,
            structs: None,
            is_deleted: true,
            block_timestamp,
        }
    }

    pub fn convert_move_module_bytecode(
        mmb: &MoveModuleBytecode,
    ) -> Option<MoveModuleByteCodeParsed> {
        mmb.abi
            .as_ref()
            .map(|abi| Self::convert_move_module(abi, mmb.bytecode.clone()))
    }

    pub fn convert_move_module(
        move_module: &MoveModulePB,
        bytecode: Vec<u8>,
    ) -> MoveModuleByteCodeParsed {
        MoveModuleByteCodeParsed {
            address: standardize_address(&move_module.address.to_string()),
            name: move_module.name.clone(),
            bytecode,
            exposed_functions: move_module
                .exposed_functions
                .iter()
                .map(|move_func| serde_json::to_value(move_func).unwrap())
                .map(|value| canonical_json::to_string(&value).unwrap())
                .collect(),
            friends: move_module
                .friends
                .iter()
                .map(|move_module_id| serde_json::to_value(move_module_id).unwrap())
                .map(|value| canonical_json::to_string(&value).unwrap())
                .collect(),
            structs: move_module
                .structs
                .iter()
                .map(|move_struct| serde_json::to_value(move_struct).unwrap())
                .map(|value| canonical_json::to_string(&value).unwrap())
                .collect(),
        }
    }
}
