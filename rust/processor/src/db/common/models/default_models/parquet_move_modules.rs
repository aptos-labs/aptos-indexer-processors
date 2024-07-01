// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::utils::util::standardize_address;
use aptos_protos::transaction::v1::{
    DeleteModule, MoveModule as MoveModulePB, MoveModuleBytecode, WriteModule,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Serialize)]
pub struct MoveModule {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub name: String,
    pub address: String,
    pub bytecode: Option<Vec<u8>>,
    pub exposed_functions: Option<String>,
    pub friends: Option<String>,
    pub structs: Option<String>,
    pub is_deleted: bool,
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
