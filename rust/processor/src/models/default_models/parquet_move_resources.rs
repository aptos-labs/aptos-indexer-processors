// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::transactions::Transaction;
use crate::{schema::move_resources, utils::util::standardize_address};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{
    DeleteResource, MoveStructTag as MoveStructTagPB, WriteResource,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use parquet_derive::{ParquetRecordWriter};
use parquet::record::RecordWriter;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Serialize, ParquetRecordWriter
)]
pub struct MoveResource {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub name: String,
    pub type_: String,
    pub address: String,
    pub module: String,
    pub generic_type_params: Option<String>,
    pub data: Option<String>,
    pub is_deleted: bool,
    pub state_key_hash: String,
    pub block_timestamp: chrono::NaiveDateTime, 
}

pub struct MoveStructTag {
    address: String,
    pub module: String,
    pub name: String,
    pub generic_type_params: Option<String>,
}

impl MoveResource {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        let parsed_data = Self::convert_move_struct_tag(
            write_resource
                .r#type
                .as_ref()
                .expect("MoveStructTag Not Exists."),
        );
        Self {
            transaction_version,
            transaction_block_height,
            write_set_change_index,
            type_: write_resource.type_str.clone(),
            name: parsed_data.name.clone(),
            address: standardize_address(&write_resource.address.to_string()),
            module: parsed_data.module.clone(),
            generic_type_params: parsed_data.generic_type_params,
            data: Some(write_resource.data.clone()),
            is_deleted: false,
            state_key_hash: standardize_address(
                hex::encode(write_resource.state_key_hash.as_slice()).as_str(),
            ),
            block_timestamp,
        }
    }

    pub fn from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        let parsed_data = Self::convert_move_struct_tag(
            delete_resource
                .r#type
                .as_ref()
                .expect("MoveStructTag Not Exists."),
        );
        Self {
            transaction_version,
            transaction_block_height,
            write_set_change_index,
            type_: delete_resource.type_str.clone(),
            name: parsed_data.name.clone(),
            address: standardize_address(&delete_resource.address.to_string()),
            module: parsed_data.module.clone(),
            generic_type_params: parsed_data.generic_type_params,
            data: None,
            is_deleted: true,
            state_key_hash: standardize_address(
                hex::encode(delete_resource.state_key_hash.as_slice()).as_str(),
            ),
            block_timestamp,
        }
    }

    pub fn convert_move_struct_tag(struct_tag: &MoveStructTagPB) -> MoveStructTag {
        MoveStructTag {
            address: standardize_address(struct_tag.address.as_str()),
            module: struct_tag.module.to_string(),
            name: struct_tag.name.to_string(),
            generic_type_params: struct_tag
                .generic_type_params
                .iter()
                .map(|move_type| -> Result<Option<String>> {
                    Ok(Some(
                        serde_json::to_string(move_type).context("Failed to parse move type")?,
                    ))
                })
                .collect::<Result<Option<String>>>()
                .unwrap_or(None),
        }
    }

    pub fn get_outer_type_from_resource(write_resource: &WriteResource) -> String {
        let move_struct_tag =
            Self::convert_move_struct_tag(write_resource.r#type.as_ref().unwrap());

        format!(
            "{}::{}::{}",
            move_struct_tag.get_address(),
            move_struct_tag.module,
            move_struct_tag.name,
        )
    }
}

impl MoveStructTag {
    pub fn get_address(&self) -> String {
        standardize_address(self.address.as_str())
    }
}

pub trait DataSize {
    fn size_of(&self) -> usize;
}

impl DataSize for MoveResource {
    fn size_of(&self) -> usize {
        let base_size = 
            std::mem::size_of::<i64>() * 3 +  // Three i64 fields
            std::mem::size_of::<String>() * 5 +  // Five String fields (name, type_, address, module, state_key_hash)
            std::mem::size_of::<Option<String>>() * 2 +  // Two Optional Strings (generic_type_params, data)
            std::mem::size_of::<bool>() +  // One boolean field
            std::mem::size_of::<chrono::NaiveDateTime>(); // One NaiveDateTime field

        let dynamic_size = 
            self.name.len() + self.name.capacity() +
            self.type_.len() + self.type_.capacity() +
            self.address.len() + self.address.capacity() +
            self.module.len() + self.module.capacity() +
            self.state_key_hash.len() + self.state_key_hash.capacity() +
            self.generic_type_params.as_ref().map_or(0, |s| s.len() + s.capacity()) +
            self.data.as_ref().map_or(0, |s| s.len() + s.capacity());

        base_size + dynamic_size
    }
}
