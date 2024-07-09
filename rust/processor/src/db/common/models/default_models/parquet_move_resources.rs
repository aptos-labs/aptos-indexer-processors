// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    utils::util::standardize_address,
};
use allocative_derive::Allocative;
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{
    DeleteResource, MoveStructTag as MoveStructTagPB, WriteResource,
};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct MoveResource {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub block_height: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub resource_address: String,
    pub resource_type: String,
    pub module: String,
    pub fun: String,
    pub is_deleted: bool,
    pub generic_type_params: Option<String>,
    pub data: Option<String>,
    pub state_key_hash: String,
}

impl NamedTable for MoveResource {
    const TABLE_NAME: &'static str = "move_resources";
}

impl HasVersion for MoveResource {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for MoveResource {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

pub struct MoveStructTag {
    #[allow(dead_code)]
    resource_address: String,
    pub module: String,
    pub fun: String,
    pub generic_type_params: Option<String>,
}

impl MoveResource {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        let move_struct_tag = match write_resource.r#type.as_ref() {
            Some(t) => t,
            None => return Ok(None),
        };

        let parsed_data = convert_move_struct_tag(move_struct_tag);

        let move_resource = Self {
            txn_version,
            block_height,
            write_set_change_index,
            resource_type: write_resource.type_str.clone(),
            fun: parsed_data.fun.clone(),
            resource_address: standardize_address(&write_resource.address.to_string()),
            module: parsed_data.module.clone(),
            generic_type_params: parsed_data.generic_type_params,
            data: Some(write_resource.data.clone()),
            is_deleted: false,
            state_key_hash: standardize_address(
                hex::encode(write_resource.state_key_hash.as_slice()).as_str(),
            ),
            block_timestamp,
        };
        Ok(Some(move_resource))
    }

    pub fn from_delete_resource(
        delete_resource: &DeleteResource,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Result<Option<Self>> {
        let move_struct_tag = match delete_resource.r#type.as_ref() {
            Some(t) => t,
            None => return Ok(None),
        };
        let parsed_data = convert_move_struct_tag(move_struct_tag);
        let move_resource = Self {
            txn_version,
            block_height,
            write_set_change_index,
            resource_type: delete_resource.type_str.clone(),
            fun: parsed_data.fun.clone(),
            resource_address: standardize_address(&delete_resource.address.to_string()),
            module: parsed_data.module.clone(),
            generic_type_params: parsed_data.generic_type_params,
            data: None,
            is_deleted: true,
            state_key_hash: standardize_address(
                hex::encode(delete_resource.state_key_hash.as_slice()).as_str(),
            ),
            block_timestamp,
        };
        Ok(Some(move_resource))
    }
}

pub fn convert_move_struct_tag(struct_tag: &MoveStructTagPB) -> MoveStructTag {
    MoveStructTag {
        resource_address: standardize_address(struct_tag.address.as_str()),
        module: struct_tag.module.to_string(),
        fun: struct_tag.name.to_string(),
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
