// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule,
    parquet_move_resources::MoveResource,
    parquet_move_tables::{CurrentTableItem, TableItem, TableMetadata},
};
use crate::{
    bq_analytics::generic_parquet_processor::{HasVersion, NamedTable},
    utils::util::standardize_address,
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{
    write_set_change::{Change as WriteSetChangeEnum, Type as WriteSetChangeTypeEnum},
    WriteSetChange as WriteSetChangePB,
};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Deserialize, FieldCount, Serialize, ParquetRecordWriter)]
pub struct WriteSetChange {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub state_key_hash: String,
    pub change_type: String,
    pub resource_address: String,
    pub block_height: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for WriteSetChange {
    const TABLE_NAME: &'static str = "write_set_changes";
}

impl HasVersion for WriteSetChange {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl Default for WriteSetChange {
    fn default() -> Self {
        Self {
            txn_version: 0,
            write_set_change_index: 0,
            state_key_hash: "".to_string(),
            change_type: "".to_string(),
            resource_address: "".to_string(),
            block_height: 0,
            #[allow(deprecated)]
            block_timestamp: chrono::NaiveDateTime::from_timestamp(0, 0),
        }
    }
}

impl WriteSetChange {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        write_set_change_index: i64,
        txn_version: i64,
        block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, WriteSetChangeDetail) {
        let change_type = Self::get_write_set_change_type(write_set_change);
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");
        match change {
            WriteSetChangeEnum::WriteModule(inner) => (
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address.to_string()),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                )),
            ),
            WriteSetChangeEnum::DeleteModule(inner) => (
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address.to_string()),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_delete_module(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                )),
            ),
            WriteSetChangeEnum::WriteResource(inner) => (
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address.to_string()),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_write_resource(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                )),
            ),
            WriteSetChangeEnum::DeleteResource(inner) => (
                Self {
                    txn_version,
                    state_key_hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    block_height,
                    change_type,
                    resource_address: standardize_address(&inner.address.to_string()),
                    write_set_change_index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_delete_resource(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                )),
            ),
            WriteSetChangeEnum::WriteTableItem(inner) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );
                (
                    Self {
                        txn_version,
                        state_key_hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        block_height,
                        change_type,
                        resource_address: String::default(),
                        write_set_change_index,
                        block_timestamp,
                    },
                    WriteSetChangeDetail::Table(
                        ti,
                        cti,
                        Some(TableMetadata::from_write_table_item(inner)),
                    ),
                )
            },
            WriteSetChangeEnum::DeleteTableItem(inner) => {
                let (ti, cti) = TableItem::from_delete_table_item(
                    inner,
                    write_set_change_index,
                    txn_version,
                    block_height,
                    block_timestamp,
                );
                (
                    Self {
                        txn_version,
                        state_key_hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        block_height,
                        change_type,
                        resource_address: String::default(),
                        write_set_change_index,
                        block_timestamp,
                    },
                    WriteSetChangeDetail::Table(ti, cti, None),
                )
            },
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        txn_version: i64,
        block_height: i64,
        timestamp: chrono::NaiveDateTime,
    ) -> (Vec<Self>, Vec<WriteSetChangeDetail>) {
        write_set_changes
            .iter()
            .enumerate()
            .map(|(write_set_change_index, write_set_change)| {
                Self::from_write_set_change(
                    write_set_change,
                    write_set_change_index as i64,
                    txn_version,
                    block_height,
                    timestamp,
                )
            })
            .collect::<Vec<(Self, WriteSetChangeDetail)>>()
            .into_iter()
            .unzip()
    }

    fn get_write_set_change_type(t: &WriteSetChangePB) -> String {
        match WriteSetChangeTypeEnum::try_from(t.r#type)
            .expect("WriteSetChange must have a valid type.")
        {
            WriteSetChangeTypeEnum::DeleteModule => "delete_module".to_string(),
            WriteSetChangeTypeEnum::DeleteResource => "delete_resource".to_string(),
            WriteSetChangeTypeEnum::DeleteTableItem => "delete_table_item".to_string(),
            WriteSetChangeTypeEnum::WriteModule => "write_module".to_string(),
            WriteSetChangeTypeEnum::WriteResource => "write_resource".to_string(),
            WriteSetChangeTypeEnum::WriteTableItem => "write_table_item".to_string(),
            WriteSetChangeTypeEnum::Unspecified => {
                panic!("WriteSetChange type must be specified.")
            },
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum WriteSetChangeDetail {
    Module(MoveModule),
    Resource(MoveResource),
    Table(TableItem, CurrentTableItem, Option<TableMetadata>),
}

// Prevent conflicts with other things named `WriteSetChange`
pub type WriteSetChangeModel = WriteSetChange;
