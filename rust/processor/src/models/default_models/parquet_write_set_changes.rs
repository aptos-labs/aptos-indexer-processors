// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule, move_tables::{CurrentTableItem, TableItem, TableMetadata}, parquet_move_resources::MoveResource, DataSize
};
use crate::{utils::util::standardize_address};
use aptos_protos::transaction::v1::{
    write_set_change::{Change as WriteSetChangeEnum, Type as WriteSetChangeTypeEnum},
    WriteSetChange as WriteSetChangePB,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use parquet_derive::{ParquetRecordWriter};


#[derive(
     Clone, Debug, Deserialize, FieldCount, Serialize, ParquetRecordWriter
)]
pub struct WriteSetChange {
    pub transaction_version: i64,
    pub index: i64,
    pub hash: String,
    transaction_block_height: i64,
    pub type_: String,
    pub address: String,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl WriteSetChange {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, WriteSetChangeDetail) {
        let type_ = Self::get_write_set_change_type(write_set_change);
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");
        match change {
            WriteSetChangeEnum::WriteModule(inner) => (
                Self {
                    transaction_version,
                    hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address.to_string()),
                    index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            ),
            WriteSetChangeEnum::DeleteModule(inner) => (
                Self {
                    transaction_version,
                    hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address.to_string()),
                    index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Module(MoveModule::from_delete_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            ),
            WriteSetChangeEnum::WriteResource(inner) => (
                Self {
                    transaction_version,
                    hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address.to_string()),
                    index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_write_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                    block_timestamp,
                )),
            ),
            WriteSetChangeEnum::DeleteResource(inner) => (
                Self {
                    transaction_version,
                    hash: standardize_address(
                        hex::encode(inner.state_key_hash.as_slice()).as_str(),
                    ),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address.to_string()),
                    index,
                    block_timestamp,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_delete_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                    block_timestamp,
                )),
            ),
            WriteSetChangeEnum::WriteTableItem(inner) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                (
                    Self {
                        transaction_version,
                        hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        transaction_block_height,
                        type_,
                        address: String::default(),
                        index,
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
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                (
                    Self {
                        transaction_version,
                        hash: standardize_address(
                            hex::encode(inner.state_key_hash.as_slice()).as_str(),
                        ),
                        transaction_block_height,
                        type_,
                        address: String::default(),
                        index,
                        block_timestamp,
                    },
                    WriteSetChangeDetail::Table(ti, cti, None),
                )
            },
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        transaction_version: i64,
        transaction_block_height: i64,
        timestamp: chrono::NaiveDateTime,
    ) -> (Vec<Self>, Vec<WriteSetChangeDetail>) {
        write_set_changes
            .iter()
            .enumerate()
            .map(|(index, write_set_change)| {
                Self::from_write_set_change(
                    write_set_change,
                    index as i64,
                    transaction_version,
                    transaction_block_height,
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

impl DataSize for WriteSetChange {
    fn size_of(&self) -> usize {
        let base_size = std::mem::size_of::<i64>() * 3; // Three i64 fields
        let hash_size = self.hash.len() + self.hash.capacity(); // Actual length + allocated capacity
        let type_size = self.type_.len() + self.type_.capacity();
        let address_size = self.address.len() + self.address.capacity();
        
        base_size + hash_size + type_size + address_size
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
