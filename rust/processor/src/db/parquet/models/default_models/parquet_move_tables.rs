// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::default_models::{
        raw_current_table_items::{CurrentTableItemConvertible, RawCurrentTableItem},
        raw_table_items::{RawTableItem, TableItemConvertible},
    },
    utils::util::{hash_str, standardize_address},
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct TableItem {
    pub txn_version: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl NamedTable for TableItem {
    const TABLE_NAME: &'static str = "table_items";
}

impl HasVersion for TableItem {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TableItem {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct CurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for CurrentTableItem {
    const TABLE_NAME: &'static str = "current_table_items";
}

impl HasVersion for CurrentTableItem {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTableItem {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct TableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl NamedTable for TableMetadata {
    const TABLE_NAME: &'static str = "table_metadata";
}

impl HasVersion for TableMetadata {
    fn version(&self) -> i64 {
        0 // This is a placeholder value to avoid a compile error
    }
}

impl GetTimeStamp for TableMetadata {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        #[warn(deprecated)]
        chrono::NaiveDateTime::default()
    }
}

impl TableItem {
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: write_table_item.key.to_string(),
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                is_deleted: false,
                block_timestamp,
            },
            CurrentTableItem {
                table_handle: standardize_address(&write_table_item.handle.to_string()),
                key_hash: hash_str(&write_table_item.key.to_string()),
                key: write_table_item.key.to_string(),
                decoded_key: write_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: Some(write_table_item.data.as_ref().unwrap().value.clone()),
                last_transaction_version: txn_version,
                is_deleted: false,
                block_timestamp,
            },
        )
    }

    pub fn from_delete_table_item(
        delete_table_item: &DeleteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, CurrentTableItem) {
        (
            Self {
                txn_version,
                write_set_change_index,
                transaction_block_height,
                table_key: delete_table_item.key.to_string(),
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                is_deleted: true,
                block_timestamp,
            },
            CurrentTableItem {
                table_handle: standardize_address(&delete_table_item.handle.to_string()),
                key_hash: hash_str(&delete_table_item.key.to_string()),
                key: delete_table_item.key.to_string(),
                decoded_key: delete_table_item.data.as_ref().unwrap().key.clone(),
                decoded_value: None,
                last_transaction_version: txn_version,
                is_deleted: true,
                block_timestamp,
            },
        )
    }
}

impl TableMetadata {
    pub fn from_write_table_item(table_item: &WriteTableItem) -> Self {
        Self {
            handle: table_item.handle.to_string(),
            key_type: table_item.data.as_ref().unwrap().key_type.clone(),
            value_type: table_item.data.as_ref().unwrap().value_type.clone(),
        }
    }
}

impl TableItemConvertible for TableItem {
    fn from_raw(raw_item: &RawTableItem) -> Self {
        TableItem {
            txn_version: raw_item.txn_version,
            write_set_change_index: raw_item.write_set_change_index,
            transaction_block_height: raw_item.transaction_block_height,
            table_key: raw_item.table_key.clone(),
            table_handle: raw_item.table_handle.clone(),
            decoded_key: raw_item.decoded_key.clone(),
            decoded_value: raw_item.decoded_value.clone(),
            is_deleted: raw_item.is_deleted,
            block_timestamp: raw_item.block_timestamp,
        }
    }
}

impl CurrentTableItemConvertible for CurrentTableItem {
    fn from_raw(raw_item: &RawCurrentTableItem) -> Self {
        CurrentTableItem {
            table_handle: raw_item.table_handle.clone(),
            key_hash: raw_item.key_hash.clone(),
            key: raw_item.key.clone(),
            decoded_key: raw_item.decoded_key.clone(),
            decoded_value: raw_item.decoded_value.clone(),
            last_transaction_version: raw_item.last_transaction_version,
            is_deleted: raw_item.is_deleted,
            block_timestamp: raw_item.block_timestamp,
        }
    }
}
