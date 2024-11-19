// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::default_models::raw_table_items::{RawTableItem, TableItemConvertible},
    schema::{current_table_items, table_items, table_metadatas},
};
use aptos_protos::transaction::v1::WriteTableItem;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(table_handle, key_hash))]
#[diesel(table_name = current_table_items)]
pub struct CurrentTableItem {
    pub table_handle: String,
    pub key_hash: String,
    pub key: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = table_items)]
pub struct TableItem {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub key: String,
    pub table_handle: String,
    pub decoded_key: serde_json::Value,
    pub decoded_value: Option<serde_json::Value>,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(handle))]
#[diesel(table_name = table_metadatas)]
pub struct TableMetadata {
    pub handle: String,
    pub key_type: String,
    pub value_type: String,
}

impl TableItemConvertible for TableItem {
    fn from_raw(raw_item: &RawTableItem) -> Self {
        TableItem {
            transaction_version: raw_item.txn_version,
            write_set_change_index: raw_item.write_set_change_index,
            transaction_block_height: raw_item.transaction_block_height,
            key: raw_item.table_key.clone(),
            table_handle: raw_item.table_handle.clone(),
            decoded_key: serde_json::from_str(raw_item.decoded_key.as_str()).unwrap(),
            decoded_value: raw_item
                .decoded_value
                .clone()
                .map(|v| serde_json::from_str(v.as_str()).unwrap()),
            is_deleted: raw_item.is_deleted,
        }
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
