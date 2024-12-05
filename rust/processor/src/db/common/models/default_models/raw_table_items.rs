use crate::{
    db::{
        common::models::default_models::raw_current_table_items::RawCurrentTableItem,
        postgres::models::default_models::move_tables::TableItem,
    },
    utils::util::{hash_str, standardize_address},
};
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};

/// RawTableItem is a struct that will be used to converted into Postgres or Parquet TableItem
pub struct RawTableItem {
    pub txn_version: i64,
    pub block_timestamp: chrono::NaiveDateTime,
    pub write_set_change_index: i64,
    pub transaction_block_height: i64,
    pub table_key: String,
    pub table_handle: String,
    pub decoded_key: String,
    pub decoded_value: Option<String>,
    pub is_deleted: bool,
}

impl RawTableItem {
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> (Self, RawCurrentTableItem) {
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
            RawCurrentTableItem {
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
    ) -> (Self, RawCurrentTableItem) {
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
            RawCurrentTableItem {
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

    pub fn postgres_table_item_from_write_item(
        write_table_item: &WriteTableItem,
        write_set_change_index: i64,
        txn_version: i64,
        transaction_block_height: i64,
        block_timestamp: chrono::NaiveDateTime,
    ) -> TableItem {
        let (raw_table_item, _current_table_item) = RawTableItem::from_write_table_item(
            write_table_item,
            write_set_change_index,
            txn_version,
            transaction_block_height,
            block_timestamp,
        );
        TableItem::from_raw(&raw_table_item)
    }
}

pub trait TableItemConvertible {
    fn from_raw(raw_item: &RawTableItem) -> Self;
}
