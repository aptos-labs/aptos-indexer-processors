// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule,
    move_resources::MoveResource,
    move_tables::{CurrentTableItem, TableItem, TableMetadata},
    transactions::Transaction,
};
use crate::{
    db::common::models::TableName, schema::write_set_changes, utils::util::standardize_address,
    worker::TableFlags,
};
use aptos_protos::transaction::v1::{
    write_set_change::{Change as WriteSetChangeEnum, Type as WriteSetChangeTypeEnum},
    WriteSetChange as WriteSetChangePB,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Associations, Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize,
)]
#[diesel(belongs_to(Transaction, foreign_key = transaction_version))]
#[diesel(primary_key(transaction_version, index))]
#[diesel(table_name = write_set_changes)]
pub struct WriteSetChange {
    pub transaction_version: i64,
    pub index: i64,
    pub hash: String,
    transaction_block_height: i64,
    pub type_: String,
    pub address: String,
}

impl TableName for WriteSetChange {
    fn table_name() -> &'static str {
        "write_set_changes"
    }
}

impl WriteSetChange {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Self {
        let type_ = Self::get_write_set_change_type(write_set_change);
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");

        match change {
            WriteSetChangeEnum::WriteModule(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: standardize_address(&inner.address.to_string()),
                index,
            },
            WriteSetChangeEnum::DeleteModule(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: standardize_address(&inner.address.to_string()),
                index,
            },
            WriteSetChangeEnum::WriteResource(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: standardize_address(&inner.address.to_string()),
                index,
            },
            WriteSetChangeEnum::DeleteResource(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: standardize_address(&inner.address.to_string()),
                index,
            },
            WriteSetChangeEnum::WriteTableItem(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: String::default(),
                index,
            },
            WriteSetChangeEnum::DeleteTableItem(inner) => Self {
                transaction_version,
                hash: standardize_address(hex::encode(inner.state_key_hash.as_slice()).as_str()),
                transaction_block_height,
                type_,
                address: String::default(),
                index,
            },
        }
    }

    pub fn get_wsc_detail_from_write_set_change(
        write_set_change: &WriteSetChangePB,
        index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
        deprecated_tables: &TableFlags,
    ) -> Option<WriteSetChangeDetail> {
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");
        match change {
            WriteSetChangeEnum::WriteModule(inner) => {
                Some(WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )))
            },
            WriteSetChangeEnum::DeleteModule(inner) => Some(WriteSetChangeDetail::Module(
                MoveModule::from_delete_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                ),
            )),
            WriteSetChangeEnum::WriteResource(inner) => {
                (!deprecated_tables.contains(TableFlags::MOVE_RESOURCES)).then(|| {
                    WriteSetChangeDetail::Resource(MoveResource::from_write_resource(
                        inner,
                        index,
                        transaction_version,
                        transaction_block_height,
                    ))
                })
            },
            WriteSetChangeEnum::DeleteResource(inner) => {
                (!deprecated_tables.contains(TableFlags::MOVE_RESOURCES)).then(|| {
                    WriteSetChangeDetail::Resource(MoveResource::from_delete_resource(
                        inner,
                        index,
                        transaction_version,
                        transaction_block_height,
                    ))
                })
            },
            WriteSetChangeEnum::WriteTableItem(inner) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                Some(WriteSetChangeDetail::Table(
                    ti,
                    cti,
                    Some(TableMetadata::from_write_table_item(inner)),
                ))
            },
            WriteSetChangeEnum::DeleteTableItem(inner) => {
                let (ti, cti) = TableItem::from_delete_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                Some(WriteSetChangeDetail::Table(ti, cti, None))
            },
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        transaction_version: i64,
        transaction_block_height: i64,
        deprecated_tables: &TableFlags,
    ) -> (Vec<Self>, Vec<WriteSetChangeDetail>) {
        let wscs_details = write_set_changes
            .iter()
            .enumerate()
            .filter_map(|(index, write_set_change)| {
                Self::get_wsc_detail_from_write_set_change(
                    write_set_change,
                    index as i64,
                    transaction_version,
                    transaction_block_height,
                    deprecated_tables,
                )
            })
            .collect::<Vec<WriteSetChangeDetail>>();

        if deprecated_tables.contains(TableFlags::WRITE_SET_CHANGES) {
            return (Vec::new(), wscs_details);
        }

        (
            write_set_changes
                .iter()
                .enumerate()
                .map(|(index, write_set_change)| {
                    Self::from_write_set_change(
                        write_set_change,
                        index as i64,
                        transaction_version,
                        transaction_block_height,
                    )
                })
                .collect::<Vec<Self>>(),
            wscs_details,
        )
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
