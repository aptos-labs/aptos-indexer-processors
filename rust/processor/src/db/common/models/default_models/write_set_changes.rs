// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule,
    move_resources::MoveResource,
    move_tables::{CurrentTableItem, TableItem, TableMetadata},
    transactions::Transaction,
};
use crate::schema::write_set_changes;
use aptos_protos::transaction::v1::{
    write_set_change::Change as WriteSetChangeEnum, WriteSetChange as WriteSetChangePB,
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

#[derive(Deserialize, Serialize)]
pub enum WriteSetChangeDetail {
    Module(MoveModule),
    Resource(MoveResource),
    Table(TableItem, CurrentTableItem, Option<TableMetadata>),
}

impl WriteSetChangeDetail {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> WriteSetChangeDetail {
        let change = write_set_change
            .change
            .as_ref()
            .expect("WriteSetChange must have a change");

        match change {
            WriteSetChangeEnum::WriteModule(inner) => {
                WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                ))
            },
            WriteSetChangeEnum::DeleteModule(inner) => {
                WriteSetChangeDetail::Module(MoveModule::from_delete_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                ))
            },
            WriteSetChangeEnum::WriteResource(inner) => {
                WriteSetChangeDetail::Resource(MoveResource::from_write_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                ))
            },
            WriteSetChangeEnum::DeleteResource(inner) => {
                WriteSetChangeDetail::Resource(MoveResource::from_delete_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                ))
            },
            WriteSetChangeEnum::WriteTableItem(inner) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );

                WriteSetChangeDetail::Table(
                    ti,
                    cti,
                    Some(TableMetadata::from_write_table_item(inner)),
                )
            },
            WriteSetChangeEnum::DeleteTableItem(inner) => {
                let (ti, cti) = TableItem::from_delete_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                WriteSetChangeDetail::Table(ti, cti, None)
            },
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Vec<WriteSetChangeDetail> {
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
            .collect::<Vec<WriteSetChangeDetail>>()
    }
}

// Prevent conflicts with other things named `WriteSetChange`
pub type WriteSetChangeModel = WriteSetChange;
