// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::write_set_size_info;
use aptos_protos::transaction::v1::WriteOpSizeInfo;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, index))]
#[diesel(table_name = write_set_size_info)]
pub struct WriteSetSize {
    pub transaction_version: i64,
    pub index: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
}

impl WriteSetSize {
    pub fn from_transaction_info(
        info: &WriteOpSizeInfo,
        transaction_version: i64,
        index: i64,
    ) -> Self {
        WriteSetSize {
            transaction_version,
            index,
            key_bytes: info.key_bytes as i64,
            value_bytes: info.value_bytes as i64,
        }
    }
}
