// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::transaction_size_info;
use aptos_protos::transaction::v1::TransactionSizeInfo;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version))]
#[diesel(table_name = transaction_size_info)]
pub struct TransactionSize {
    pub transaction_version: i64,
    pub size_bytes: i64,
}

impl TransactionSize {
    pub fn from_transaction_info(info: &TransactionSizeInfo, transaction_version: i64) -> Self {
        TransactionSize {
            transaction_version,
            size_bytes: info.transaction_bytes as i64,
        }
    }
}
