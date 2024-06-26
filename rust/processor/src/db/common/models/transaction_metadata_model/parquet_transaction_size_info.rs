// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::bq_analytics::generic_parquet_processor::{HasVersion, NamedTable};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::TransactionSizeInfo;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct TransactionSize {
    pub txn_version: i64,
    pub size_bytes: i64,
}

impl NamedTable for TransactionSize {
    const TABLE_NAME: &'static str = "transaction_size";
}

impl HasVersion for TransactionSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl TransactionSize {
    pub fn from_transaction_info(info: &TransactionSizeInfo, txn_version: i64) -> Self {
        TransactionSize {
            txn_version,
            size_bytes: info.transaction_bytes as i64,
        }
    }
}
