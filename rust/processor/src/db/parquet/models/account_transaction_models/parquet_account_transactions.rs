// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

pub type AccountTransactionPK = (String, i64);

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct AccountTransaction {
    pub txn_version: i64,
    pub account_address: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for AccountTransaction {
    const TABLE_NAME: &'static str = "account_transactions";
}

impl HasVersion for AccountTransaction {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for AccountTransaction {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}
