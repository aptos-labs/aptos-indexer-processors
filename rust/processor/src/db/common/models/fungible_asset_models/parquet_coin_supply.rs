// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CoinSupply {
    pub txn_version: i64,
    pub coin_type_hash: String,
    pub coin_type: String,
    pub supply: String, // it is a string representation of the u128
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}
