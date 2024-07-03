// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::default_models::move_tables::TableItem,
    utils::util::{hash_str, APTOS_COIN_TYPE_STR},
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::WriteTableItem;
use bigdecimal::{BigDecimal, ToPrimitive};
use field_count::FieldCount;
use parquet::data_type::Decimal;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

const APTOS_COIN_SUPPLY_TABLE_HANDLE: &str =
    "0x1b854694ae746cdbd8d44186ca4929b2b337df21d1c74633be19b2710552fdca";
const APTOS_COIN_SUPPLY_TABLE_KEY: &str =
    "0x619dc29a0aac8fa146714058e8dd6d2d0f3bdf5f6331907bf91f3acd81e6935";

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
