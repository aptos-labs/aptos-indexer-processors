// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::default_models::move_tables::TableItem,
    utils::util::{hash_str, APTOS_COIN_TYPE_STR},
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::WriteTableItem;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
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

impl NamedTable for CoinSupply {
    const TABLE_NAME: &'static str = "coin_supply";
}

impl HasVersion for CoinSupply {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for CoinSupply {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl CoinSupply {
    /// Currently only supports aptos_coin. Aggregator table detail is in CoinInfo which for aptos coin appears during genesis.
    /// We query for the aggregator table details (handle and key) once upon indexer initiation and use it to fetch supply.
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(data) = &write_table_item.data {
            // Return early if not aggregator table type
            if !(data.key_type == "address" && data.value_type == "u128") {
                return Ok(None);
            }
            // Return early if not aggregator table handle
            if write_table_item.handle.as_str() != APTOS_COIN_SUPPLY_TABLE_HANDLE {
                return Ok(None);
            }

            // Convert to TableItem model. Some fields are just placeholders
            let (table_item_model, _) =
                TableItem::from_write_table_item(write_table_item, 0, txn_version, 0);

            // Return early if not aptos coin aggregator key
            let table_key = table_item_model.decoded_key.as_str().unwrap();
            if table_key != APTOS_COIN_SUPPLY_TABLE_KEY {
                return Ok(None);
            }
            // Everything matches. Get the coin supply
            let supply = table_item_model
                .decoded_value
                .as_ref()
                .unwrap()
                .as_str()
                .unwrap()
                .parse::<BigDecimal>()
                .context(format!(
                    "cannot parse string as u128: {:?}, version {}",
                    table_item_model.decoded_value.as_ref(),
                    txn_version
                ))?;
            return Ok(Some(Self {
                txn_version,
                coin_type_hash: hash_str(APTOS_COIN_TYPE_STR),
                coin_type: APTOS_COIN_TYPE_STR.to_string(),
                supply: supply.to_string(),
                block_timestamp: txn_timestamp,
            }));
        }
        Ok(None)
    }
}
