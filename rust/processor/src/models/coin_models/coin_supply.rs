// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    models::default_models::move_tables::TableItem,
    processors::coin_processor::APTOS_COIN_TYPE_STR, schema::coin_supply, utils::util::hash_str,
};
use anyhow::Context;
use aptos_protos::transaction::v1::WriteTableItem;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

const APTOS_COIN_SUPPLY_TABLE_HANDLE: &str =
    "0x1b854694ae746cdbd8d44186ca4929b2b337df21d1c74633be19b2710552fdca";
const APTOS_COIN_SUPPLY_TABLE_KEY: &str =
    "0x619dc29a0aac8fa146714058e8dd6d2d0f3bdf5f6331907bf91f3acd81e6935";

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Clone)]
#[diesel(primary_key(transaction_version, coin_type_hash))]
#[diesel(table_name = coin_supply)]
pub struct CoinSupply {
    pub transaction_version: i64,
    pub coin_type_hash: String,
    pub coin_type: String,
    pub supply: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub transaction_epoch: i64,
}

impl CoinSupply {
    /// Currently only supports aptos_coin. Aggregator table detail is in CoinInfo which for aptos coin appears during genesis.
    /// We query for the aggregator table details (handle and key) once upon indexer initiation and use it to fetch supply.
    pub fn from_write_table_item(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        txn_epoch: i64,
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
                transaction_version: txn_version,
                coin_type_hash: hash_str(APTOS_COIN_TYPE_STR),
                coin_type: APTOS_COIN_TYPE_STR.to_string(),
                supply,
                transaction_timestamp: txn_timestamp,
                transaction_epoch: txn_epoch,
            }));
        }
        Ok(None)
    }
}
