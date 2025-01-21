// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::token_v2_models::raw_token_claims::{
        CurrentTokenPendingClaimConvertible, RawCurrentTokenPendingClaim,
    },
};
use allocative_derive::Allocative;
use bigdecimal::ToPrimitive;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentTokenPendingClaim {
    pub token_data_id_hash: String,
    pub property_version: u64,
    pub from_address: String,
    pub to_address: String,
    pub collection_data_id_hash: String,
    pub creator_address: String,
    pub collection_name: String,
    pub name: String,
    pub amount: String, // String format of BigDecimal
    pub table_handle: String,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub token_data_id: String,
    pub collection_id: String,
}

impl NamedTable for CurrentTokenPendingClaim {
    const TABLE_NAME: &'static str = "current_token_pending_claims";
}

impl HasVersion for CurrentTokenPendingClaim {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTokenPendingClaim {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

impl CurrentTokenPendingClaimConvertible for CurrentTokenPendingClaim {
    fn from_raw(raw_item: RawCurrentTokenPendingClaim) -> Self {
        Self {
            token_data_id_hash: raw_item.token_data_id_hash,
            property_version: raw_item
                .property_version
                .to_u64()
                .expect("Failed to convert property_version to u64"),
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            collection_data_id_hash: raw_item.collection_data_id_hash,
            creator_address: raw_item.creator_address,
            collection_name: raw_item.collection_name,
            name: raw_item.name,
            amount: raw_item.amount.to_string(), // (assuming amount is non-critical)
            table_handle: raw_item.table_handle,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
            token_data_id: raw_item.token_data_id,
            collection_id: raw_item.collection_id,
        }
    }
}
