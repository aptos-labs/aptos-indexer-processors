// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::{
        common::models::token_v2_models::raw_v2_token_metadata::{
            CurrentTokenV2MetadataConvertible, RawCurrentTokenV2Metadata,
        },
        parquet::models::DEFAULT_NONE,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentTokenV2Metadata {
    pub object_address: String,
    pub resource_type: String,
    pub data: String,
    pub state_key_hash: String,
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}
impl NamedTable for CurrentTokenV2Metadata {
    const TABLE_NAME: &'static str = "current_token_v2_metadata";
}

impl HasVersion for CurrentTokenV2Metadata {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTokenV2Metadata {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

impl CurrentTokenV2MetadataConvertible for CurrentTokenV2Metadata {
    // TODO: consider returning a Result
    fn from_raw(raw_item: RawCurrentTokenV2Metadata) -> Self {
        Self {
            object_address: raw_item.object_address,
            resource_type: raw_item.resource_type,
            data: canonical_json::to_string(&raw_item.data).unwrap_or_else(|_| {
                error!("Failed to serialize data to JSON: {:?}", raw_item.data);
                DEFAULT_NONE.to_string()
            }),
            state_key_hash: raw_item.state_key_hash,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
        }
    }
}
