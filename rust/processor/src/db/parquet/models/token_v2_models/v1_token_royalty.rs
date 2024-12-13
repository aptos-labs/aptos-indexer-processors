// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::token_v2_models::raw_v1_token_royalty::{
        CurrentTokenRoyaltyV1Convertible, RawCurrentTokenRoyaltyV1,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentTokenRoyaltyV1 {
    pub token_data_id: String,
    pub payee_address: String,
    pub royalty_points_numerator: String, // String format of BigDecimal
    pub royalty_points_denominator: String, // String format of BigDecimal
    pub last_transaction_version: i64,
    #[allocative(skip)]
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for CurrentTokenRoyaltyV1 {
    const TABLE_NAME: &'static str = "current_token_royalties_v1";
}

impl HasVersion for CurrentTokenRoyaltyV1 {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl GetTimeStamp for CurrentTokenRoyaltyV1 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.last_transaction_timestamp
    }
}

impl CurrentTokenRoyaltyV1Convertible for CurrentTokenRoyaltyV1 {
    // TODO: consider returning a Result
    fn from_raw(raw_item: RawCurrentTokenRoyaltyV1) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            payee_address: raw_item.payee_address,
            royalty_points_numerator: raw_item.royalty_points_numerator.to_string(),
            royalty_points_denominator: raw_item.royalty_points_denominator.to_string(),
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
        }
    }
}
