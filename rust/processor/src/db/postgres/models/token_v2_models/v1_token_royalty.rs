// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_v2_models::raw_v1_token_royalty::{
        CurrentTokenRoyaltyV1Convertible, RawCurrentTokenRoyaltyV1,
    },
    schema::current_token_royalty_v1,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, PartialEq, Eq,
)]
#[diesel(primary_key(token_data_id))]
#[diesel(table_name = current_token_royalty_v1)]
pub struct CurrentTokenRoyaltyV1 {
    pub token_data_id: String,
    pub payee_address: String,
    pub royalty_points_numerator: BigDecimal,
    pub royalty_points_denominator: BigDecimal,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl Ord for CurrentTokenRoyaltyV1 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id.cmp(&other.token_data_id)
    }
}
impl PartialOrd for CurrentTokenRoyaltyV1 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentTokenRoyaltyV1Convertible for CurrentTokenRoyaltyV1 {
    fn from_raw(raw_item: RawCurrentTokenRoyaltyV1) -> Self {
        Self {
            token_data_id: raw_item.token_data_id,
            payee_address: raw_item.payee_address,
            royalty_points_numerator: raw_item.royalty_points_numerator,
            royalty_points_denominator: raw_item.royalty_points_denominator,
            last_transaction_version: raw_item.last_transaction_version,
            last_transaction_timestamp: raw_item.last_transaction_timestamp,
        }
    }
}
