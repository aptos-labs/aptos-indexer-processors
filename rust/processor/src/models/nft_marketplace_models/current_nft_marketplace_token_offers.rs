// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::schema::current_nft_marketplace_token_offers;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(offer_id, token_data_id))]
#[diesel(table_name = current_nft_marketplace_token_offers)]
pub struct CurrentNftMarketplaceTokenOffer {
    pub offer_id: String,
    pub token_data_id: String,
    pub collection_id: String,
    pub fee_schedule_id: String,
    pub buyer: Option<String>,
    pub price: BigDecimal,
    pub token_amount: BigDecimal,
    pub expiration_time: BigDecimal,
    pub is_deleted: bool,
    pub token_standard: String,
    pub coin_type: Option<String>,
    pub marketplace: String,
    pub contract_address: String,
    pub entry_function_id_str: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}