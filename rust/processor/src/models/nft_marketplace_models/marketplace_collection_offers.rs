// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::schema::current_nft_marketplace_collection_offers;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(collection_offer_id, collection_id))]
#[diesel(table_name = current_nft_marketplace_collection_offers)]
pub struct MarketplaceCollectionOffer {
    pub collection_offer_id: String,
    pub collection_id: String,
    pub fee_schedule_id: String,
    pub buyer: String,
    pub item_price: BigDecimal,
    pub remaining_token_amount: BigDecimal,
    pub expiration_time: BigDecimal,
    pub is_deleted: bool,
    pub token_standard: String,
    pub coin_type: Option<String>,
    pub marketplace: String,
    pub contract_address: String,
    pub entry_function_id_str: String,
    pub last_transaction_version: i64,
    pub transaction_timestamp: chrono::NaiveDateTime,
}