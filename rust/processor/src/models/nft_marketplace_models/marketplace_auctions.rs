// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use aptos_protos::transaction::v1::Event;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::{schema::marketplace_auctions, utils::util::standardize_address};
use super::marketplace_utils::MarketplaceEvent;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(listing_id, token_data_id))]
#[diesel(table_name = marketplace_auctions)]
pub struct MarketplaceAuction {
    pub listing_id: String,
    pub token_data_id: String,
    pub collection_id: String,
    pub fee_schedule_id: String,
    pub seller: String,
    pub bid_price: Option<BigDecimal>,
    pub bidder: Option<String>,
    pub starting_bid_price: BigDecimal,
    pub buy_it_now_price: Option<BigDecimal>,
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

impl MarketplaceAuction {
    pub fn from_event(
        event: &Event,
        event_index: i64,
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
        entry_function_id_str: &Option<String>,
        contract_address: &str,
        coin_type: &Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.as_str();
        if let Some(marketplace_event) =
            &MarketplaceEvent::from_event(&event_type, &event.data, transaction_version)?
        {

            let auction = match marketplace_event {
                MarketplaceEvent::ListingFilledEvent(marketplace_event) => {
                    if marketplace_event.r#type == "auction" {
                        Some(MarketplaceAuction {
                            listing_id: marketplace_event.get_listing_address(),
                            token_data_id: marketplace_event.token_metadata.get_token_address().unwrap(),
                            collection_id: marketplace_event.token_metadata.get_collection_address(),
                            fee_schedule_id: event.key.as_ref().unwrap().account_address,
                            seller: marketplace_event.get_seller_address(),
                            bid_price: Some(marketplace_event.price),
                            bidder: Some(marketplace_event.get_purchaser_address()),
                            starting_bid_price: BigDecimal::from(0),
                            buy_it_now_price: None,
                            token_amount: BigDecimal::from(1),
                            expiration_time: BigDecimal::from(0),
                            token_standard: marketplace_event.token_metadata.get_token_standard(),
                            is_deleted: true,
                            coin_type: coin_type.clone(),
                            marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                            contract_address: standardize_address(contract_address),
                            entry_function_id_str: entry_function_id_str
                                .clone()
                                .unwrap_or_else(|| "".to_string()),
                            last_transaction_version: transaction_version,
                            last_transaction_timestamp: transaction_timestamp,
                        })
                    } else {
                        None
                    }
                },
                _ => {
                    None
                },
            };
            return Ok(auction);
        }
        Ok(None)
    }
}