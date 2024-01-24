// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use aptos_protos::transaction::v1::{Event, WriteResource};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use super::marketplace_utils::{AuctionListing, ListingMetadata, MarketplaceEvent, TokenMetadata};
use crate::{schema::marketplace_auctions, utils::util::standardize_address};

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
                            token_data_id: marketplace_event.token_metadata.get_token_address(),
                            collection_id: marketplace_event
                                .token_metadata
                                .get_collection_address(),
                            fee_schedule_id: event.key.as_ref().unwrap().account_address.clone(),
                            seller: marketplace_event.get_seller_address(),
                            bid_price: Some(marketplace_event.price.clone()),
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
                _ => None,
            };
            return Ok(auction);
        }
        Ok(None)
    }

    pub fn from_aggregated_data(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        contract_address: &str,
        coin_type: &Option<String>,
        entry_function_id_str: &Option<String>,
        listing_metadata: Option<&ListingMetadata>,
        auction_listing_metadata: &AuctionListing,
        token_metadata: Option<&TokenMetadata>,
    ) -> anyhow::Result<Option<Self>> {
        let move_resource_address = standardize_address(&write_resource.address);
        let move_resource_type = write_resource.type_str.clone();
        if !move_resource_type.eq(format!("{}::listing::Listing", contract_address).as_str()) {
            return Ok(None);
        }

        if let (Some(listing_metadata), Some(token_metadata)) =
            (listing_metadata, token_metadata)
        {
            let current_auction = MarketplaceAuction {
                listing_id: move_resource_address.clone(),
                token_data_id: listing_metadata
                    .object
                    .get_reference_address(),
                collection_id: token_metadata.get_collection_address(),
                fee_schedule_id: listing_metadata
                    .fee_schedule
                    .get_reference_address(),
                seller: listing_metadata.get_seller_address(),
                bid_price: Some(auction_listing_metadata.get_current_bid_price()),
                bidder: Some(auction_listing_metadata.get_current_bidder()),
                starting_bid_price: auction_listing_metadata.starting_bid.clone(),
                buy_it_now_price: auction_listing_metadata.get_buy_it_now_price(),
                token_amount: BigDecimal::from(1),
                expiration_time: auction_listing_metadata.auction_end_time.clone(),
                is_deleted: false,
                token_standard: token_metadata.get_token_standard(),
                coin_type: coin_type.clone(),
                marketplace: "example_marketplace".to_string(),
                contract_address: contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
            };
            return Ok(Some(current_auction));
        }

        Ok(None)
    }
}
