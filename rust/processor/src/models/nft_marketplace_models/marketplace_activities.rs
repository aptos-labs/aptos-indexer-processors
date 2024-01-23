// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use std::collections::HashSet;

use anyhow::bail;
use aptos_protos::transaction::v1::Event;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

use crate::{schema::marketplace_activities, utils::util::standardize_address};

use super::marketplace_utils::MarketplaceEvent;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = marketplace_activities)]
pub struct MarketplaceActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub offer_or_listing_id: String,
    pub fee_schedule_id: String,
    pub collection_id: String,
    pub token_data_id: Option<String>,
    pub creator_address: String,
    pub collection_name: String,
    pub token_name: Option<String>,
    pub property_version: Option<BigDecimal>,
    pub price: BigDecimal,
    pub token_amount: BigDecimal,
    pub token_standard: String,
    pub seller: Option<String>,
    pub buyer: Option<String>,
    pub coin_type: Option<String>,
    pub marketplace: String,
    pub contract_address: String,
    pub entry_function_id_str: String,
    pub event_type: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl MarketplaceActivity {
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
            let activity = match marketplace_event {
                MarketplaceEvent::ListingFilledEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_listing_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::LISTING_FILLED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::ListingCanceledEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_listing_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
                        buyer: None,
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::LISTING_CANCELED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::ListingPlacedEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_listing_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
                        buyer: None,
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::LISTING_PLACED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::CollectionOfferPlacedEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_collection_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.collection_metadata.get_collection_address().unwrap(),
                        token_data_id: None,
                        creator_address: marketplace_event.collection_metadata.get_creator_address(),
                        collection_name: marketplace_event.collection_metadata.get_collection_name_truncated(),
                        token_name: None,
                        property_version: None,
                        price: marketplace_event.price.clone(),
                        token_amount: marketplace_event.token_amount.clone(),
                        token_standard: marketplace_event.collection_metadata.get_token_standard(),
                        seller: None,
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::COLLECTION_OFFER_PLACED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::CollectionOfferCanceledEvent(
                    marketplace_event,
                ) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_collection_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.collection_metadata.get_collection_address().unwrap(),
                        token_data_id: None,
                        creator_address: marketplace_event.collection_metadata.get_creator_address(),
                        collection_name: marketplace_event.collection_metadata.get_collection_name_truncated(),
                        token_name: None,
                        property_version: None,
                        price: marketplace_event.price.clone(),
                        token_amount: marketplace_event.remaining_token_amount.clone(),
                        token_standard: marketplace_event.collection_metadata.get_token_standard(),
                        seller: None,
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::COLLECTION_OFFER_CANCELED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::CollectionOfferFilledEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_collection_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(0), // there is no token amount for CollectionOfferFilledEvent
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::COLLECTION_OFFER_FILLED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::TokenOfferPlacedEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_token_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: None,
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::TOKEN_OFFER_PLACED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::TokenOfferCanceledEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_token_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(0),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: None,
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::TOKEN_OFFER_CANCELED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::TokenOfferFilledEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_token_offer_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
                        buyer: Some(marketplace_event.get_purchaser_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::TOKEN_OFFER_FILLED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
                MarketplaceEvent::AuctionBidEvent(marketplace_event) => {
                    MarketplaceActivity {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: marketplace_event.get_listing_address(), 
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        creator_address: marketplace_event.token_metadata.get_creator_address(),
                        collection_name: marketplace_event.token_metadata.get_collection_name_truncated(),
                        token_name: Some(marketplace_event.token_metadata.get_token_name_truncated()),
                        property_version: marketplace_event.token_metadata.get_property_version(),
                        price: marketplace_event.new_bid.clone(),
                        token_amount: BigDecimal::from(1),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: None,
                        buyer: Some(marketplace_event.get_new_bidder_address()),
                        coin_type: coin_type.clone(),
                        marketplace: "example_v2_marketplace".to_string(), // TODO: update this to actual marketpalce name
                        contract_address: standardize_address(contract_address),
                        entry_function_id_str: entry_function_id_str
                            .clone()
                            .unwrap_or_else(|| "".to_string()),
                        event_type: MarketplaceEvent::TOKEN_OFFER_FILLED_EVENT.to_string(),
                        transaction_timestamp,
                    }
                },
            };
            return Ok(Some(activity));
        }
        Ok(None)
    }
}