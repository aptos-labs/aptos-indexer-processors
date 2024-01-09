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

use crate::{schema::nft_marketplace_activities, utils::util::standardize_address};

use super::nft_marketplace_utils::{MarketplaceTokenMetadata, MarketplaceCollectionMetadata};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = nft_marketplace_activities)]
pub struct NftMarketplaceActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub offer_or_listing_id: String,
    pub fee_schedule_id: String,
    pub collection_id: String,
    pub token_data_id: String,
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

impl NftMarketplaceActivity {
    pub fn from_event(
        event: &Event,
        transaction_version: i64,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        coin_type: &Option<String>,
        transaction_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {

        let type_str_vec = event.type_str.as_str().split("::").collect::<Vec<&str>>();
        let contract_addr = type_str_vec[0];
        let event_type = type_str_vec[type_str_vec.len() - 1];

        // TODO - We currently only index events from our own smart contracts
        // Need to update this code to include all marketplaces. e.g Topaz
        // Ideally, we will have a config file that lists all the marketplaces we want to index
        // And if there any new marketplace we want to index, we only need to update the config file
        // Though, the event type namings has to match exactly. e.g "ListingFilledEvent"
        let example_marketplace_contract_address = "0x6de37368e31dff4580b211295198159ee6f98b42ffa93c5683bb955ca1be67e0";
        if contract_addr.eq(example_marketplace_contract_address) {
            return Ok(None);
        }

        // Get the events set. Only the events in the set will be indexed
        let events_set = MarketplaceEventType::get_events_set();
        if !events_set.contains(event_type) {
            return Ok(None);
        }

        let event_data = serde_json::from_str::<serde_json::Value>(&event.data)?;
        let price = event_data["price"].as_str().expect("Error: Price missing from marketplace activity").parse::<BigDecimal>()?;
        let activity_offer_or_listing_id = if let Some(listing) = event_data["listing"].as_str() {
            standardize_address(listing)
        } else if let Some(token_offer) = event_data["token_offer"].as_str() {
            standardize_address(token_offer)
        } else if let Some(collection_offer) = event_data["collection_offer"].as_str() {
            standardize_address(collection_offer)
        } else {
            // throw error because its missing the offer or listing id
            bail!("Error: listing, token_offer, or collection offer id missing from marketplace activity");
        };

        let activity_fee_schedule_id = event.key.as_ref().unwrap().account_address.as_str();
        let token_metadata = MarketplaceTokenMetadata::from_event_data(event_data)?;
        let collection_metadata = MarketplaceCollectionMetadata::from_event_data(event_data)?;

        // create the activity based on the event type
        match event_type {
            MarketplaceEventType::ListingPlacedEvent => {
                // assert token_metadata exists
                if token_metadata.is_none() {
                    bail!("Error: Token metadata missing from marketplace activity - {}", event_type);
                }

                if let Some(token_metadata) = token_metadata {
                    Ok(Some(Self {
                        transaction_version,
                        event_index,
                        offer_or_listing_id: activity_offer_or_listing_id,
                        fee_schedule_id: activity_fee_schedule_id.to_string(),
                        collection_id: token_metadata.collection_id,
                        token_data_id: token_metadata.token_data_id,
                        creator_address: token_metadata.creator_address,
                        collection_name: token_metadata.collection_name,
                        token_name: Some(token_metadata.token_name),
                        property_version: token_metadata.property_version,
                        price: price,
                        token_amount: 1,
                        token_standard: token_metadata.token_standard,
                        seller: Some(standardize_address(event_data["seller"].as_str())),
                        buyer: None,
                        coin_type: coin_type.clone(),
                        marketplace: "example_marketplace".to_string(),
                        contract_address: example_marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                        entry_function_id_str: entry_function_id_str.clone(),
                        event_type: event_type.to_string(),
                        transaction_timestamp,
                    }))
                } else {
                    bail!("Error: Token metadata missing from marketplace activity - {}", event_type);
                }
            },
            _ => {
                Ok(None)
            }
        }
    }
}

pub enum MarketplaceEventType {
    ListingFilledEvent,
    ListingCanceledEvent,
    ListingPlacedEvent,
    CollectionOfferPlacedEvent,
    CollectionOfferCanceledEvent,
    CollectionOfferFilledEvent,
    TokenOfferPlacedEvent,
    TokenOfferCanceledEvent,
    TokenOfferFilledEvent,
    AuctionBidEvent,
}

impl MarketplaceEventType {
    pub const LISTING_FILLED_EVENT: &'static str = "ListingFilledEvent";
    pub const LISTING_CANCELED_EVENT: &'static str = "ListingCanceledEvent";
    pub const LISTING_PLACED_EVENT: &'static str = "ListingPlacedEvent";
    pub const COLLECTION_OFFER_PLACED_EVENT: &'static str = "CollectionOfferPlacedEvent";
    pub const COLLECTION_OFFER_CANCELED_EVENT: &'static str = "CollectionOfferCanceledEvent";
    pub const COLLECTION_OFFER_FILLED_EVENT: &'static str = "CollectionOfferFilledEvent";
    pub const TOKEN_OFFER_PLACED_EVENT: &'static str = "TokenOfferPlacedEvent";
    pub const TOKEN_OFFER_CANCELED_EVENT: &'static str = "TokenOfferCanceledEvent";
    pub const TOKEN_OFFER_FILLED_EVENT: &'static str = "TokenOfferFilledEvent";
    pub const AUCTION_BID_EVENT: &'static str = "AuctionBidEvent";

    pub fn get_events_set() -> HashSet<&'static str> {
        let mut events = HashSet::new();
        events.insert(Self::LISTING_FILLED_EVENT);
        events.insert(Self::LISTING_CANCELED_EVENT);
        events.insert(Self::LISTING_PLACED_EVENT);
        events.insert(Self::COLLECTION_OFFER_PLACED_EVENT);
        events.insert(Self::COLLECTION_OFFER_CANCELED_EVENT);
        events.insert(Self::COLLECTION_OFFER_FILLED_EVENT);
        events.insert(Self::TOKEN_OFFER_PLACED_EVENT);
        events.insert(Self::TOKEN_OFFER_CANCELED_EVENT);
        events.insert(Self::TOKEN_OFFER_FILLED_EVENT);
        events.insert(Self::AUCTION_BID_EVENT);
        events
    }
}