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