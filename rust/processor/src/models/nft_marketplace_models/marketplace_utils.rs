// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use std::collections::HashSet;

use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    models::{
        ans_models::ans_utils::OptionalString,
        default_models::move_resources::MoveResource,
        fungible_asset_models::v2_fungible_asset_utils::OptionalBigDecimal,
        token_models::token_utils::{CollectionDataIdType, TokenDataIdType, NAME_LENGTH},
        token_v2_models::v2_token_utils::{ResourceReference, TokenStandard},
    },
    utils::util::{deserialize_from_string, standardize_address, truncate_str},
};
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{Event, WriteResource};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferEventMetadata {
    pub collection_offer_id: String,
    pub collection_metadata: CollectionMetadata,
    pub item_price: BigDecimal,
    pub buyer: String,
    pub fee_schedule_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingMetadata {
    seller: String,
    pub fee_schedule: ResourceReference,
    pub object: ResourceReference, // token address
}

impl ListingMetadata {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::listing::Listing", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "ListingMetadata from event with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FixedPriceListing {
    pub price: BigDecimal,
}

impl FixedPriceListing {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::coin_listing::FixedPriceListing", contract_address) {
            return Ok(None);
        }

        serde_json::from_str(&write_resource.data)
            .map(|inner| Some(inner))
            .context(format!(
                "FixedPriceListing from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingTokenV1Container {
    pub token_metadata: TokenMetadata,
    pub amount: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferMetadata {
    pub expiration_time: BigDecimal,
    pub item_price: BigDecimal,
    pub fee_schedule: ResourceReference,
}

impl TokenOfferMetadata {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::token_offer::TokenOffer", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "ListingMetadata from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferV1 {
    token_name: String,
    collection_name: String,
    creator_address: String,
    property_version: BigDecimal,
}

impl TokenOfferV1 {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::token_offer::TokenOfferTokenV1", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "TokenOfferV1 from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }

    pub fn get_token_data_id(&self) -> TokenDataIdType {
        let token_data_id = TokenDataIdType::new(
            self.creator_address.clone(),
            self.collection_name.clone(),
            self.token_name.clone(),
        );

        token_data_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferV2 {
    pub token: ResourceReference,
}

impl TokenOfferV2 {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::token_offer::TokenOfferTokenV2", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "TokenOfferV2 from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferMetadata {
    pub expiration_time: BigDecimal,
    pub item_price: BigDecimal,
    pub remaining: BigDecimal,
    pub fee_schedule: ResourceReference,
}

impl CollectionOfferMetadata {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::collection_offer::CollectionOffer", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "CollectionOfferMetadata from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferV1 {
    creator_address: String,
    collection_name: String,
}

impl CollectionOfferV1 {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str
            != format!(
                "{}::collection_offer::CollectionOfferTokenV1",
                contract_address
            )
        {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "CollectionOfferV1 from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }

    pub fn get_collection_data_id(&self) -> CollectionDataIdType {
        let collection_data_id =
            CollectionDataIdType::new(self.creator_address.clone(), self.collection_name.clone());

        collection_data_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferV2 {
    pub collection: ResourceReference,
}

impl CollectionOfferV2 {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str
            != format!(
                "{}::collection_offer::CollectionOfferTokenV2",
                contract_address
            )
        {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "CollectionOfferV2 from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuctionListing {
    pub buy_it_now_price: OptionalBigDecimal,
    pub current_bid: CurrentBidWrapper,
    pub auction_end_time: BigDecimal,
    pub starting_bid: BigDecimal,
}

impl AuctionListing {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();
        if type_str != format!("{}::coin_listing::AuctionListing", contract_address) {
            return Ok(None);
        }

        Ok(Some(serde_json::from_str::<Self>(data).context(
            format!(
                "AuctionListing from wsc with version {} failed! failed to parse data {:?}",
                txn_version, data
            ),
        )?))
    }

    pub fn get_current_bidder(&self) -> String {
        self.current_bid.vec.first().unwrap().get_bidder_address()
    }

    pub fn get_current_bid_price(&self) -> BigDecimal {
        self.current_bid.vec.first().unwrap().coins.value.clone()
    }

    pub fn get_buy_it_now_price(&self) -> Option<BigDecimal> {
        self.buy_it_now_price.vec.first().map(|x| x.0.clone())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentBidWrapper {
    pub vec: Vec<CurrentBid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentBid {
    pub coins: CurrentBidValue, // coins
    bidder: String,
}

impl CurrentBid {
    pub fn get_bidder_address(&self) -> String {
        standardize_address(&self.bidder)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CurrentBidValue {
    pub value: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InnerResourceVec {
    pub vec: Vec<ResourceReference>,
}

impl InnerResourceVec {
    pub fn get_address(&self) -> Option<String> {
        if self.vec.is_empty() {
            None
        } else {
            self.vec.first().as_ref().map(|x| x.get_reference_address())
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenMetadata {
    creator_address: String,
    collection_name: String,
    collection: InnerResourceVec,
    token_name: String,
    token: InnerResourceVec,
    property_version: OptionalBigDecimal,
}

impl TokenMetadata {
    pub fn get_creator_address(&self) -> String {
        standardize_address(&self.creator_address)
    }

    pub fn get_collection_address(&self) -> String {
        if let Some(inner) = self.collection.get_address() {
            inner
        } else {
            let token_data_id = TokenDataIdType::new(
                self.creator_address.clone(),
                self.collection_name.clone(),
                self.token_name.clone(),
            );
            token_data_id.get_collection_id()
        }
    }

    pub fn get_collection_name_truncated(&self) -> String {
        truncate_str(&self.collection_name, NAME_LENGTH)
    }

    pub fn get_token_name_truncated(&self) -> String {
        truncate_str(&self.token_name, NAME_LENGTH)
    }

    pub fn get_token_address(&self) -> String {
        if let Some(inner) = self.token.get_address() {
            inner
        } else {
            let token_data_id = TokenDataIdType::new(
                self.creator_address.clone(),
                self.collection_name.clone(),
                self.token_name.clone(),
            );
            token_data_id.to_id()
        }
    }

    pub fn get_property_version(&self) -> Option<BigDecimal> {
        self.property_version.vec.first().map(|x| x.0.clone())
    }

    pub fn get_token_standard(&self) -> String {
        if let Some(_inner) = self.token.get_address() {
            TokenStandard::V2.to_string()
        } else {
            TokenStandard::V1.to_string()
        }
    }

    pub fn from_event(event: &Event, transaction_version: i64) -> anyhow::Result<Option<Self>> {
        serde_json::from_str(&event.data)
            .map(|inner| Some(inner))
            .context(format!(
                "TokenMetadata from event with version {} failed! failed to parse data {:?}",
                transaction_version, event.data
            ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionMetadata {
    creator_address: String,
    collection_name: String,
    collection: OptionalString,
}

impl CollectionMetadata {
    pub fn new(
        creator_address: String,
        collection_name: String,
        collection: OptionalString,
    ) -> Self {
        Self {
            creator_address,
            collection_name,
            collection,
        }
    }

    pub fn get_creator_address(&self) -> String {
        standardize_address(&self.creator_address)
    }

    pub fn get_collection_address(&self) -> Option<String> {
        if let Some(inner) = self.collection.get_string() {
            Some(standardize_address(&inner))
        } else {
            let collection_data_id = CollectionDataIdType::new(
                self.creator_address.clone(),
                self.collection_name.clone(),
            );
            Some(collection_data_id.to_id())
        }
    }

    pub fn get_token_standard(&self) -> String {
        if let Some(_inner) = self.collection.get_string() {
            TokenStandard::V2.to_string()
        } else {
            TokenStandard::V1.to_string()
        }
    }

    pub fn get_collection_name_truncated(&self) -> String {
        truncate_str(&self.collection_name, NAME_LENGTH)
    }

    pub fn from_event(event: &Event, transaction_version: i64) -> anyhow::Result<Option<Self>> {
        serde_json::from_str(&event.data)
            .map(|inner| Some(inner))
            .context(format!(
                "CollectionMetadata from event with version {} failed! failed to parse data {:?}",
                transaction_version, event.data
            ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MarketplaceEvent {
    ListingFilledEvent(ListingFilledEvent),
    ListingCanceledEvent(ListingCanceledEvent),
    ListingPlacedEvent(ListingPlacedEvent),
    CollectionOfferPlacedEvent(CollectionOfferPlacedEvent),
    CollectionOfferCanceledEvent(CollectionOfferCanceledEvent),
    CollectionOfferFilledEvent(CollectionOfferFilledEvent),
    TokenOfferPlacedEvent(TokenOfferPlacedEvent),
    TokenOfferCanceledEvent(TokenOfferCanceledEvent),
    TokenOfferFilledEvent(TokenOfferFilledEvent),
    AuctionBidEvent(AuctionBidEvent),
}

impl MarketplaceEvent {
    pub const LISTING_FILLED_EVENT: &'static str = "events::ListingFilledEvent";
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

    pub fn from_event(data_type: &str, data: &str, txn_version: i64) -> Result<Option<Self>> {
        match data_type {
            x if x.ends_with(Self::LISTING_FILLED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::ListingFilledEvent(inner)))
            },
            x if x.ends_with(Self::LISTING_CANCELED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::ListingCanceledEvent(inner)))
            },
            x if x.ends_with(Self::LISTING_PLACED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::ListingPlacedEvent(inner)))
            },
            x if x.ends_with(Self::COLLECTION_OFFER_PLACED_EVENT) => serde_json::from_str(data)
                .map(|inner| Some(Self::CollectionOfferPlacedEvent(inner))),
            x if x.ends_with(Self::COLLECTION_OFFER_CANCELED_EVENT) => serde_json::from_str(data)
                .map(|inner| Some(Self::CollectionOfferCanceledEvent(inner))),
            x if x.ends_with(Self::COLLECTION_OFFER_FILLED_EVENT) => serde_json::from_str(data)
                .map(|inner| Some(Self::CollectionOfferFilledEvent(inner))),
            x if x.ends_with(Self::TOKEN_OFFER_PLACED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::TokenOfferPlacedEvent(inner)))
            },
            x if x.ends_with(Self::TOKEN_OFFER_CANCELED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::TokenOfferCanceledEvent(inner)))
            },
            x if x.ends_with(Self::TOKEN_OFFER_FILLED_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::TokenOfferFilledEvent(inner)))
            },
            x if x.ends_with(Self::AUCTION_BID_EVENT) => {
                serde_json::from_str(data).map(|inner| Some(Self::AuctionBidEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "MarketplaceEvent with version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type, data
        ))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingFilledEvent {
    pub r#type: String,
    listing: String,
    seller: String,
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingFilledEvent {
    pub fn get_listing_address(&self) -> String {
        standardize_address(&self.listing)
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingCanceledEvent {
    pub r#type: String,
    listing: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingCanceledEvent {
    pub fn get_listing_address(&self) -> String {
        standardize_address(&self.listing)
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingPlacedEvent {
    pub r#type: String,
    listing: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingPlacedEvent {
    pub fn get_listing_address(&self) -> String {
        standardize_address(&self.listing)
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferPlacedEvent {
    collection_offer: String, // address
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_amount: BigDecimal,
    pub collection_metadata: CollectionMetadata,
}

impl CollectionOfferPlacedEvent {
    pub fn get_collection_offer_address(&self) -> String {
        standardize_address(&self.collection_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferCanceledEvent {
    collection_offer: String, // address
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub remaining_token_amount: BigDecimal,
    pub collection_metadata: CollectionMetadata,
}

impl CollectionOfferCanceledEvent {
    pub fn get_collection_offer_address(&self) -> String {
        standardize_address(&self.collection_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferFilledEvent {
    collection_offer: String, // address
    purchaser: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl CollectionOfferFilledEvent {
    pub fn get_collection_offer_address(&self) -> String {
        standardize_address(&self.collection_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferPlacedEvent {
    token_offer: String, // address
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl TokenOfferPlacedEvent {
    pub fn get_token_offer_address(&self) -> String {
        standardize_address(&self.token_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferCanceledEvent {
    token_offer: String, // address
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl TokenOfferCanceledEvent {
    pub fn get_token_offer_address(&self) -> String {
        standardize_address(&self.token_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenOfferFilledEvent {
    token_offer: String, // address
    purchaser: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl TokenOfferFilledEvent {
    pub fn get_token_offer_address(&self) -> String {
        standardize_address(&self.token_offer)
    }

    pub fn get_purchaser_address(&self) -> String {
        standardize_address(&self.purchaser)
    }

    pub fn get_seller_address(&self) -> String {
        standardize_address(&self.seller)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuctionBidEvent {
    listing: String,    // address
    new_bidder: String, // address
    pub new_bid: BigDecimal,
    new_end_time: BigDecimal,
    previous_bidder: Option<String>,
    previous_bid: Option<BigDecimal>,
    previous_end_time: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl AuctionBidEvent {
    pub fn get_listing_address(&self) -> String {
        standardize_address(&self.listing)
    }

    pub fn get_new_bidder_address(&self) -> String {
        standardize_address(&self.new_bidder)
    }

    pub fn get_previous_bidder_address(&self) -> Option<String> {
        if let Some(previous_bidder) = &self.previous_bidder {
            Some(standardize_address(previous_bidder))
        } else {
            None
        }
    }
}
