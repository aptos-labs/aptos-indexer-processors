// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use std::str::FromStr;

use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    models::{
        ans_models::ans_utils::OptionalString,
        fungible_asset_models::v2_fungible_asset_utils::OptionalBigDecimal,
        token_models::token_utils::{CollectionDataIdType, TokenDataIdType},
        token_v2_models::v2_token_utils::TokenStandard,
    },
    utils::util::{deserialize_from_string, standardize_address},
};
use anyhow::{Context, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MarketplaceTokenMetadata {
    pub collection_id: String,
    pub token_data_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub token_name: String,
    pub property_version: Option<BigDecimal>,
    pub token_standard: String,
}

impl MarketplaceTokenMetadata {
    pub fn from_event_data(event_data: &serde_json::Value) -> anyhow::Result<Option<Self>> {
        let token_metadata = event_data["token_metadata"].clone();
        if token_metadata.is_null() {
            return Ok(None);
        }

        // V2 token
        let is_token_v2 = token_metadata["token"]["vec"].as_array().unwrap().len() > 0;
        let creator_address =
            standardize_address(token_metadata["creator_address"].as_str().unwrap());
        let property_version = if token_metadata["property_version"]["vec"]
            .as_array()
            .unwrap()
            .len()
            > 0
        {
            Some(
                BigDecimal::from_str(
                    token_metadata["property_version"]["vec"][0]
                        .as_str()
                        .unwrap(),
                )
                .unwrap(),
            )
        } else {
            None
        };

        // V2 Token
        if is_token_v2 {
            let collection_id = standardize_address(
                token_metadata["collection"]["vec"][0]["inner"]
                    .as_str()
                    .unwrap(),
            );
            let token_data_id =
                standardize_address(token_metadata["token"]["vec"][0]["inner"].as_str().unwrap());
            let collection_name = token_metadata["collection_name"]
                .as_str()
                .unwrap()
                .to_string();
            let token_name = token_metadata["token_name"].as_str().unwrap().to_string();
            let token_standard = TokenStandard::V2.to_string();

            Ok(Some(Self {
                collection_id,
                token_data_id,
                creator_address,
                collection_name,
                token_name,
                property_version,
                token_standard,
            }))
        } else {
            // V1 Token
            let token_data_id_type = TokenDataIdType::new(
                creator_address.clone(),
                token_metadata["collection_name"]
                    .as_str()
                    .unwrap()
                    .to_string(),
                token_metadata["token_name"].as_str().unwrap().to_string(),
            );

            let collection_id = token_data_id_type.to_id();
            let token_data_id = token_data_id_type.to_hash();
            let collection_name = token_metadata["collection_name"]
                .as_str()
                .unwrap()
                .to_string();
            let token_name = token_metadata["token_name"].as_str().unwrap().to_string();
            let token_standard = TokenStandard::V1.to_string();

            Ok(Some(Self {
                collection_id,
                token_data_id,
                creator_address,
                collection_name,
                token_name,
                property_version,
                token_standard,
            }))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MarketplaceCollectionMetadata {
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub token_standard: String,
}

impl MarketplaceCollectionMetadata {
    pub fn from_event_data(event_data: &serde_json::Value) -> anyhow::Result<Option<Self>> {
        let collection_metadata = event_data["collection_metadata"].clone();
        if collection_metadata.is_null() {
            return Ok(None);
        }

        let creator_address =
            standardize_address(collection_metadata["creator_address"].as_str().unwrap());
        let collection_id = standardize_address(
            collection_metadata["collection"]["vec"][0]["inner"]
                .as_str()
                .unwrap(),
        );
        let collection_v2 = collection_metadata["collection"]["vec"].as_array().unwrap();
        let collection_name = collection_metadata["collection_name"]
            .as_str()
            .unwrap()
            .to_string();

        if collection_v2.len() > 0 {
            // is v2 collection
            let token_standard = TokenStandard::V2.to_string();

            Ok(Some(Self {
                collection_id,
                creator_address,
                collection_name,
                token_standard,
            }))
        } else {
            // is v1 collection
            let token_standard = TokenStandard::V1.to_string();
            let collection_data_type =
                CollectionDataIdType::new(creator_address.clone(), collection_name.clone());

            Ok(Some(Self {
                collection_id: collection_data_type.to_hash(),
                creator_address,
                collection_name,
                token_standard,
            }))
        }
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct TokenDataIdType {
//     pub creator: String,
//     pub collection_name: String,
//     pub token_name: String,
// }

// impl TokenDataIdType {
//     pub fn new (
//         creator: String,
//         collection_name: String,
//         token_name: String,
//     ) -> Self {
//         Self {
//             creator,
//             collection_name,
//             token_name,
//         }
//     }

//     pub fn to_collection_id(&self) -> String {
//         CollectionDataIdType::new(self.creator.clone(), self.collection_name.clone()).to_hash()
//     }

//     pub fn to_hash(&self) -> String {
//         let key = format!("{}::{}::{}", self.creator, self.collection_name, self.token_name);
//         standardize_address(key.as_str())
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferEventMetadata {
    pub collection_offer_id: String,
    pub collection_metadata: MarketplaceCollectionMetadata,
    pub item_price: BigDecimal,
    pub buyer: String,
    pub fee_schedule_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingMetadata {
    pub seller: String,
    pub fee_schedule_id: String,
    pub token_address: String,
}

#[derive(Debug, Clone)]
pub struct FixedPriceListing {
    pub price: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct ListingTokenV1Container {
    pub token_metadata: MarketplaceTokenMetadata,
    pub amount: BigDecimal,
}

pub struct TokenOfferMetadata {
    pub expiration_time: BigDecimal,
    pub price: BigDecimal,
    pub fee_schedule_id: String,
}

pub struct TokenOfferV1 {
    pub token_metadata: MarketplaceTokenMetadata,
}

pub struct TokenOfferV2 {
    pub token_address: String,
}

pub struct CollectionOfferMetadata {
    pub expiration_time: BigDecimal,
    pub item_price: BigDecimal,
    pub remaining_token_amount: BigDecimal,
    pub fee_schedule_id: String,
}

pub struct CollectionOfferV1 {
    pub collection_metadata: MarketplaceCollectionMetadata,
}

pub struct CollectionOfferV2 {
    pub collection_address: String,
}

pub struct AuctionListing {
    pub auction_end_time: BigDecimal,
    pub starting_bid_price: BigDecimal,
    pub current_bid_price: Option<BigDecimal>,
    pub current_bidder: Option<String>,
    pub buy_it_now_price: Option<BigDecimal>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenMetadata {
    creator_address: String,
    collection_name: String,
    collection: OptionalString,
    token_name: String,
    token: OptionalString,
    property_version: OptionalBigDecimal,
}

impl TokenMetadata {
    pub fn get_creator_address(&self) -> String {
        standardize_address(&self.creator_address)
    }

    pub fn get_collection_address(&self) -> Option<String> {
        if let Some(inner) = self.collection.get_string() {
            Some(standardize_address(&inner))
        } else {
            let token_data_id = TokenDataIdType::new(
                self.creator_address.clone(),
                self.collection_name.clone(),
                self.token_name.clone(),
            );
            Some(token_data_id.get_collection_id())
        }
    }

    pub fn get_token_address(&self) -> Option<String> {
        if let Some(inner) = self.token.get_string() {
            Some(standardize_address(&inner))
        } else {
            let token_data_id = TokenDataIdType::new(
                self.creator_address.clone(),
                self.collection_name.clone(),
                self.token_name.clone(),
            );
            Some(token_data_id.to_id())
        }
    }

    pub fn get_property_version(&self) -> Option<BigDecimal> {
        self.property_version.vec.first().map(|x| x.0.clone())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionMetadata {
    creator_address: String,
    pub collection_name: String,
    collection: OptionalString,
}

impl CollectionMetadata {
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
        if let Some(inner) = self.collection.get_string() {
            TokenStandard::V2.to_string()
        } else {
            TokenStandard::V1.to_string()
        }
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
    pub fn from_event(
        data_type: &str,
        data: &str,
        txn_version: i64,
        contract_addr: &str,
    ) -> Result<Option<Self>> {
        match data_type {
            x if x == format!("{}::event::ListingFilledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::ListingFilledEvent(inner)))
            },
            x if x == format!("{}::event::ListingCanceledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::ListingCanceledEvent(inner)))
            },
            x if x == format!("{}::event::ListingPlacedEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::ListingPlacedEvent(inner)))
            },
            x if x == format!("{}::event::CollectionOfferPlacedEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::CollectionOfferPlacedEvent(inner)))
            },
            x if x == format!("{}::event::CollectionOfferCanceledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::CollectionOfferCanceledEvent(inner)))
            },
            x if x == format!("{}::event::CollectionOfferFilledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::CollectionOfferFilledEvent(inner)))
            },
            x if x == format!("{}::event::TokenOfferPlacedEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::TokenOfferPlacedEvent(inner)))
            },
            x if x == format!("{}::event::TokenOfferCanceledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::TokenOfferCanceledEvent(inner)))
            },
            x if x == format!("{}::event::TokenOfferFilledEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::TokenOfferFilledEvent(inner)))
            },
            x if x == format!("{}::event::AuctionBidEvent", contract_addr) => {
                serde_json::from_str(data)
                    .map(|inner| Some(Self::AuctionBidEvent(inner)))
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
    token: String,
    listing: String,
    seller: String,
    purchaser: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingFilledEvent {
    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingCanceledEvent {
    token: String,
    listing: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingCanceledEvent {
    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListingPlacedEvent {
    token: String,
    listing: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl ListingPlacedEvent {
    pub fn get_token_address(&self) -> String {
        standardize_address(&self.token)
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
    pub fn get_collection_address(&self) -> String {
        standardize_address(&self.collection_offer)
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
    pub fn get_collection_address(&self) -> String {
        standardize_address(&self.collection_offer)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionOfferFilledEvent {
    collection_offer: String, // address
    purchaser: String,
    seller: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub price: BigDecimal,
    pub remaining_token_amount: BigDecimal,
    pub token_metadata: TokenMetadata,
}

impl CollectionOfferFilledEvent {
    pub fn get_collection_address(&self) -> String {
        standardize_address(&self.collection_offer)
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
}
