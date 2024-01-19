// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use std::str::FromStr;

use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{utils::util::standardize_address, models::token_v2_models::v2_token_utils::TokenStandard};


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
    pub fn from_event_data(
        event_data: &serde_json::Value,
    ) -> anyhow::Result<Option<Self>> {
        let token_metadata = event_data["token_metadata"].clone();
        if token_metadata.is_null() {
            return Ok(None);
        }

        // V2 token will 
        let is_token_v2 = token_metadata["token"]["vec"].as_array().unwrap().len() > 0;
        let creator_address = standardize_address(token_metadata["creator_address"].as_str().unwrap());
        let property_version = if token_metadata["property_version"]["vec"].as_array().unwrap().len() > 0 {
            Some(BigDecimal::from_str(token_metadata["property_version"]["vec"][0].as_str().unwrap()).unwrap())
        } else {
            None
        };

        // V2 Token
        if is_token_v2 {
            let collection_id = standardize_address(token_metadata["collection"]["vec"][0]["inner"].as_str().unwrap());
            let token_data_id = standardize_address(token_metadata["token"]["vec"][0]["inner"].as_str().unwrap());
            let collection_name = token_metadata["collection_name"].as_str().unwrap().to_string();
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
                token_metadata["collection_name"].as_str().unwrap().to_string(),
                token_metadata["token_name"].as_str().unwrap().to_string(),
            );

            let collection_id = token_data_id_type.to_collection_id();
            let token_data_id = token_data_id_type.to_hash();
            let collection_name = token_metadata["collection_name"].as_str().unwrap().to_string();
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
    pub fn from_event_data(
        event_data: &serde_json::Value,
    ) -> anyhow::Result<Option<Self>> {
        let collection_metadata = event_data["collection_metadata"].clone();
        if collection_metadata.is_null() {
            return Ok(None);
        }

        let creator_address = standardize_address(collection_metadata["creator_address"].as_str().unwrap());
        let collection_id = standardize_address(collection_metadata["collection"]["vec"][0]["inner"].as_str().unwrap());
        let collection_v2 = collection_metadata["collection"]["vec"].as_array().unwrap();
        let collection_name = collection_metadata["collection_name"].as_str().unwrap().to_string();

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
            let collection_data_type = CollectionDataIdType::new(
                creator_address.clone(),
                collection_name.clone(),
            );

            Ok(Some(Self {
                collection_id: collection_data_type.to_hash(),
                creator_address,
                collection_name,
                token_standard,
            }))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenDataIdType {
    pub creator: String,
    pub collection_name: String,
    pub token_name: String,
}

impl TokenDataIdType {
    pub fn new (
        creator: String,
        collection_name: String,
        token_name: String,
    ) -> Self {
        Self {
            creator,
            collection_name,
            token_name,
        }
    }

    pub fn to_collection_id(&self) -> String {
        CollectionDataIdType::new(self.creator.clone(), self.collection_name.clone()).to_hash()
    }

    pub fn to_hash(&self) -> String {
        let key = format!("{}::{}::{}", self.creator, self.collection_name, self.token_name);
        standardize_address(key.as_str())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectionDataIdType {
    pub creator: String,
    pub collection_name: String,
}

impl CollectionDataIdType {
    pub fn new (
        creator: String,
        collection_name: String,
    ) -> Self {
        Self {
            creator,
            collection_name,
        }
    }

    pub fn to_hash(&self) -> String {
        let key = format!("{}::{}", self.creator, self.collection_name);
        standardize_address(key.as_str())
    }
}

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

pub struct FixedPriceListing {
    pub price: BigDecimal,
}

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