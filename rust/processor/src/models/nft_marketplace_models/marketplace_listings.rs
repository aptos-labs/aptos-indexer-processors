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

use crate::{
    models::{
        token_models::token_utils::TokenTypeWrapper, token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::marketplace_listings,
    utils::util::standardize_address,
};

use super::marketplace_utils::{
    FixedPriceListing, ListingMetadata, MarketplaceEvent, TokenMetadata,
};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(listing_id, token_data_id))]
#[diesel(table_name = marketplace_listings)]
pub struct MarketplaceListing {
    pub listing_id: String,
    pub token_data_id: String,
    pub collection_id: String,
    pub fee_schedule_id: String,
    pub price: BigDecimal,
    pub token_amount: BigDecimal,
    pub token_standard: String,
    pub seller: Option<String>,
    pub is_deleted: bool,
    pub coin_type: Option<String>,
    pub marketplace: String,
    pub contract_address: String,
    pub entry_function_id_str: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl MarketplaceListing {
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
            let listing = match marketplace_event {
                MarketplaceEvent::ListingFilledEvent(marketplace_event) => {
                    if marketplace_event.r#type != "auction" {
                        Some(MarketplaceListing {
                            listing_id: marketplace_event.get_listing_address(),
                            token_data_id: marketplace_event.token_metadata.get_token_address(),
                            collection_id: marketplace_event
                                .token_metadata
                                .get_collection_address(),
                            fee_schedule_id: event.key.as_ref().unwrap().account_address.clone(),
                            price: marketplace_event.price.clone(),
                            token_amount: BigDecimal::from(0),
                            token_standard: marketplace_event.token_metadata.get_token_standard(),
                            seller: Some(marketplace_event.get_seller_address()),
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
                MarketplaceEvent::ListingCanceledEvent(marketplace_event) => {
                    Some(MarketplaceListing {
                        listing_id: marketplace_event.get_listing_address(),
                        token_data_id: marketplace_event.token_metadata.get_token_address(),
                        collection_id: marketplace_event.token_metadata.get_collection_address(),
                        fee_schedule_id: event.key.as_ref().unwrap().account_address.clone(),
                        price: marketplace_event.price.clone(),
                        token_amount: BigDecimal::from(0),
                        token_standard: marketplace_event.token_metadata.get_token_standard(),
                        seller: Some(marketplace_event.get_seller_address()),
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
                },
                _ => None,
            };
            return Ok(listing);
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
        fixed_price_listing_metadata: Option<&FixedPriceListing>,
        token_v1_container: Option<&TokenTypeWrapper>,
        token_v2_metadata: Option<&TokenMetadata>,
    ) -> anyhow::Result<Option<Self>> {
        let move_resource_address = standardize_address(&write_resource.address);
        let move_resource_type = write_resource.type_str.clone();
        if !move_resource_type.eq(format!("{}::listing::Listing", contract_address).as_str()) {
            return Ok(None);
        }

        let token_standard_value = if token_v1_container.is_some() {
            TokenStandard::V1
        } else {
            TokenStandard::V2
        };

        let listing = match token_standard_value {
            TokenStandard::V1 => {
                if let (Some(listing_metadata), Some(fixed_price_listing)) =
                    (listing_metadata, fixed_price_listing_metadata)
                {
                    let v1_container = token_v1_container.unwrap();
                    let token_v1_metadata = v1_container.token.id.clone();

                    let listing = MarketplaceListing {
                        listing_id: move_resource_address.clone(),
                        token_data_id: token_v1_metadata.token_data_id.to_id(),
                        collection_id: token_v1_metadata.token_data_id.get_collection_id(),
                        fee_schedule_id: listing_metadata.fee_schedule.get_reference_address(),
                        seller: Some(listing_metadata.get_seller_address()),
                        price: fixed_price_listing.price.clone(),
                        token_amount: v1_container.token.amount.clone(),
                        is_deleted: false,
                        token_standard: TokenStandard::V1.to_string(),
                        coin_type: coin_type.clone(),
                        marketplace: "example_marketplace".to_string(),
                        contract_address: contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                        entry_function_id_str: entry_function_id_str.clone().unwrap(),
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                    };

                    Some(listing)
                } else {
                    None
                }
            },
            TokenStandard::V2 => {
                if let (
                    Some(token_v2_metadata),
                    Some(listing_metadata),
                    Some(fixed_price_listing),
                ) = (
                    token_v2_metadata,
                    listing_metadata,
                    fixed_price_listing_metadata,
                ) {
                    let listing = MarketplaceListing {
                        listing_id: move_resource_address.clone(),
                        token_data_id: token_v2_metadata.get_token_address(),
                        collection_id: token_v2_metadata.get_collection_address(),
                        fee_schedule_id: listing_metadata
                            .fee_schedule
                            .get_reference_address(),
                        seller: Some(listing_metadata.get_seller_address()),
                        price: fixed_price_listing.price.clone(),
                        token_amount: BigDecimal::from(1),
                        is_deleted: false,
                        token_standard: TokenStandard::V2.to_string(),
                        coin_type: coin_type.clone(),
                        marketplace: "example_marketplace".to_string(),
                        contract_address: contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                        entry_function_id_str: entry_function_id_str.clone().unwrap(),
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                    };

                    Some(listing)
                } else {
                    None
                }
            },
        };

        Ok(listing)
    }
}
