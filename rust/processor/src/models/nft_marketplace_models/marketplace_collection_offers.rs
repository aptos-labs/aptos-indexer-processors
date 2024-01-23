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

use crate::{schema::marketplace_collection_offers, utils::util::standardize_address};

use super::marketplace_utils::MarketplaceEvent;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(collection_offer_id, collection_id))]
#[diesel(table_name = marketplace_collection_offers)]
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
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl MarketplaceCollectionOffer {
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

            let token_offer = match marketplace_event {
                MarketplaceEvent::CollectionOfferCanceledEvent(marketplace_event) => {
                    Some(MarketplaceCollectionOffer {
                        collection_offer_id: marketplace_event.get_collection_offer_address(),
                        collection_id: marketplace_event.collection_metadata.get_collection_address().unwrap(),
                        fee_schedule_id: event.key.as_ref().unwrap().account_address,
                        buyer: marketplace_event.get_purchaser_address(),
                        item_price: marketplace_event.price.clone(),
                        remaining_token_amount: marketplace_event.remaining_token_amount.clone(),
                        expiration_time: BigDecimal::from(0),
                        token_standard: marketplace_event.collection_metadata.get_token_standard(),
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
                _ => {
                    None
                },
            };
            return Ok(token_offer);
        }
        Ok(None)
    }
}