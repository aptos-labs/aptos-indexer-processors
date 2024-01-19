// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{nft_marketplace_models::{nft_marketplace_activities::{NftMarketplaceActivity, MarketplaceEventType}, nft_marketplace_utils::{MarketplaceTokenMetadata, MarketplaceCollectionMetadata, CollectionOfferEventMetadata, ListingMetadata, FixedPriceListing, ListingTokenV1Container, TokenOfferMetadata, TokenOfferV1, TokenOfferV2, CollectionOfferMetadata, CollectionOfferV1, CollectionOfferV2, AuctionListing, TokenDataIdType, CollectionDataIdType}, current_nft_marketplace_listings::{CurrentNftMarketplaceListing, self}, current_nft_marketplace_token_offers::CurrentNftMarketplaceTokenOffer, current_nft_marketplace_collection_offers::CurrentNftMarketplaceCollectionOffer, current_nft_marketplace_auctions::CurrentNftMarketplaceAuction}, token_v2_models::v2_token_utils::{ObjectCore, ObjectWithMetadata, TokenStandard}},
    schema::{self},
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::{get_entry_function_from_user_request, get_clean_payload, standardize_address},
    },
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction, write_set_change::Change};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::result::Error;
use field_count::FieldCount;
use gcloud_sdk::Token;
use std::{fmt::Debug, collections::HashMap, str::FromStr};
use tracing::error;

pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";
pub struct NftMarketplacesProcessor {
    connection_pool: PgDbPool,
}

impl NftMarketplacesProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for NftMarketplacesProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "NftMarketplacesProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    nft_marketplace_activities: &[NftMarketplaceActivity],
    nft_marketplace_listings: &[CurrentNftMarketplaceListing],
    nft_marketplace_token_offers: &[CurrentNftMarketplaceTokenOffer],
    nft_marketplace_collection_offers: &[CurrentNftMarketplaceCollectionOffer],
    nft_marketplace_auctions: &[CurrentNftMarketplaceAuction],
) -> Result<(), diesel::result::Error> {
    insert_nft_marketplace_activities(conn, nft_marketplace_activities).await?;
    insert_current_nft_marketplace_listings(conn, nft_marketplace_listings).await?;
    insert_current_nft_marketplace_token_offers(conn, nft_marketplace_token_offers).await?;
    insert_current_nft_marketplace_collection_offers(conn, nft_marketplace_collection_offers).await?;
    insert_current_nft_marketplace_auctions(conn, nft_marketplace_auctions).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    nft_marketplace_activities: Vec<NftMarketplaceActivity>,
    nft_marketplace_listings: Vec<CurrentNftMarketplaceListing>,
    nft_marketplace_token_offers: Vec<CurrentNftMarketplaceTokenOffer>,
    nft_marketplace_collection_offers: Vec<CurrentNftMarketplaceCollectionOffer>,
    nft_marketplace_auctions: Vec<CurrentNftMarketplaceAuction>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            Box::pin(insert_to_db_impl(
                pg_conn,
                &nft_marketplace_activities,
                &nft_marketplace_listings,
                &nft_marketplace_token_offers,
                &nft_marketplace_collection_offers,
                &nft_marketplace_auctions,
            ))
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| {
                    Box::pin(async {
                        let nft_marketplace_activities =
                            clean_data_for_db(nft_marketplace_activities, true);
                        let nft_marketplace_listings =
                            clean_data_for_db(nft_marketplace_listings, true);
                        let nft_marketplace_token_offers = 
                            clean_data_for_db(nft_marketplace_token_offers, true);
                        let nft_marketplace_collection_offers = 
                            clean_data_for_db(nft_marketplace_collection_offers, true);
                        let nft_marketplace_auctions =
                            clean_data_for_db(nft_marketplace_auctions, true);
                        insert_to_db_impl(
                            pg_conn,
                            &nft_marketplace_activities,
                            &nft_marketplace_listings,
                            &nft_marketplace_token_offers,
                            &nft_marketplace_collection_offers,
                            &nft_marketplace_auctions,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_nft_marketplace_activities(
    conn: &mut MyDbConnection,
    item_to_insert: &[NftMarketplaceActivity],
) -> Result<(), diesel::result::Error> {
    use schema::nft_marketplace_activities::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), NftMarketplaceActivity::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::nft_marketplace_activities::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, event_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_nft_marketplace_listings(
    conn: &mut MyDbConnection,
    item_to_insert: &[CurrentNftMarketplaceListing],
) -> Result<(), diesel::result::Error> {
    use schema::current_nft_marketplace_listings::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentNftMarketplaceListing::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_nft_marketplace_listings::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((listing_id, token_data_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_nft_marketplace_token_offers(
    conn: &mut MyDbConnection,
    item_to_insert: &[CurrentNftMarketplaceTokenOffer],
) -> Result<(), diesel::result::Error> {
    use schema::current_nft_marketplace_token_offers::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentNftMarketplaceTokenOffer::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_nft_marketplace_token_offers::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((offer_id, token_data_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_nft_marketplace_collection_offers(
    conn: &mut MyDbConnection,
    item_to_insert: &[CurrentNftMarketplaceCollectionOffer],
) -> Result<(), diesel::result::Error> {
    use schema::current_nft_marketplace_collection_offers::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentNftMarketplaceCollectionOffer::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_nft_marketplace_collection_offers::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((collection_offer_id, collection_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_nft_marketplace_auctions(
    conn: &mut MyDbConnection,
    item_to_insert: &[CurrentNftMarketplaceAuction],
) -> Result<(), diesel::result::Error> {
    use schema::current_nft_marketplace_auctions::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentNftMarketplaceAuction::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_nft_marketplace_auctions::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((listing_id, token_data_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for NftMarketplacesProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::NftMarketplacesProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;
        let (
            nft_marketplace_activities,
            current_nft_marketplace_listings,
            current_nft_marketplace_token_offers,
            current_nft_marketplace_collection_offers,
            current_nft_marketplace_auctions,
        ) = parse_transactions(&transactions).await;
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        // Start insert to db
        let db_insertion_start = std::time::Instant::now();
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            nft_marketplace_activities,
            current_nft_marketplace_listings,
            current_nft_marketplace_token_offers,
            current_nft_marketplace_collection_offers,
            current_nft_marketplace_auctions,
        )
        .await;
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
            }),
            Err(err) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    "[Parser] Error inserting nft marketplace transactions to db: {:?}",
                    err
                );
                bail!(format!("Error inserting nft marketplace transactions to db. Processor {}. Start {}. End {}. Error {:?}", self.name(), start_version, end_version, err))
            },
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

async fn parse_transactions(
    transactions: &[Transaction],
) -> (
    Vec<NftMarketplaceActivity>,
    Vec<CurrentNftMarketplaceListing>,
    Vec<CurrentNftMarketplaceTokenOffer>,
    Vec<CurrentNftMarketplaceCollectionOffer>,
    Vec<CurrentNftMarketplaceAuction>,
) {
    let mut nft_marketplace_activities = vec![];
    let mut nft_listings = vec![];
    let mut nft_token_offers = vec![];
    let mut nft_collection_offers = vec![];
    let mut nft_auctions = vec![];

    for txn in transactions {
        let txn_version = txn.version as i64;
        let txn_timestamp = txn
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");
        let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
        
        // Make sure this is a user transaction
        if let TxnData::User(inner) = txn_data {
            let payload = inner
                .request
                .as_ref()
                .expect("Getting user request failed.")
                .payload
                .as_ref()
                .expect("Getting payload failed.");
            let payload_cleaned = get_clean_payload(payload, txn_version).unwrap();
            let coin_type = if payload_cleaned["type_arguments"].as_array().unwrap().len() > 0 {
                let struct_type = &payload_cleaned["type_arguments"]
                    .as_array()
                    .unwrap()[0]["struct"];
                Some(format!(
                    "{}::{}::{}",
                    struct_type["address"].as_str().unwrap(),
                    struct_type["module"].as_str().unwrap(),
                    struct_type["name"].as_str().unwrap()
                ))
            } else {
                None
            };

            // TODO - We currently only index events from our own smart contracts
            // Need to update this code to include all marketplaces. e.g Topaz
            // Ideally, we will have a config file that lists all the marketplaces we want to index
            // And if there any new marketplace we want to index, we only need to update the config file
            // Though, the event type namings has to match exactly. e.g "ListingFilledEvent"
            let marketplace_contract_address = "0x6de37368e31dff4580b211295198159ee6f98b42ffa93c5683bb955ca1be67e0";
            let entry_function_id_str = get_entry_function_from_user_request(inner.request.as_ref().unwrap());

            // Get the events set. Only the events in the set will be indexed
            let events_set = MarketplaceEventType::get_events_set();

            // Token metadatas parsed from events. The key is generated token_data_id for token v1,
            // and token address for token v2.
            let token_metadatas: HashMap<String, MarketplaceTokenMetadata> = HashMap::new();
            // Collection metaddatas parsed from events. The key is generated collection_id for token v1,
            // and collection address for token v2.
            let collection_metadatas: HashMap<String, MarketplaceCollectionMetadata> = HashMap::new();

            let collection_offer_filled_metadatas: HashMap<String, CollectionOfferEventMetadata> = HashMap::new();

            // Loop through the events for the activities
            for (index, event) in inner.events.iter().enumerate() {
                // get event type and contract address
                let type_str_vec = event.type_str.as_str().split("::").collect::<Vec<&str>>();
                let contract_addr = type_str_vec[0];
                let event_type = type_str_vec[type_str_vec.len() - 1];

                // Only the events in the set will be indexed
                if !events_set.contains(event_type) {
                    continue;
                }
                
                // TODO - We currently only index events from our own smart contracts
                if !contract_addr.eq(marketplace_contract_address) {
                    continue;
                }

                let event_data = serde_json::from_str::<serde_json::Value>(&event.data).unwrap();
                let price = event_data["price"].as_str().expect("Error: Price missing from marketplace activity").parse::<BigDecimal>().unwrap();
                let activity_offer_or_listing_id = if let Some(listing) = event_data["listing"].as_str() {
                    standardize_address(listing)
                } else if let Some(token_offer) = event_data["token_offer"].as_str() {
                    standardize_address(token_offer)
                } else {
                    // collection_offer
                    standardize_address(event_data["collection_offer"].as_str().unwrap())
                };

                let activity_fee_schedule_id = event.key.as_ref().unwrap().account_address.as_str();
                let token_metadata = MarketplaceTokenMetadata::from_event_data(&event_data).unwrap();
                // TODO - for collection data
                let _collection_metadata = MarketplaceCollectionMetadata::from_event_data(&event_data).unwrap();

                let mut activity = Option::<NftMarketplaceActivity>::None;
                let mut current_listing = Option::<CurrentNftMarketplaceListing>::None;
                let mut current_token_offer = Option::<CurrentNftMarketplaceTokenOffer>::None;
                let mut current_collection_offer = Option::<CurrentNftMarketplaceCollectionOffer>::None;
                let mut current_auction: Option<CurrentNftMarketplaceAuction> = None;

                // create the activity based on the event type
                match event_type {
                    MarketplaceEventType::LISTING_PLACED_EVENT => {
                        // assert token_metadata exists
                        if token_metadata.is_none() {
                            continue;
                        }

                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                buyer: None,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::LISTING_PLACED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::LISTING_FILLED_EVENT => {
                        // assert token_metadata exists
                        let listing_type = event_data["type"].clone();
                        if token_metadata.is_none() || listing_type.is_null() {
                            continue;
                        }

                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                buyer: Some(standardize_address(event_data["buyer"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::LISTING_FILLED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            if listing_type == "auction" {
                                current_auction = Some(CurrentNftMarketplaceAuction {
                                    listing_id: activity_offer_or_listing_id,
                                    token_data_id: token_metadata.token_data_id,
                                    collection_id: token_metadata.collection_id,
                                    fee_schedule_id: activity_fee_schedule_id.to_string(),
                                    seller: standardize_address(event_data["seller"].as_str().unwrap()),
                                    current_bid_price: Some(price),
                                    current_bidder: Some(standardize_address(event_data["purcheser"].as_str().unwrap())),
                                    starting_bid_price: BigDecimal::from(1),
                                    buy_it_now_price: None,
                                    token_amount: BigDecimal::from(1),
                                    expiration_time: BigDecimal::from(0),
                                    is_deleted: true,
                                    token_standard: token_metadata.token_standard,
                                    coin_type: coin_type.clone(),
                                    marketplace: "example_marketplace".to_string(),
                                    contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                    entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                    last_transaction_version: txn_version,
                                    last_transaction_timestamp: txn_timestamp,
                                });
                            } else {
                                current_listing = Some(CurrentNftMarketplaceListing {
                                    listing_id: activity_offer_or_listing_id,
                                    token_data_id: token_metadata.token_data_id,
                                    collection_id: token_metadata.collection_id,
                                    fee_schedule_id: activity_fee_schedule_id.to_string(),
                                    seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                    price: price,
                                    token_amount: BigDecimal::from(0),
                                    is_deleted: true,
                                    token_standard: token_metadata.token_standard,
                                    coin_type: coin_type.clone(),
                                    marketplace: "example_marketplace".to_string(),
                                    contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                    entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                    last_transaction_version: txn_version,
                                    last_transaction_timestamp: txn_timestamp,
                                });
                            }
                        }
                    },
                    MarketplaceEventType::LISTING_CANCELED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                buyer: None,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::LISTING_CANCELED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            current_listing = Some(CurrentNftMarketplaceListing {
                                listing_id: activity_offer_or_listing_id,
                                token_data_id: token_metadata.token_data_id,
                                collection_id: token_metadata.collection_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                price: price,
                                token_amount: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: token_metadata.token_standard,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                last_transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::COLLECTION_OFFER_PLACED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: None,
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: None,
                                property_version: None,
                                price: price,
                                token_amount: event_data["token_amount"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                token_standard: token_metadata.token_standard,
                                seller: None,
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::COLLECTION_OFFER_PLACED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::COLLECTION_OFFER_CANCELED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: None,
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: None,
                                property_version: None,
                                price: price,
                                token_amount: event_data["token_amount"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                token_standard: token_metadata.token_standard,
                                seller: None,
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::COLLECTION_OFFER_CANCELED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            current_collection_offer = Some(CurrentNftMarketplaceCollectionOffer {
                                collection_offer_id: activity_offer_or_listing_id,
                                collection_id: token_metadata.collection_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                buyer: standardize_address(event_data["purchaser"].as_str().unwrap()),
                                item_price: price,
                                remaining_token_amount: event_data["remaining_token_amount"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                expiration_time: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: token_metadata.token_standard,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::COLLECTION_OFFER_FILLED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: match event_data.get("remaining_token_amount").is_none() {
                                    true => BigDecimal::from(0),
                                    false => event_data["remaining_token_amount"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                },
                                token_standard: token_metadata.token_standard,
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::COLLECTION_OFFER_FILLED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            let collectiion_offer_filled_metadata = CollectionOfferEventMetadata {
                                collection_offer_id: activity_offer_or_listing_id,
                                collection_metadata: MarketplaceCollectionMetadata {
                                    collection_id: token_metadata.collection_id,
                                    collection_name: token_metadata.collection_name,
                                    creator_address: token_metadata.creator_address,
                                    token_standard: token_metadata.token_standard,
                                },
                                item_price: price,
                                buyer: standardize_address(event_data["purchaser"].as_str().unwrap()),
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                            };
                            collection_offer_filled_metadatas.insert(activity_offer_or_listing_id.clone(), collectiion_offer_filled_metadata);
                        }
                    },
                    MarketplaceEventType::TOKEN_OFFER_PLACED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: None,
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::TOKEN_OFFER_PLACED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::TOKEN_OFFER_CANCELED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(0),
                                token_standard: token_metadata.token_standard,
                                seller: None,
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::TOKEN_OFFER_CANCELED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            current_token_offer = Some(CurrentNftMarketplaceTokenOffer {
                                offer_id: activity_offer_or_listing_id,
                                token_data_id: token_metadata.token_data_id,
                                collection_id: token_metadata.collection_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                price: price,
                                token_amount: BigDecimal::from(0),
                                expiration_time: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: token_metadata.token_standard,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                last_transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::TOKEN_OFFER_FILLED_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id.clone(),
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: price,
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: Some(standardize_address(event_data["seller"].as_str().unwrap())),
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::TOKEN_OFFER_FILLED_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });

                            current_token_offer = Some(CurrentNftMarketplaceTokenOffer {
                                offer_id: activity_offer_or_listing_id,
                                token_data_id: token_metadata.token_data_id,
                                collection_id: token_metadata.collection_id,
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                buyer: Some(standardize_address(event_data["purchaser"].as_str().unwrap())),
                                price: price,
                                token_amount: BigDecimal::from(0),
                                expiration_time: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: token_metadata.token_standard,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                last_transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    MarketplaceEventType::AUCTION_BID_EVENT => {
                        if let Some(token_metadata) = token_metadata {
                            activity = Some(NftMarketplaceActivity {
                                transaction_version: txn_version,
                                event_index: index as i64,
                                offer_or_listing_id: activity_offer_or_listing_id.clone(),
                                fee_schedule_id: activity_fee_schedule_id.to_string(),
                                collection_id: token_metadata.collection_id,
                                token_data_id: Some(token_metadata.token_data_id),
                                creator_address: token_metadata.creator_address,
                                collection_name: token_metadata.collection_name,
                                token_name: Some(token_metadata.token_name),
                                property_version: token_metadata.property_version,
                                price: event_data["new_bid"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                token_amount: BigDecimal::from(1),
                                token_standard: token_metadata.token_standard,
                                seller: None,
                                buyer: Some(standardize_address(event_data["new_bidder"].as_str().unwrap())),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                event_type: MarketplaceEventType::AUCTION_BID_EVENT.to_string(),
                                transaction_timestamp: txn_timestamp,
                            });
                        }
                    },
                    &_ => {
                        continue;
                    },
                }
                nft_marketplace_activities.push(activity.unwrap());
                if let Some(listing) = current_listing {
                    nft_listings.push(listing);
                }
                if let Some(token_offer) = current_token_offer {
                    nft_token_offers.push(token_offer);
                }
                if let Some(auction) = current_auction {
                    nft_auctions.push(auction);
                }
            }

            let mut object_metadatas: HashMap<String, ObjectCore> = HashMap::new();
            let mut listing_metadatas: HashMap<String, ListingMetadata> = HashMap::new();
            let mut fixed_price_listings: HashMap<String, FixedPriceListing> = HashMap::new();
            let mut listing_token_v1_containers: HashMap<String, ListingTokenV1Container> = HashMap::new();
            let mut token_offer_metadatas: HashMap<String, TokenOfferMetadata> = HashMap::new();
            let mut token_offer_v1s: HashMap<String, TokenOfferV1> = HashMap::new();
            let mut token_offer_v2s: HashMap<String, TokenOfferV2> = HashMap::new();
            let mut collection_offer_metadatas: HashMap<String, CollectionOfferMetadata> = HashMap::new();
            let mut collection_offer_v1s: HashMap<String, CollectionOfferV1> = HashMap::new();
            let mut collection_offer_v2s: HashMap<String, CollectionOfferV2> = HashMap::new();
            let mut auction_listings: HashMap<String, AuctionListing> = HashMap::new();

            // Loop 2
            // Parse out all the listing, auction, bid, and offer data from write set changes.
            // This is a bit more complicated than the other parsers because the data is spread out across multiple write set changes,
            // so we need a first loop to get all the data.
            for (wsc_index, wsc) in txn.info.unwrap().changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        let move_resource_address = standardize_address(&write_resource.address);
                        let move_resource_type = write_resource.type_str;
                        let move_resource_type_address = write_resource.r#type.as_ref().unwrap().address;
                        let data = serde_json::from_str::<serde_json::Value>(&write_resource.data).unwrap();

                        // Parse object metadata
                        let object_core_metadata = ObjectWithMetadata::from_write_resource(write_resource, txn_version).unwrap();
                        if let Some(object_core_metadata) = object_core_metadata {
                            object_metadatas.insert(move_resource_address.clone(), object_core_metadata.object_core);
                        }

                        if !move_resource_type_address.eq(marketplace_contract_address) {
                            continue;
                        }

                        // Parse listing metadata
                        let listing_metadata = if move_resource_type.eq(format!("{}::listing::Listing", marketplace_contract_address).as_str()) {
                            Some(ListingMetadata {
                                seller: standardize_address(data["seller"].as_str().unwrap()),
                                fee_schedule_id: standardize_address(data["fee_schedule"]["innder"].as_str().unwrap()),
                                token_address: standardize_address(data["object"]["inner"].as_str().unwrap()),
                            })
                        } else {
                            None
                        };
                        let fixed_price_listing = if move_resource_type.eq(format!("{}::coin_listing::FixedPriceListing", marketplace_contract_address).as_str()) {
                            Some(FixedPriceListing {
                                price: data["price"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                            })
                        } else {
                            None
                        };
                        let listing_token_v1_container = if move_resource_type.eq(format!("{}::listing::TokenV1Container", marketplace_contract_address).as_str()) {
                            let token = data.get("token");
                            let amount = if token.is_some() {
                                token.unwrap().get("amount").unwrap().to_string().parse::<BigDecimal>().unwrap()
                            } else {
                                BigDecimal::from(0)
                            };
                            let property_version = data["token"]["id"]["property_version"].as_str().unwrap();
                            let token_data_id_struct = data["token"]["id"]["token_data_id"];
                            let token_data_id_type = TokenDataIdType {
                                creator: token_data_id_struct.get("creator").unwrap().to_string(),
                                collection_name: token_data_id_struct.get("collection").unwrap().to_string(),
                                token_name: token_data_id_struct.get("name").unwrap().to_string(),
                            };

                            Some(ListingTokenV1Container {
                                token_metadata: MarketplaceTokenMetadata {
                                    collection_id: token_data_id_type.to_collection_id(),
                                    collection_name: token_data_id_type.collection_name, // TODO probably need to truncate this
                                    creator_address: standardize_address(token_data_id_type.creator.as_str()),
                                    token_data_id: token_data_id_type.to_hash(),
                                    token_name: token_data_id_type.token_name, // TODO probably need to cruncate this
                                    property_version: Some(BigDecimal::from_str(property_version).unwrap_or_default()),
                                    token_standard: TokenStandard::V1.to_string(),
                                },
                                amount: amount,
                            })
                        } else {
                            None
                        };

                        if let Some(listing_metadata) = listing_metadata {
                            listing_metadatas.insert(move_resource_address.clone(), listing_metadata);
                        }
                        if let Some(fixed_price_listing) = fixed_price_listing {
                            fixed_price_listings.insert(move_resource_address.clone(), fixed_price_listing);
                        }
                        if let Some(listing_token_v1_container) = listing_token_v1_container {
                            listing_token_v1_containers.insert(move_resource_address.clone(), listing_token_v1_container);
                        }

                        // Parse token offer metadata
                        let token_offer_metadata = if move_resource_type.eq(format!("{}::token_offer::TokenOffer", marketplace_contract_address).as_str()) {
                            Some(TokenOfferMetadata {
                                expiration_time: BigDecimal::from_str(data["expiration_time"].as_str().unwrap()).unwrap(),
                                price: BigDecimal::from_str(data["item_price"].as_str().unwrap()).unwrap(),
                                fee_schedule_id: standardize_address(data["fee_schedule"]["inner"].as_str().unwrap()),
                            })
                        } else {
                            None
                        };
                        let token_offer_v1 = if move_resource_type.eq(format!("{}::token_offer::TokenOfferTokenV1", marketplace_contract_address).as_str()) {
                            let property_version = data["property_version"].as_str().unwrap();
                            let token_data_id_type = TokenDataIdType {
                                creator: data["creator_address"].as_str().unwrap().to_string(),
                                collection_name: data["collection_name"].as_str().unwrap().to_string(),
                                token_name: data["token_name"].as_str().unwrap().to_string(),
                            };

                            Some(TokenOfferV1 {
                                token_metadata: MarketplaceTokenMetadata {
                                    collection_id: token_data_id_type.to_collection_id(),
                                    collection_name: token_data_id_type.collection_name, // TODO probably need to truncate this
                                    creator_address: standardize_address(token_data_id_type.creator.as_str()),
                                    token_data_id: token_data_id_type.to_hash(),
                                    token_name: token_data_id_type.token_name, // TODO probably need to cruncate this
                                    property_version: Some(BigDecimal::from_str(property_version).unwrap_or_default()),
                                    token_standard: TokenStandard::V1.to_string(),
                                },
                            })
                        } else {
                            None
                        };
                        let token_offer_v2 = if move_resource_type.eq(format!("{}::token_offer::TokenOfferTokenV2", marketplace_contract_address).as_str()) {
                            Some(TokenOfferV2 {
                                token_address: standardize_address(data["token"]["inner"].as_str().unwrap()),
                            })
                        } else {
                            None
                        };

                        if let Some(token_offer_metadata) = token_offer_metadata {
                            token_offer_metadatas.insert(move_resource_address.clone(), token_offer_metadata);
                        }
                        if let Some(token_offer_v1) = token_offer_v1 {
                            token_offer_v1s.insert(move_resource_address.clone(), token_offer_v1);
                        }
                        if let Some(token_offer_v2) = token_offer_v2 {
                            token_offer_v2s.insert(move_resource_address.clone(), token_offer_v2);
                        }

                        // Parse collection offer metadata
                        let collection_offer_metadata = if move_resource_type.eq(format!("{}::collection_offer::CollectionOffer", marketplace_contract_address).as_str()) {
                            Some(CollectionOfferMetadata {
                                expiration_time: BigDecimal::from_str(data["expiration_time"].as_str().unwrap()).unwrap(),
                                item_price: BigDecimal::from_str(data["item_price"].as_str().unwrap()).unwrap(),
                                remaining_token_amount: BigDecimal::from_str(data["remaining"].as_str().unwrap()).unwrap(),
                                fee_schedule_id: standardize_address(data["fee_schedule"]["inner"].as_str().unwrap()),
                            })
                        } else {
                            None
                        };
                        let collection_offer_v1 = if move_resource_type.eq(format!("{}::collection_offer::CollectionOfferTokenV1", marketplace_contract_address).as_str()) {
                            let collection_name = data["collection_name"].as_str().unwrap();
                            let creator_address = data["creator_address"].as_str().unwrap();
                            let collection_id_type = CollectionDataIdType {
                                creator: creator_address.to_string(),
                                collection_name: collection_name.to_string(),
                            };
                            Some(CollectionOfferV1 {
                                collection_metadata: MarketplaceCollectionMetadata {
                                    collection_id: collection_id_type.to_hash(),
                                    collection_name: collection_name.to_string(),
                                    creator_address: standardize_address(creator_address),
                                    token_standard: TokenStandard::V1.to_string(),
                                },
                            })
                        } else {
                            None
                        };
                        let collection_offer_v2 = if move_resource_type.eq(format!("{}::collection_offer::CollectionOfferTokenV2", marketplace_contract_address).as_str()) {
                            Some(CollectionOfferV2 {
                                collection_address: standardize_address(data["collection"]["inner"].as_str().unwrap()),
                            })
                        } else {
                            None
                        };

                        if let Some(collection_offer_metadata) = collection_offer_metadata {
                            collection_offer_metadatas.insert(move_resource_address.clone(), collection_offer_metadata);
                        }
                        if let Some(collection_offer_v1) = collection_offer_v1 {
                            collection_offer_v1s.insert(move_resource_address.clone(), collection_offer_v1);
                        }
                        if let Some(collection_offer_v2) = collection_offer_v2 {
                            collection_offer_v2s.insert(move_resource_address.clone(), collection_offer_v2);
                        }

                        // Parse auction metadata
                        let auction_listing = if move_resource_type.eq(format!("{}::coin_listing::AuctionListing", marketplace_contract_address).as_str()) {
                            let maybe_buy_it_now_price = data["buy_it_now_price"]["vec"].as_array();
                            let buy_it_now_price = if maybe_buy_it_now_price.is_some() && maybe_buy_it_now_price.unwrap().len() > 0 {
                                Some(BigDecimal::from_str(maybe_buy_it_now_price.unwrap()[0].as_str().unwrap()).unwrap())
                            } else {
                                None
                            };

                            let mut current_bid_price = None;
                            let mut current_bidder = None;
                            let maybe_current_bid = data["current_bid"]["vec"].as_array();
                            if maybe_current_bid.is_some() && maybe_current_bid.unwrap().len() > 0 {
                                let current_bid = maybe_current_bid.unwrap()[0].as_object().unwrap();
                                current_bid_price = Some(BigDecimal::from_str(current_bid["price"].as_str().unwrap()).unwrap());
                                current_bidder = Some(standardize_address(current_bid["bidder"].as_str().unwrap()));
                            }
                            
                            Some(AuctionListing {
                                auction_end_time: data["auction_end_time"].as_str().unwrap().parse::<BigDecimal>().unwrap(),
                                starting_bid_price: BigDecimal::from_str(data["starting_bid_price"].as_str().unwrap()).unwrap(),
                                current_bid_price: current_bid_price,
                                current_bidder: current_bidder,
                                buy_it_now_price: buy_it_now_price,
                            })
                        } else {
                            None
                        };

                        // Reconstruct the full listing and offer models and create DB objects
                        if let Some(auction_listing) = auction_listing {
                            auction_listings.insert(move_resource_address.clone(), auction_listing);
                        }

                    },
                    _ => continue,
                }
            }

            // Loop 3
            for (wsc_index, wsc) in txn.info.unwrap().changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        let move_type_address = write_resource.r#type.as_ref().unwrap().address;
                        if move_type_address != marketplace_contract_address {
                            continue;
                        }

                        let move_resource_address = standardize_address(&write_resource.address);
                        let move_resource_type = write_resource.type_str;

                        if move_resource_type.eq(format!("{}::listing::Listing", marketplace_contract_address).as_str()) {
                            // Get the data related to this listing that was parsed from loop 2
                            let listing_metadata = listing_metadatas.get(&move_resource_address);
                            let fixed_price_listing = fixed_price_listings.get(&move_resource_address);
                            let auction_listing = auction_listings.get(&move_resource_address);

                            // TODO Check if we should bail here
                            // if listing_metadata.is_none() {
                            //     bail!("Error: Listing metadata not found for transaction {}", txn_version);
                            // }

                            if let Some(auction_listing) = auction_listing {
                                let token_metadata = token_metadatas.get(&listing_metadata.as_ref().unwrap().token_address);
                                if let Some(token_metadata) = token_metadata {
                                    let current_auction = CurrentNftMarketplaceAuction {
                                        listing_id: move_resource_address.clone(),
                                        token_data_id: listing_metadata.unwrap().token_address.clone(),
                                        collection_id: token_metadata.collection_id,
                                        fee_schedule_id: listing_metadata.as_ref().unwrap().fee_schedule_id.to_string(),
                                        seller: listing_metadata.as_ref().unwrap().seller,
                                        current_bid_price: auction_listing.current_bid_price,
                                        current_bidder: auction_listing.current_bidder,
                                        starting_bid_price: auction_listing.starting_bid_price,
                                        buy_it_now_price: auction_listing.buy_it_now_price,
                                        token_amount: BigDecimal::from(1),
                                        expiration_time: auction_listing.auction_end_time,
                                        is_deleted: false,
                                        token_standard: TokenStandard::V2.to_string(),
                                        coin_type: coin_type.clone(),
                                        marketplace: "example_marketplace".to_string(),
                                        contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                        entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                        last_transaction_version: txn_version,
                                        last_transaction_timestamp: txn_timestamp,
                                    };
                                    nft_auctions.push(current_auction);
                                }
                            } else {
                                if let Some(fixed_price_listing) = fixed_price_listing {
                                    let token_address = listing_metadata.as_ref().unwrap().token_address.clone();
                                    let token_v1_container = listing_token_v1_containers.get(&token_address);

                                    let mut current_listing = None;
                                    if let Some(token_v1_container) = token_v1_container {
                                        let token_v1_metadata = token_v1_container.token_metadata.clone();
                                        current_listing = Some(CurrentNftMarketplaceListing {
                                            listing_id: move_resource_address.clone(),
                                            token_data_id: token_v1_metadata.token_data_id,
                                            collection_id: token_v1_metadata.collection_id,
                                            fee_schedule_id: listing_metadata.as_ref().unwrap().fee_schedule_id.to_string(),
                                            seller: Some(listing_metadata.as_ref().unwrap().seller),
                                            price: fixed_price_listing.price,
                                            token_amount: token_v1_container.amount,
                                            is_deleted: false,
                                            token_standard: TokenStandard::V1.to_string(),
                                            coin_type: coin_type.clone(),
                                            marketplace: "example_marketplace".to_string(),
                                            contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                            entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                            last_transaction_version: txn_version,
                                            last_transaction_timestamp: txn_timestamp,
                                        });
                                    } else {
                                        let token_v2_metadata = token_metadatas.get(&token_address);
                                        if let Some(token_v2_metadata) = token_v2_metadata {
                                            current_listing = Some(CurrentNftMarketplaceListing {
                                                listing_id: move_resource_address.clone(),
                                                token_data_id: token_v2_metadata.token_data_id,
                                                collection_id: token_v2_metadata.collection_id,
                                                fee_schedule_id: listing_metadata.as_ref().unwrap().fee_schedule_id.to_string(),
                                                seller: Some(listing_metadata.as_ref().unwrap().seller),
                                                price: fixed_price_listing.price,
                                                token_amount: BigDecimal::from(1),
                                                is_deleted: false,
                                                token_standard: TokenStandard::V2.to_string(),
                                                coin_type: coin_type.clone(),
                                                marketplace: "example_marketplace".to_string(),
                                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                                last_transaction_version: txn_version,
                                                last_transaction_timestamp: txn_timestamp,
                                            });
                                        }
                                    }

                                    nft_listings.push(current_listing.unwrap());
                                }
                            }
                        } else if move_resource_type.eq(format!("{}::token_offer::TokenOffer", marketplace_contract_address).as_str()) {
                            let token_offer_object = object_metadatas.get(&move_resource_address);
                            let token_offer_metadata = token_offer_metadatas.get(&move_resource_address);
                            let token_offer_v1 = token_offer_v1s.get(&move_resource_address);

                            // TODO - Check if we should bail here
                            // if token_offer_object.is_none() || token_offer_metadata.is_none() {
                            //     bail!("Error: Token offer metadata not found for transaction {}", txn_version);
                            // }

                            let mut current_token_offer = None;
                            if let Some(token_offer_v1) = token_offer_v1 {
                                let token_metadata = token_offer_v1.token_metadata.clone();
                                current_token_offer = Some(CurrentNftMarketplaceTokenOffer {
                                    offer_id: move_resource_address.clone(),
                                    token_data_id: token_metadata.token_data_id,
                                    collection_id: token_metadata.collection_id,
                                    fee_schedule_id: token_offer_metadata.unwrap().fee_schedule_id,
                                    buyer: Some(token_offer_object.unwrap().get_owner_address()),
                                    price: token_offer_metadata.unwrap().price,
                                    token_amount: BigDecimal::from(1),
                                    expiration_time: token_offer_metadata.unwrap().expiration_time,
                                    is_deleted: false,
                                    token_standard: TokenStandard::V1.to_string(),
                                    coin_type: coin_type.clone(),
                                    marketplace: "example_marketplace".to_string(),
                                    contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                    entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                    last_transaction_version: txn_version,
                                    last_transaction_timestamp: txn_timestamp,
                                });
                            } else {
                                let token_offer_v2 = token_offer_v2s.get(&move_resource_address);

                                if let Some(token_offer_v2) = token_offer_v2 {
                                    let token_v2_metadata = token_metadatas.get(&token_offer_v2.token_address);
                                    if let Some(token_v2_metadata) = token_v2_metadata {
                                        current_token_offer = Some(CurrentNftMarketplaceTokenOffer {
                                            offer_id: move_resource_address.clone(),
                                            token_data_id: token_offer_v2.token_address,
                                            collection_id: token_v2_metadata.collection_id,
                                            fee_schedule_id: token_offer_metadata.unwrap().fee_schedule_id,
                                            buyer: Some(token_offer_object.unwrap().get_owner_address()),
                                            price: token_offer_metadata.unwrap().price,
                                            token_amount: BigDecimal::from(1),
                                            expiration_time: token_offer_metadata.unwrap().expiration_time,
                                            is_deleted: false,
                                            token_standard: TokenStandard::V2.to_string(),
                                            coin_type: coin_type.clone(),
                                            marketplace: "example_marketplace".to_string(),
                                            contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                            entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                            last_transaction_version: txn_version,
                                            last_transaction_timestamp: txn_timestamp,
                                        });
                                    }
                                }
                            }
                            nft_token_offers.push(current_token_offer.unwrap());
                        } else if move_resource_type.eq(format!("{}::collection_offer::CollectionOffer", marketplace_contract_address).as_str()) {
                            let collection_object = object_metadatas.get(&move_resource_address);
                            let collection_offer_metadata = collection_offer_metadatas.get(&move_resource_address);
                            let collection_offer_v1 = collection_offer_v1s.get(&move_resource_address);

                            // TODO - Check if we should bail here
                            // if collection_offer_object.is_none() || collection_offer_metadata.is_none() {
                            //     bail!("Error: Collection offer metadata not found for transaction {}", txn_version);
                            // }

                            let mut current_collection_offer = None;
                            if let Some(collection_offer_v1) = collection_offer_v1 {
                                current_collection_offer = Some(CurrentNftMarketplaceCollectionOffer {
                                    collection_offer_id: move_resource_address.clone(),
                                    collection_id: collection_offer_v1.collection_metadata.collection_id,
                                    fee_schedule_id: collection_offer_metadata.unwrap().fee_schedule_id,
                                    buyer: collection_object.unwrap().get_owner_address(),
                                    item_price: collection_offer_metadata.unwrap().item_price,
                                    remaining_token_amount: collection_offer_metadata.unwrap().remaining_token_amount,
                                    expiration_time: collection_offer_metadata.unwrap().expiration_time,
                                    is_deleted: false,
                                    token_standard: TokenStandard::V1.to_string(),
                                    coin_type: coin_type.clone(),
                                    marketplace: "example_marketplace".to_string(),
                                    contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                    entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                    last_transaction_version: txn_version,
                                    transaction_timestamp: txn_timestamp,
                                });
                            } else {
                                let collection_offer_v2 = collection_offer_v2s.get(&move_resource_address);
                                if let Some(collection_ofer_v2) = collection_offer_v2 {

                                    current_collection_offer = Some(CurrentNftMarketplaceCollectionOffer {
                                        collection_offer_id: move_resource_address.clone(),
                                        collection_id: collection_offer_v2.unwrap().collection_address,
                                        fee_schedule_id: collection_offer_metadata.unwrap().fee_schedule_id,
                                        buyer: collection_object.unwrap().get_owner_address(),
                                        item_price: collection_offer_metadata.unwrap().item_price,
                                        remaining_token_amount: collection_offer_metadata.unwrap().remaining_token_amount,
                                        expiration_time: collection_offer_metadata.unwrap().expiration_time,
                                        is_deleted: false,
                                        token_standard: TokenStandard::V2.to_string(),
                                        coin_type: coin_type.clone(),
                                        marketplace: "example_marketplace".to_string(),
                                        contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                        entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                        last_transaction_version: txn_version,
                                        transaction_timestamp: txn_timestamp,
                                    });
                                }
                            }
                            nft_collection_offers.push(current_collection_offer.unwrap());
                        }
                                
                    },
                    Change::DeleteResource(delete_resource) => {
                        let move_resource_address = standardize_address(&delete_resource.address);

                        // If a collection offer resource gets deleted, that means it the offer was filled completely
                        // and we handle that here.
                        let maybe_collection_offer_filled_metadata = collection_offer_filled_metadatas.get(&move_resource_address);
                        if let Some(maybe_collection_offer_filled_metadata) = maybe_collection_offer_filled_metadata {
                            let collection_metadata = maybe_collection_offer_filled_metadata.collection_metadata.clone();
                            let current_collection_offer = CurrentNftMarketplaceCollectionOffer {
                                collection_offer_id: move_resource_address.clone(),
                                collection_id: collection_metadata.collection_id,
                                fee_schedule_id: maybe_collection_offer_filled_metadata.fee_schedule_id.clone(),
                                buyer: maybe_collection_offer_filled_metadata.buyer,
                                item_price: maybe_collection_offer_filled_metadata.item_price,
                                remaining_token_amount: BigDecimal::from(0),
                                expiration_time: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: collection_metadata.token_standard,
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                transaction_timestamp: txn_timestamp,
                            };
                            nft_collection_offers.push(current_collection_offer);
                        }
                    },
                    _ => continue,
                }
            }
        }
    }
    
    (
        nft_marketplace_activities, 
        nft_listings, 
        nft_token_offers, 
        nft_collection_offers, 
        nft_auctions
    )
}
