// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        nft_marketplace_models::{
            marketplace_activities::MarketplaceActivity,
            marketplace_auctions::MarketplaceAuction,
            marketplace_collection_offers::MarketplaceCollectionOffer,
            marketplace_listings::MarketplaceListing,
            marketplace_token_offers::MarketplaceTokenOffer,
            marketplace_utils::{
                AuctionListing, CollectionMetadata, CollectionOfferEventMetadata,
                CollectionOfferMetadata, CollectionOfferV1, CollectionOfferV2, FixedPriceListing,
                ListingMetadata, MarketplaceEvent, TokenMetadata, TokenOfferMetadata, TokenOfferV1,
                TokenOfferV2,
            },
        },
        token_models::token_utils::TokenTypeWrapper,
        token_v2_models::v2_token_utils::{ObjectCore, ObjectWithMetadata, TokenStandard},
    },
    schema::{self},
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::{get_clean_payload, get_entry_function_from_user_request, standardize_address},
    },
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::result::Error;
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";
pub struct MarketplacesProcessor {
    connection_pool: PgDbPool,
}

impl MarketplacesProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for MarketplacesProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "MarketplacesProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    marketplace_activities: &[MarketplaceActivity],
    nft_marketplace_listings: &[MarketplaceListing],
    nft_marketplace_token_offers: &[MarketplaceTokenOffer],
    nft_marketplace_collection_offers: &[MarketplaceCollectionOffer],
    nft_marketplace_auctions: &[MarketplaceAuction],
) -> Result<(), diesel::result::Error> {
    insert_marketplace_activities(conn, marketplace_activities).await?;
    insert_marketplace_listings(conn, nft_marketplace_listings).await?;
    insert_marketplace_token_offers(conn, nft_marketplace_token_offers).await?;
    insert_marketplace_collection_offers(conn, nft_marketplace_collection_offers).await?;
    insert_marketplace_auctions(conn, nft_marketplace_auctions).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    marketplace_activities: Vec<MarketplaceActivity>,
    nft_marketplace_listings: Vec<MarketplaceListing>,
    nft_marketplace_token_offers: Vec<MarketplaceTokenOffer>,
    nft_marketplace_collection_offers: Vec<MarketplaceCollectionOffer>,
    nft_marketplace_auctions: Vec<MarketplaceAuction>,
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
                &marketplace_activities,
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
                        let marketplace_activities =
                            clean_data_for_db(marketplace_activities, true);
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
                            &marketplace_activities,
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

async fn insert_marketplace_activities(
    conn: &mut MyDbConnection,
    item_to_insert: &[MarketplaceActivity],
) -> Result<(), diesel::result::Error> {
    use schema::marketplace_activities::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), MarketplaceActivity::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_activities::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, event_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_marketplace_listings(
    conn: &mut MyDbConnection,
    item_to_insert: &[MarketplaceListing],
) -> Result<(), diesel::result::Error> {
    use schema::marketplace_listings::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), MarketplaceListing::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_listings::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((listing_id, token_data_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_marketplace_token_offers(
    conn: &mut MyDbConnection,
    item_to_insert: &[MarketplaceTokenOffer],
) -> Result<(), diesel::result::Error> {
    use schema::marketplace_token_offers::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), MarketplaceTokenOffer::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_token_offers::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((offer_id, token_data_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_marketplace_collection_offers(
    conn: &mut MyDbConnection,
    item_to_insert: &[MarketplaceCollectionOffer],
) -> Result<(), diesel::result::Error> {
    use schema::marketplace_collection_offers::dsl::*;

    let chunks = get_chunks(
        item_to_insert.len(),
        MarketplaceCollectionOffer::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_collection_offers::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((collection_offer_id, collection_id))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_marketplace_auctions(
    conn: &mut MyDbConnection,
    item_to_insert: &[MarketplaceAuction],
) -> Result<(), diesel::result::Error> {
    use schema::marketplace_auctions::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), MarketplaceAuction::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::marketplace_auctions::table)
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
impl ProcessorTrait for MarketplacesProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::MarketplacesProcessor.into()
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
            marketplace_activities,
            marketplace_listings,
            marketplace_token_offers,
            marketplace_collection_offers,
            marketplace_auctions,
        ) = parse_transactions(&transactions).await;
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        // Start insert to db
        let db_insertion_start = std::time::Instant::now();
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            marketplace_activities,
            marketplace_listings,
            marketplace_token_offers,
            marketplace_collection_offers,
            marketplace_auctions,
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
    Vec<MarketplaceActivity>,
    Vec<MarketplaceListing>,
    Vec<MarketplaceTokenOffer>,
    Vec<MarketplaceCollectionOffer>,
    Vec<MarketplaceAuction>,
) {
    let mut marketplace_activities = vec![];
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
                let struct_type =
                    &payload_cleaned["type_arguments"].as_array().unwrap()[0]["struct"];
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
            let marketplace_contract_address =
                "0x6de37368e31dff4580b211295198159ee6f98b42ffa93c5683bb955ca1be67e0";
            let entry_function_id_str =
                get_entry_function_from_user_request(inner.request.as_ref().unwrap());

            // Get the events set. Only the events in the set will be indexed
            let events_set = MarketplaceEvent::get_events_set();

            // Token metadatas parsed from events. The key is generated token_data_id for token v1,
            // and token address for token v2.
            let mut token_metadatas: HashMap<String, TokenMetadata> = HashMap::new();
            // Collection metaddatas parsed from events. The key is generated collection_id for token v1,
            // and collection address for token v2.
            let mut collection_metadatas: HashMap<String, CollectionMetadata> = HashMap::new();

            let mut collection_offer_filled_metadatas: HashMap<
                String,
                CollectionOfferEventMetadata,
            > = HashMap::new();
            // Loop through the events for the all activities
            for (index, event) in inner.events.iter().enumerate() {
                // get event type and contract address
                let type_str_vec = event.type_str.as_str().split("::").collect::<Vec<&str>>();
                let contract_addr = type_str_vec[0];
                let event_type = type_str_vec[type_str_vec.len() - 1];

                // Only the events in the set will be indexed
                // TODO - We currently only index events from our own smart contracts
                if !events_set.contains(event_type)
                    || !contract_addr.eq(marketplace_contract_address)
                {
                    continue;
                }

                // Get the acitivty, listing, token_offer, and auction from event.
                let (activity, token_metadata, collection_metadata) =
                    MarketplaceActivity::from_event(
                        &event,
                        index as i64,
                        txn_version,
                        txn_timestamp,
                        &entry_function_id_str,
                        &marketplace_contract_address,
                        &coin_type,
                    )
                    .unwrap();
                if let Some(token_metadata) = token_metadata {
                    token_metadatas
                        .insert(token_metadata.get_token_address(), token_metadata.clone());
                }
                if let Some(collection_metadata) = &collection_metadata {
                    collection_metadatas.insert(
                        collection_metadata.get_collection_address().unwrap(),
                        collection_metadata.clone(),
                    );
                }

                if let Some(activity) = activity {
                    marketplace_activities.push(activity);
                }
                if let Some(listing) = MarketplaceListing::from_event(
                    &event,
                    txn_version,
                    txn_timestamp,
                    &entry_function_id_str,
                    &marketplace_contract_address,
                    &coin_type,
                )
                .unwrap()
                {
                    nft_listings.push(listing);
                }
                if let Some(token_offer) = MarketplaceTokenOffer::from_event(
                    &event,
                    txn_version,
                    txn_timestamp,
                    &entry_function_id_str,
                    &marketplace_contract_address,
                    &coin_type,
                )
                .unwrap()
                {
                    nft_token_offers.push(token_offer);
                }
                if let Some(auction) = MarketplaceAuction::from_event(
                    &event,
                    txn_version,
                    txn_timestamp,
                    &entry_function_id_str,
                    &marketplace_contract_address,
                    &coin_type,
                )
                .unwrap()
                {
                    nft_auctions.push(auction);
                }
                if let Some(collection_offer) = MarketplaceCollectionOffer::from_event(
                    &event,
                    txn_version,
                    txn_timestamp,
                    &entry_function_id_str,
                    &marketplace_contract_address,
                    &coin_type,
                )
                .unwrap()
                {
                    // The collection offer resource may be deleted after it is filled,
                    // so we need to parse collection offer metadata from the event
                    let collection_offer_filled_metadata = CollectionOfferEventMetadata {
                        collection_offer_id: collection_offer.collection_offer_id.clone(),
                        fee_schedule_id: collection_offer.fee_schedule_id.clone(),
                        buyer: collection_offer.buyer.clone(),
                        item_price: collection_offer.item_price.clone(),
                        collection_metadata: collection_metadata.clone().unwrap(),
                    };
                    collection_offer_filled_metadatas.insert(
                        collection_offer_filled_metadata.collection_offer_id.clone(),
                        collection_offer_filled_metadata,
                    );

                    nft_collection_offers.push(collection_offer);
                }
            }

            let mut object_metadatas: HashMap<String, ObjectCore> = HashMap::new();
            let mut listing_metadatas: HashMap<String, ListingMetadata> = HashMap::new();
            let mut fixed_price_listings: HashMap<String, FixedPriceListing> = HashMap::new();
            let mut listing_token_v1_containers: HashMap<String, TokenTypeWrapper> = HashMap::new(); // String to ListingTokenV1Container
            let mut token_offer_metadatas: HashMap<String, TokenOfferMetadata> = HashMap::new();
            let mut token_offer_v1s: HashMap<String, TokenOfferV1> = HashMap::new();
            let mut token_offer_v2s: HashMap<String, TokenOfferV2> = HashMap::new();
            let mut collection_offer_metadatas: HashMap<String, CollectionOfferMetadata> =
                HashMap::new();
            let mut collection_offer_v1s: HashMap<String, CollectionOfferV1> = HashMap::new();
            let mut collection_offer_v2s: HashMap<String, CollectionOfferV2> = HashMap::new();
            let mut auction_listings: HashMap<String, AuctionListing> = HashMap::new();

            // Loop 2
            // Parse out all the listing, auction, bid, and offer data from write set changes.
            // This is a bit more complicated than the other parsers because the data is spread out across multiple write set changes,
            // so we need a first loop to get all the data.
            for (_wsc_index, wsc) in txn.info.as_ref().unwrap().changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        let move_resource_address = standardize_address(&write_resource.address);
                        let move_resource_type_address =
                            write_resource.r#type.as_ref().unwrap().address.clone();
                        if !move_resource_type_address.eq(marketplace_contract_address) {
                            continue;
                        }

                        // 1. Parse object metadata
                        if let Some(object_core_metadata) =
                            ObjectWithMetadata::from_write_resource(write_resource, txn_version)
                                .unwrap()
                        {
                            object_metadatas.insert(
                                move_resource_address.clone(),
                                object_core_metadata.object_core,
                            );
                        }

                        // 2. Parse listing metadatas
                        if let Some(listing_metadata) = ListingMetadata::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            listing_metadatas
                                .insert(move_resource_address.clone(), listing_metadata);
                        }
                        if let Some(fixed_price_listing) = FixedPriceListing::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            fixed_price_listings
                                .insert(move_resource_address.clone(), fixed_price_listing);
                        }
                        if let Some(listing_token_v1_container) =
                            TokenTypeWrapper::from_marketplace_write_resource(
                                write_resource,
                                marketplace_contract_address,
                                txn_version,
                            )
                            .unwrap()
                        {
                            listing_token_v1_containers
                                .insert(move_resource_address.clone(), listing_token_v1_container);
                        }

                        // 3. Parse token offer metadata
                        if let Some(token_offer_metadata) = TokenOfferMetadata::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            token_offer_metadatas
                                .insert(move_resource_address.clone(), token_offer_metadata);
                        }
                        if let Some(token_offer_v1) = TokenOfferV1::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            token_offer_v1s.insert(move_resource_address.clone(), token_offer_v1);
                        }
                        if let Some(token_offer_v2) = TokenOfferV2::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            token_offer_v2s.insert(move_resource_address.clone(), token_offer_v2);
                        }

                        // 4. Parse collection offer metadata
                        if let Some(collection_offer_metadata) =
                            CollectionOfferMetadata::from_write_resource(
                                &write_resource,
                                marketplace_contract_address,
                                txn_version,
                            )
                            .unwrap()
                        {
                            collection_offer_metadatas
                                .insert(move_resource_address.clone(), collection_offer_metadata);
                        }
                        if let Some(collection_offer_v1) = CollectionOfferV1::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            collection_offer_v1s
                                .insert(move_resource_address.clone(), collection_offer_v1);
                        }
                        if let Some(collection_offer_v2) = CollectionOfferV2::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            collection_offer_v2s
                                .insert(move_resource_address.clone(), collection_offer_v2);
                        }

                        // Parse auction metadata
                        if let Some(auction_listing) = AuctionListing::from_write_resource(
                            &write_resource,
                            marketplace_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            auction_listings.insert(move_resource_address.clone(), auction_listing);
                        }
                    },
                    _ => continue,
                }
            }

            // Loop 3
            // Reconstruct the full listing and offer models and create DB objects
            for (_wsc_index, wsc) in txn.info.as_ref().unwrap().changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        let move_type_address =
                            write_resource.r#type.as_ref().unwrap().address.clone();
                        if !move_type_address.eq(marketplace_contract_address) {
                            continue;
                        }

                        let move_resource_address = standardize_address(&write_resource.address);
                        let move_resource_type = write_resource.type_str.clone();
                        if move_resource_type.eq(format!(
                            "{}::listing::Listing",
                            marketplace_contract_address
                        )
                        .as_str())
                        {
                            // Get the data related to this listing that was parsed from loop 2
                            let listing_metadata = listing_metadatas.get(&move_resource_address);
                            let fixed_price_listing =
                                fixed_price_listings.get(&move_resource_address);
                            let auction_listing = auction_listings.get(&move_resource_address);

                            if let Some(auction_listing) = auction_listing {
                                let token_metadata = token_metadatas.get(
                                    &listing_metadata
                                        .as_ref()
                                        .unwrap()
                                        .object
                                        .get_reference_address(),
                                );
                                if let Some(token_metadata) = token_metadata {
                                    let current_auction = MarketplaceAuction {
                                        listing_id: move_resource_address.clone(),
                                        token_data_id: listing_metadata
                                            .as_ref()
                                            .unwrap()
                                            .object
                                            .get_reference_address(),
                                        collection_id: token_metadata.get_collection_address(),
                                        fee_schedule_id: listing_metadata
                                            .as_ref()
                                            .unwrap()
                                            .fee_schedule
                                            .get_reference_address(),
                                        seller: listing_metadata
                                            .as_ref()
                                            .unwrap()
                                            .get_seller_address(),
                                        bid_price: Some(auction_listing.get_current_bid_price()),
                                        bidder: Some(auction_listing.get_current_bidder()),
                                        starting_bid_price: auction_listing.starting_bid.clone(),
                                        buy_it_now_price: auction_listing.get_buy_it_now_price(),
                                        token_amount: BigDecimal::from(1),
                                        expiration_time: auction_listing.auction_end_time.clone(),
                                        is_deleted: false,
                                        token_standard: TokenStandard::V2.to_string(),
                                        coin_type: coin_type.clone(),
                                        marketplace: "example_marketplace".to_string(),
                                        contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                        entry_function_id_str: entry_function_id_str
                                            .clone()
                                            .unwrap(),
                                        last_transaction_version: txn_version,
                                        last_transaction_timestamp: txn_timestamp,
                                    };
                                    nft_auctions.push(current_auction);
                                }
                            } else {
                                if let Some(fixed_price_listing) = fixed_price_listing {
                                    let token_address = listing_metadata
                                        .as_ref()
                                        .unwrap()
                                        .object
                                        .get_reference_address();
                                    let token_v1_container =
                                        listing_token_v1_containers.get(&token_address);

                                    let mut current_listing = None;
                                    if let Some(token_v1_container) = token_v1_container {
                                        let token_v1_metadata = token_v1_container.token.id.clone();
                                        current_listing = Some(MarketplaceListing {
                                            listing_id: move_resource_address.clone(),
                                            token_data_id: token_v1_metadata.token_data_id.to_id(),
                                            collection_id: token_v1_metadata
                                                .token_data_id
                                                .get_collection_id(),
                                            fee_schedule_id: listing_metadata
                                                .as_ref()
                                                .unwrap()
                                                .fee_schedule
                                                .get_reference_address(),
                                            seller: Some(
                                                listing_metadata
                                                    .as_ref()
                                                    .unwrap()
                                                    .get_seller_address(),
                                            ),
                                            price: fixed_price_listing.price.clone(),
                                            token_amount: token_v1_container.token.amount.clone(),
                                            is_deleted: false,
                                            token_standard: TokenStandard::V1.to_string(),
                                            coin_type: coin_type.clone(),
                                            marketplace: "example_marketplace".to_string(),
                                            contract_address: marketplace_contract_address
                                                .to_string(), // TODO - update this to the actual marketplace contract address
                                            entry_function_id_str: entry_function_id_str
                                                .clone()
                                                .unwrap(),
                                            last_transaction_version: txn_version,
                                            last_transaction_timestamp: txn_timestamp,
                                        });
                                    } else {
                                        let token_v2_metadata = token_metadatas.get(&token_address);
                                        if let Some(token_v2_metadata) = token_v2_metadata {
                                            current_listing = Some(MarketplaceListing {
                                                listing_id: move_resource_address.clone(),
                                                token_data_id: token_v2_metadata
                                                    .get_token_address(),
                                                collection_id: token_v2_metadata
                                                    .get_collection_address(),
                                                fee_schedule_id: listing_metadata
                                                    .as_ref()
                                                    .unwrap()
                                                    .fee_schedule
                                                    .get_reference_address(),
                                                seller: Some(
                                                    listing_metadata
                                                        .as_ref()
                                                        .unwrap()
                                                        .get_seller_address(),
                                                ),
                                                price: fixed_price_listing.price.clone(),
                                                token_amount: BigDecimal::from(1),
                                                is_deleted: false,
                                                token_standard: TokenStandard::V2.to_string(),
                                                coin_type: coin_type.clone(),
                                                marketplace: "example_marketplace".to_string(),
                                                contract_address: marketplace_contract_address
                                                    .to_string(), // TODO - update this to the actual marketplace contract address
                                                entry_function_id_str: entry_function_id_str
                                                    .clone()
                                                    .unwrap(),
                                                last_transaction_version: txn_version,
                                                last_transaction_timestamp: txn_timestamp,
                                            });
                                        }
                                    }
                                    nft_listings.push(current_listing.unwrap());
                                }
                            }
                        } else if move_resource_type.eq(format!(
                            "{}::token_offer::TokenOffer",
                            marketplace_contract_address
                        )
                        .as_str())
                        {
                            let token_offer_object = object_metadatas.get(&move_resource_address);
                            let token_offer_metadata =
                                token_offer_metadatas.get(&move_resource_address);
                            let token_offer_v1 = token_offer_v1s.get(&move_resource_address);

                            let mut current_token_offer = None;
                            if let Some(token_offer_v1) = token_offer_v1 {
                                let token_data_id = token_offer_v1.get_token_data_id();
                                current_token_offer = Some(MarketplaceTokenOffer {
                                    offer_id: move_resource_address.clone(),
                                    token_data_id: token_data_id.to_id(),
                                    collection_id: token_data_id.get_collection_id(),
                                    fee_schedule_id: token_offer_metadata
                                        .unwrap()
                                        .fee_schedule
                                        .get_reference_address(),
                                    buyer: Some(token_offer_object.unwrap().get_owner_address()),
                                    price: token_offer_metadata.unwrap().item_price.clone(),
                                    token_amount: BigDecimal::from(1),
                                    expiration_time: token_offer_metadata
                                        .unwrap()
                                        .expiration_time
                                        .clone(),
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
                                    let token_v2_metadata = token_metadatas
                                        .get(&token_offer_v2.token.get_reference_address());
                                    if let Some(token_v2_metadata) = token_v2_metadata {
                                        current_token_offer = Some(MarketplaceTokenOffer {
                                            offer_id: move_resource_address.clone(),
                                            token_data_id: token_v2_metadata.get_token_address(),
                                            collection_id: token_v2_metadata
                                                .get_collection_address(),
                                            fee_schedule_id: token_offer_metadata
                                                .unwrap()
                                                .fee_schedule
                                                .get_reference_address(),
                                            buyer: Some(
                                                token_offer_object.unwrap().get_owner_address(),
                                            ),
                                            price: token_offer_metadata.unwrap().item_price.clone(),
                                            token_amount: BigDecimal::from(1),
                                            expiration_time: token_offer_metadata
                                                .unwrap()
                                                .expiration_time
                                                .clone(),
                                            is_deleted: false,
                                            token_standard: TokenStandard::V2.to_string(),
                                            coin_type: coin_type.clone(),
                                            marketplace: "example_marketplace".to_string(),
                                            contract_address: marketplace_contract_address
                                                .to_string(), // TODO - update this to the actual marketplace contract address
                                            entry_function_id_str: entry_function_id_str
                                                .clone()
                                                .unwrap(),
                                            last_transaction_version: txn_version,
                                            last_transaction_timestamp: txn_timestamp,
                                        });
                                    }
                                }
                            }
                            nft_token_offers.push(current_token_offer.unwrap());
                        } else if move_resource_type.eq(format!(
                            "{}::collection_offer::CollectionOffer",
                            marketplace_contract_address
                        )
                        .as_str())
                        {
                            let collection_object = object_metadatas.get(&move_resource_address);
                            let collection_offer_metadata =
                                collection_offer_metadatas.get(&move_resource_address);
                            let collection_offer_v1 =
                                collection_offer_v1s.get(&move_resource_address);

                            let mut current_collection_offer = None;
                            if let Some(collection_offer_v1) = collection_offer_v1 {
                                let collection_data_id =
                                    collection_offer_v1.get_collection_data_id();
                                current_collection_offer = Some(MarketplaceCollectionOffer {
                                    collection_offer_id: move_resource_address.clone(),
                                    collection_id: collection_data_id.to_id(),
                                    fee_schedule_id: collection_offer_metadata
                                        .unwrap()
                                        .fee_schedule
                                        .get_reference_address(),
                                    buyer: collection_object.unwrap().get_owner_address(),
                                    item_price: collection_offer_metadata
                                        .unwrap()
                                        .item_price
                                        .clone(),
                                    remaining_token_amount: collection_offer_metadata
                                        .unwrap()
                                        .remaining
                                        .clone(),
                                    expiration_time: collection_offer_metadata
                                        .unwrap()
                                        .expiration_time
                                        .clone(),
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
                                let collection_offer_v2 =
                                    collection_offer_v2s.get(&move_resource_address);
                                if let Some(collection_offer_v2) = collection_offer_v2 {
                                    current_collection_offer = Some(MarketplaceCollectionOffer {
                                        collection_offer_id: move_resource_address.clone(),
                                        collection_id: collection_offer_v2
                                            .collection
                                            .get_reference_address(),
                                        fee_schedule_id: collection_offer_metadata
                                            .unwrap()
                                            .fee_schedule
                                            .get_reference_address(),
                                        buyer: collection_object.unwrap().get_owner_address(),
                                        item_price: collection_offer_metadata
                                            .unwrap()
                                            .item_price
                                            .clone(),
                                        remaining_token_amount: collection_offer_metadata
                                            .unwrap()
                                            .remaining
                                            .clone(),
                                        expiration_time: collection_offer_metadata
                                            .unwrap()
                                            .expiration_time
                                            .clone(),
                                        is_deleted: false,
                                        token_standard: TokenStandard::V2.to_string(),
                                        coin_type: coin_type.clone(),
                                        marketplace: "example_marketplace".to_string(),
                                        contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                        entry_function_id_str: entry_function_id_str
                                            .clone()
                                            .unwrap(),
                                        last_transaction_version: txn_version,
                                        last_transaction_timestamp: txn_timestamp,
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
                        let maybe_collection_offer_filled_metadata =
                            collection_offer_filled_metadatas.get(&move_resource_address);
                        if let Some(maybe_collection_offer_filled_metadata) =
                            maybe_collection_offer_filled_metadata
                        {
                            let collection_metadata = maybe_collection_offer_filled_metadata
                                .collection_metadata
                                .clone();
                            let current_collection_offer = MarketplaceCollectionOffer {
                                collection_offer_id: move_resource_address.clone(),
                                collection_id: collection_metadata
                                    .get_collection_address()
                                    .unwrap(),
                                fee_schedule_id: maybe_collection_offer_filled_metadata
                                    .fee_schedule_id
                                    .clone(),
                                buyer: maybe_collection_offer_filled_metadata.buyer.clone(),
                                item_price: maybe_collection_offer_filled_metadata
                                    .item_price
                                    .clone(),
                                remaining_token_amount: BigDecimal::from(0),
                                expiration_time: BigDecimal::from(0),
                                is_deleted: true,
                                token_standard: collection_metadata.get_token_standard(),
                                coin_type: coin_type.clone(),
                                marketplace: "example_marketplace".to_string(),
                                contract_address: marketplace_contract_address.to_string(), // TODO - update this to the actual marketplace contract address
                                entry_function_id_str: entry_function_id_str.clone().unwrap(),
                                last_transaction_version: txn_version,
                                last_transaction_timestamp: txn_timestamp,
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
        marketplace_activities,
        nft_listings,
        nft_token_offers,
        nft_collection_offers,
        nft_auctions,
    )
}
