// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        fungible_asset_models::v2_fungible_asset_utils::{
            FungibleAssetMetadata, FungibleAssetStore, FungibleAssetSupply,
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
        token_models::tokens::{TableHandleToOwner, TableMetadataForToken},
        token_v2_models::{
            v2_collections::{CollectionV2, CurrentCollectionV2, CurrentCollectionV2PK},
            v2_token_activities::TokenActivityV2,
            v2_token_datas::{CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2},
            v2_token_metadata::{CurrentTokenV2Metadata, CurrentTokenV2MetadataPK},
            v2_token_ownerships::{
                CurrentTokenOwnershipV2, CurrentTokenOwnershipV2PK, NFTOwnershipV2,
                TokenOwnershipV2,
            },
            v2_token_utils::{
                AptosCollection, Burn, BurnEvent, ConcurrentSupply, FixedSupply, MintEvent,
                PropertyMapModel, TokenIdentifiers, TokenV2, TokenV2Burned, TokenV2Minted,
                TransferEvent, UnlimitedSupply,
            },
        },
    },
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool, DbPoolConnection},
        util::{get_entry_function_from_user_request, parse_timestamp, standardize_address},
    },
    IndexerGrpcProcessorConfig,
};
use ahash::{AHashMap, AHashSet};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenV2ProcessorConfig {
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}

pub struct TokenV2Processor {
    connection_pool: ArcDbPool,
    config: TokenV2ProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl TokenV2Processor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: TokenV2ProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
    ) -> Self {
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for TokenV2Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TokenV2TransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    collections_v2: &[CollectionV2],
    token_datas_v2: &[TokenDataV2],
    token_ownerships_v2: &[TokenOwnershipV2],
    current_collections_v2: &[CurrentCollectionV2],
    current_token_datas_v2: &[CurrentTokenDataV2],
    current_token_ownerships_v2: &[CurrentTokenOwnershipV2],
    current_deleted_token_ownerships_v2: &[CurrentTokenOwnershipV2],
    token_activities_v2: &[TokenActivityV2],
    current_token_v2_metadata: &[CurrentTokenV2Metadata],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let coll_v2 = execute_in_chunks(
        conn.clone(),
        insert_collections_v2_query,
        collections_v2,
        get_config_table_chunk_size::<CollectionV2>("collections_v2", per_table_chunk_sizes),
    );
    let td_v2 = execute_in_chunks(
        conn.clone(),
        insert_token_datas_v2_query,
        token_datas_v2,
        get_config_table_chunk_size::<TokenDataV2>("token_datas_v2", per_table_chunk_sizes),
    );
    let to_v2 = execute_in_chunks(
        conn.clone(),
        insert_token_ownerships_v2_query,
        token_ownerships_v2,
        get_config_table_chunk_size::<TokenOwnershipV2>(
            "token_ownerships_v2",
            per_table_chunk_sizes,
        ),
    );
    let cc_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_collections_v2_query,
        current_collections_v2,
        get_config_table_chunk_size::<CurrentCollectionV2>(
            "current_collections_v2",
            per_table_chunk_sizes,
        ),
    );
    let ctd_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_token_datas_v2_query,
        current_token_datas_v2,
        get_config_table_chunk_size::<CurrentTokenDataV2>(
            "current_token_datas_v2",
            per_table_chunk_sizes,
        ),
    );
    let cto_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_token_ownerships_v2_query,
        current_token_ownerships_v2,
        get_config_table_chunk_size::<CurrentTokenOwnershipV2>(
            "current_token_ownerships_v2",
            per_table_chunk_sizes,
        ),
    );
    let cdto_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_deleted_token_ownerships_v2_query,
        current_deleted_token_ownerships_v2,
        get_config_table_chunk_size::<CurrentTokenOwnershipV2>(
            "current_token_ownerships_v2",
            per_table_chunk_sizes,
        ),
    );
    let ta_v2 = execute_in_chunks(
        conn.clone(),
        insert_token_activities_v2_query,
        token_activities_v2,
        get_config_table_chunk_size::<TokenActivityV2>(
            "token_activities_v2",
            per_table_chunk_sizes,
        ),
    );
    let ct_v2 = execute_in_chunks(
        conn,
        insert_current_token_v2_metadatas_query,
        current_token_v2_metadata,
        get_config_table_chunk_size::<CurrentTokenV2Metadata>(
            "current_token_v2_metadata",
            per_table_chunk_sizes,
        ),
    );

    let (
        coll_v2_res,
        td_v2_res,
        to_v2_res,
        cc_v2_res,
        ctd_v2_res,
        cto_v2_res,
        cdto_v2_res,
        ta_v2_res,
        ct_v2_res,
    ) = tokio::join!(coll_v2, td_v2, to_v2, cc_v2, ctd_v2, cto_v2, cdto_v2, ta_v2, ct_v2,);

    for res in [
        coll_v2_res,
        td_v2_res,
        to_v2_res,
        cc_v2_res,
        ctd_v2_res,
        cto_v2_res,
        cdto_v2_res,
        ta_v2_res,
        ct_v2_res,
    ] {
        res?;
    }

    Ok(())
}

fn insert_collections_v2_query(
    items_to_insert: Vec<CollectionV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::collections_v2::dsl::*;
    (
        diesel::insert_into(schema::collections_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_token_datas_v2_query(
    items_to_insert: Vec<TokenDataV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::token_datas_v2::dsl::*;

    (
        diesel::insert_into(schema::token_datas_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_token_ownerships_v2_query(
    items_to_insert: Vec<TokenOwnershipV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::token_ownerships_v2::dsl::*;

    (
        diesel::insert_into(schema::token_ownerships_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_collections_v2_query(
    items_to_insert: Vec<CurrentCollectionV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_collections_v2::dsl::*;

    (
        diesel::insert_into(schema::current_collections_v2::table)
            .values(items_to_insert)
            .on_conflict(collection_id)
            .do_update()
            .set((
                creator_address.eq(excluded(creator_address)),
                collection_name.eq(excluded(collection_name)),
                description.eq(excluded(description)),
                uri.eq(excluded(uri)),
                current_supply.eq(excluded(current_supply)),
                max_supply.eq(excluded(max_supply)),
                total_minted_v2.eq(excluded(total_minted_v2)),
                mutable_description.eq(excluded(mutable_description)),
                mutable_uri.eq(excluded(mutable_uri)),
                table_handle_v1.eq(excluded(table_handle_v1)),
                token_standard.eq(excluded(token_standard)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_collections_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_current_token_datas_v2_query(
    items_to_insert: Vec<CurrentTokenDataV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_datas_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_datas_v2::table)
            .values(items_to_insert)
            .on_conflict(token_data_id)
            .do_update()
            .set((
                collection_id.eq(excluded(collection_id)),
                token_name.eq(excluded(token_name)),
                maximum.eq(excluded(maximum)),
                supply.eq(excluded(supply)),
                largest_property_version_v1.eq(excluded(largest_property_version_v1)),
                token_uri.eq(excluded(token_uri)),
                description.eq(excluded(description)),
                token_properties.eq(excluded(token_properties)),
                token_standard.eq(excluded(token_standard)),
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
                decimals.eq(excluded(decimals)),
            )),
        Some(" WHERE current_token_datas_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_current_token_ownerships_v2_query(
    items_to_insert: Vec<CurrentTokenOwnershipV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_ownerships_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_ownerships_v2::table)
            .values(items_to_insert)
            .on_conflict((token_data_id, property_version_v1, owner_address, storage_id))
            .do_update()
            .set((
                amount.eq(excluded(amount)),
                table_type_v1.eq(excluded(table_type_v1)),
                token_properties_mutated_v1.eq(excluded(token_properties_mutated_v1)),
                is_soulbound_v2.eq(excluded(is_soulbound_v2)),
                token_standard.eq(excluded(token_standard)),
                is_fungible_v2.eq(excluded(is_fungible_v2)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
                non_transferrable_by_owner.eq(excluded(non_transferrable_by_owner)),
            )),
        Some(" WHERE current_token_ownerships_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_current_deleted_token_ownerships_v2_query(
    items_to_insert: Vec<CurrentTokenOwnershipV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_ownerships_v2::dsl::*;

    (
        diesel::insert_into(schema::current_token_ownerships_v2::table)
            .values(items_to_insert)
            .on_conflict((token_data_id, property_version_v1, owner_address, storage_id))
            .do_update()
            .set((
                amount.eq(excluded(amount)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_token_ownerships_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_token_activities_v2_query(
    items_to_insert: Vec<TokenActivityV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::token_activities_v2::dsl::*;

    (
        diesel::insert_into(schema::token_activities_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                entry_function_id_str.eq(excluded(entry_function_id_str)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

fn insert_current_token_v2_metadatas_query(
    items_to_insert: Vec<CurrentTokenV2Metadata>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_v2_metadata::dsl::*;

    (
        diesel::insert_into(schema::current_token_v2_metadata::table)
            .values(items_to_insert)
            .on_conflict((object_address, resource_type))
            .do_update()
            .set((
                data.eq(excluded(data)),
                state_key_hash.eq(excluded(state_key_hash)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_token_v2_metadata.last_transaction_version <= excluded.last_transaction_version "),
    )
}

#[async_trait]
impl ProcessorTrait for TokenV2Processor {
    fn name(&self) -> &'static str {
        ProcessorName::TokenV2Processor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut conn = self.get_conn().await;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        let query_retries = self.config.query_retries;
        let query_retry_delay_ms = self.config.query_retry_delay_ms;
        // Token V2 processing which includes token v1
        let (
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_datas_v2,
            current_token_ownerships_v2,
            current_deleted_token_ownerships_v2,
            token_activities_v2,
            current_token_v2_metadata,
        ) = parse_v2_token(
            &transactions,
            &table_handle_to_owner,
            &mut conn,
            query_retries,
            query_retry_delay_ms,
        )
        .await;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &collections_v2,
            &token_datas_v2,
            &token_ownerships_v2,
            &current_collections_v2,
            &current_token_datas_v2,
            &current_token_ownerships_v2,
            &current_deleted_token_ownerships_v2,
            &token_activities_v2,
            &current_token_v2_metadata,
            &self.per_table_chunk_sizes,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp,
            }),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: &TableHandleToOwner,
    conn: &mut DbPoolConnection<'_>,
    query_retries: u32,
    query_retry_delay_ms: u64,
) -> (
    Vec<CollectionV2>,
    Vec<TokenDataV2>,
    Vec<TokenOwnershipV2>,
    Vec<CurrentCollectionV2>,
    Vec<CurrentTokenDataV2>,
    Vec<CurrentTokenOwnershipV2>,
    Vec<CurrentTokenOwnershipV2>, // deleted token ownerships
    Vec<TokenActivityV2>,
    Vec<CurrentTokenV2Metadata>,
) {
    // Token V2 and V1 combined
    let mut collections_v2 = vec![];
    let mut token_datas_v2 = vec![];
    let mut token_ownerships_v2 = vec![];
    let mut token_activities_v2 = vec![];
    let mut current_collections_v2: AHashMap<CurrentCollectionV2PK, CurrentCollectionV2> =
        AHashMap::new();
    let mut current_token_datas_v2: AHashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        AHashMap::new();
    let mut current_token_ownerships_v2: AHashMap<
        CurrentTokenOwnershipV2PK,
        CurrentTokenOwnershipV2,
    > = AHashMap::new();
    let mut current_deleted_token_ownerships_v2 = AHashMap::new();
    // Tracks prior ownership in case a token gets burned
    let mut prior_nft_ownership: AHashMap<String, NFTOwnershipV2> = AHashMap::new();
    // Get Metadata for token v2 by object
    // We want to persist this through the entire batch so that even if a token is burned,
    // we can still get the object core metadata for it
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();
    // Basically token properties
    let mut current_token_v2_metadata: AHashMap<CurrentTokenV2MetadataPK, CurrentTokenV2Metadata> =
        AHashMap::new();

    // Code above is inefficient (multiple passthroughs) so I'm approaching TokenV2 with a cleaner code structure
    for txn in transactions {
        let txn_version = txn.version;
        let txn_data = match txn.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["TokenV2Processor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        if let TxnData::User(user_txn) = txn_data {
            let user_request = user_txn
                .request
                .as_ref()
                .expect("Sends is not present in user txn");
            let entry_function_id_str = get_entry_function_from_user_request(user_request);

            // Get burn events for token v2 by object
            let mut tokens_burned: TokenV2Burned = AHashMap::new();

            // Get mint events for token v2 by object
            let mut tokens_minted: TokenV2Minted = AHashSet::new();

            // Need to do a first pass to get all the objects
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) =
                        ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                    {
                        token_v2_metadata_helper.insert(
                            standardize_address(&wr.address.to_string()),
                            ObjectAggregatedData {
                                aptos_collection: None,
                                fixed_supply: None,
                                object,
                                unlimited_supply: None,
                                concurrent_supply: None,
                                property_map: None,
                                transfer_events: vec![],
                                token: None,
                                fungible_asset_metadata: None,
                                fungible_asset_supply: None,
                                fungible_asset_store: None,
                                token_identifier: None,
                            },
                        );
                    }
                }
            }

            // Need to do a second pass to get all the structs related to the object
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(aggregated_data) = token_v2_metadata_helper.get_mut(&address) {
                        if let Some(fixed_supply) =
                            FixedSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fixed_supply = Some(fixed_supply);
                        }
                        if let Some(unlimited_supply) =
                            UnlimitedSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.unlimited_supply = Some(unlimited_supply);
                        }
                        if let Some(aptos_collection) =
                            AptosCollection::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.aptos_collection = Some(aptos_collection);
                        }
                        if let Some(property_map) =
                            PropertyMapModel::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.property_map = Some(property_map);
                        }
                        if let Some(concurrent_supply) =
                            ConcurrentSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.concurrent_supply = Some(concurrent_supply);
                        }
                        if let Some(token) = TokenV2::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.token = Some(token);
                        }
                        if let Some(fungible_asset_metadata) =
                            FungibleAssetMetadata::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                        }
                        if let Some(fungible_asset_supply) =
                            FungibleAssetSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fungible_asset_supply = Some(fungible_asset_supply);
                        }
                        if let Some(fungible_asset_store) =
                            FungibleAssetStore::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fungible_asset_store = Some(fungible_asset_store);
                        }
                        if let Some(token_identifier) =
                            TokenIdentifiers::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.token_identifier = Some(token_identifier);
                        }
                    }
                }
            }

            // Pass through events to get the burn events and token activities v2
            // This needs to be here because we need the metadata above for token activities
            // and burn / transfer events need to come before the next section
            for (index, event) in user_txn.events.iter().enumerate() {
                if let Some(burn_event) = Burn::from_event(event, txn_version).unwrap() {
                    tokens_burned.insert(burn_event.get_token_address(), Some(burn_event));
                }
                if let Some(burn_event) = BurnEvent::from_event(event, txn_version).unwrap() {
                    tokens_burned.insert(burn_event.get_token_address(), None);
                }
                if let Some(mint_event) = MintEvent::from_event(event, txn_version).unwrap() {
                    tokens_minted.insert(mint_event.get_token_address());
                }
                if let Some(transfer_events) =
                    TransferEvent::from_event(event, txn_version).unwrap()
                {
                    if let Some(aggregated_data) =
                        token_v2_metadata_helper.get_mut(&transfer_events.get_object_address())
                    {
                        // we don't want index to be 0 otherwise we might have collision with write set change index
                        // note that these will be multiplied by -1 so that it doesn't conflict with wsc index
                        let index = if index == 0 {
                            user_txn.events.len()
                        } else {
                            index
                        };
                        aggregated_data
                            .transfer_events
                            .push((index as i64, transfer_events));
                    }
                }
                // handling all the token v1 events
                if let Some(event) = TokenActivityV2::get_v1_from_parsed_event(
                    event,
                    txn_version,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                )
                .unwrap()
                {
                    token_activities_v2.push(event);
                }
                // handling all the token v2 events
                if let Some(event) = TokenActivityV2::get_nft_v2_from_parsed_event(
                    event,
                    txn_version,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &token_v2_metadata_helper,
                )
                .await
                .unwrap()
                {
                    token_activities_v2.push(event);
                }
                // handling all the token v2 events
                if let Some(event) = TokenActivityV2::get_ft_v2_from_parsed_event(
                    event,
                    txn_version,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &token_v2_metadata_helper,
                    conn,
                )
                .await
                .unwrap()
                {
                    token_activities_v2.push(event);
                }
            }

            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                let wsc_index = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteTableItem(table_item) => {
                        if let Some((collection, current_collection)) =
                            CollectionV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                                conn,
                                query_retries,
                                query_retry_delay_ms,
                            )
                            .await
                            .unwrap()
                        {
                            collections_v2.push(collection);
                            current_collections_v2.insert(
                                current_collection.collection_id.clone(),
                                current_collection,
                            );
                        }
                        if let Some((token_data, current_token_data)) =
                            TokenDataV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                            )
                            .unwrap()
                        {
                            token_datas_v2.push(token_data);
                            current_token_datas_v2.insert(
                                current_token_data.token_data_id.clone(),
                                current_token_data,
                            );
                        }
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                                current_token_ownerships_v2.insert(
                                    (
                                        cto.token_data_id.clone(),
                                        cto.property_version_v1.clone(),
                                        cto.owner_address.clone(),
                                        cto.storage_id.clone(),
                                    ),
                                    cto,
                                );
                            }
                        }
                    },
                    Change::DeleteTableItem(table_item) => {
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_delete_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                                current_deleted_token_ownerships_v2.insert(
                                    (
                                        cto.token_data_id.clone(),
                                        cto.property_version_v1.clone(),
                                        cto.owner_address.clone(),
                                        cto.storage_id.clone(),
                                    ),
                                    cto,
                                );
                            }
                        }
                    },
                    Change::WriteResource(resource) => {
                        if let Some((collection, current_collection)) =
                            CollectionV2::get_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &token_v2_metadata_helper,
                            )
                            .unwrap()
                        {
                            collections_v2.push(collection);
                            current_collections_v2.insert(
                                current_collection.collection_id.clone(),
                                current_collection,
                            );
                        }
                        if let Some((token_data, current_token_data)) =
                            TokenDataV2::get_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &token_v2_metadata_helper,
                            )
                            .unwrap()
                        {
                            // Add NFT ownership
                            let (mut ownerships, current_ownerships) =
                                TokenOwnershipV2::get_nft_v2_from_token_data(
                                    &token_data,
                                    &token_v2_metadata_helper,
                                )
                                .unwrap();
                            if let Some(current_nft_ownership) = ownerships.first() {
                                // Note that the first element in ownerships is the current ownership. We need to cache
                                // it in prior_nft_ownership so that moving forward if we see a burn we'll know
                                // where it came from.
                                prior_nft_ownership.insert(
                                    current_nft_ownership.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: current_nft_ownership.token_data_id.clone(),
                                        owner_address: current_nft_ownership
                                            .owner_address
                                            .as_ref()
                                            .unwrap()
                                            .clone(),
                                        is_soulbound: current_nft_ownership.is_soulbound_v2,
                                    },
                                );
                            }
                            token_ownerships_v2.append(&mut ownerships);
                            current_token_ownerships_v2.extend(current_ownerships);
                            token_datas_v2.push(token_data);
                            current_token_datas_v2.insert(
                                current_token_data.token_data_id.clone(),
                                current_token_data,
                            );
                        }

                        // Add burned NFT handling
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                                conn,
                                query_retries,
                                query_retry_delay_ms,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                            current_deleted_token_ownerships_v2.insert(
                                (
                                    current_nft_ownership.token_data_id.clone(),
                                    current_nft_ownership.property_version_v1.clone(),
                                    current_nft_ownership.owner_address.clone(),
                                    current_nft_ownership.storage_id.clone(),
                                ),
                                current_nft_ownership,
                            );
                        }

                        // Add fungible token handling
                        if let Some((ft_ownership, current_ft_ownership)) =
                            TokenOwnershipV2::get_ft_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &token_v2_metadata_helper,
                                conn,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(ft_ownership);
                            current_token_ownerships_v2.insert(
                                (
                                    current_ft_ownership.token_data_id.clone(),
                                    current_ft_ownership.property_version_v1.clone(),
                                    current_ft_ownership.owner_address.clone(),
                                    current_ft_ownership.storage_id.clone(),
                                ),
                                current_ft_ownership,
                            );
                        }

                        // Track token properties
                        if let Some(token_metadata) = CurrentTokenV2Metadata::from_write_resource(
                            resource,
                            txn_version,
                            &token_v2_metadata_helper,
                        )
                        .unwrap()
                        {
                            current_token_v2_metadata.insert(
                                (
                                    token_metadata.object_address.clone(),
                                    token_metadata.resource_type.clone(),
                                ),
                                token_metadata,
                            );
                        }
                    },
                    Change::DeleteResource(resource) => {
                        // Add burned NFT handling
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_delete_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                                conn,
                                query_retries,
                                query_retry_delay_ms,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                            current_deleted_token_ownerships_v2.insert(
                                (
                                    current_nft_ownership.token_data_id.clone(),
                                    current_nft_ownership.property_version_v1.clone(),
                                    current_nft_ownership.owner_address.clone(),
                                    current_nft_ownership.storage_id.clone(),
                                ),
                                current_nft_ownership,
                            );
                        }
                    },
                    _ => {},
                }
            }
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_collections_v2 = current_collections_v2
        .into_values()
        .collect::<Vec<CurrentCollectionV2>>();
    let mut current_token_datas_v2 = current_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let mut current_token_ownerships_v2 = current_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();
    let mut current_token_v2_metadata = current_token_v2_metadata
        .into_values()
        .collect::<Vec<CurrentTokenV2Metadata>>();
    let mut current_deleted_token_ownerships_v2 = current_deleted_token_ownerships_v2
        .into_values()
        .collect::<Vec<CurrentTokenOwnershipV2>>();

    // Sort by PK
    current_collections_v2.sort_by(|a, b| a.collection_id.cmp(&b.collection_id));
    current_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));
    current_token_ownerships_v2.sort_by(|a, b| {
        (
            &a.token_data_id,
            &a.property_version_v1,
            &a.owner_address,
            &a.storage_id,
        )
            .cmp(&(
                &b.token_data_id,
                &b.property_version_v1,
                &b.owner_address,
                &b.storage_id,
            ))
    });
    current_token_v2_metadata.sort_by(|a, b| {
        (&a.object_address, &a.resource_type).cmp(&(&b.object_address, &b.resource_type))
    });
    current_deleted_token_ownerships_v2.sort_by(|a, b| {
        (
            &a.token_data_id,
            &a.property_version_v1,
            &a.owner_address,
            &a.storage_id,
        )
            .cmp(&(
                &b.token_data_id,
                &b.property_version_v1,
                &b.owner_address,
                &b.storage_id,
            ))
    });

    (
        collections_v2,
        token_datas_v2,
        token_ownerships_v2,
        current_collections_v2,
        current_token_datas_v2,
        current_token_ownerships_v2,
        current_deleted_token_ownerships_v2,
        token_activities_v2,
        current_token_v2_metadata,
    )
}
