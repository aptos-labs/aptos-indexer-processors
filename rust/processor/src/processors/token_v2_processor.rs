// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
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
                AptosCollection, BurnEvent, FixedSupply, PropertyMapModel, TokenV2, TokenV2Burned,
                TransferEvent, UnlimitedSupply,
            },
        },
    },
    schema,
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::{get_entry_function_from_user_request, parse_timestamp, standardize_address},
    },
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods};
use field_count::FieldCount;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};
use tracing::error;

pub struct TokenV2Processor {
    connection_pool: PgDbPool,
}

impl TokenV2Processor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
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

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    collections_v2: &[CollectionV2],
    token_datas_v2: &[TokenDataV2],
    token_ownerships_v2: &[TokenOwnershipV2],
    current_collections_v2: &[CurrentCollectionV2],
    current_token_datas_v2: &[CurrentTokenDataV2],
    current_token_ownerships_v2: &[CurrentTokenOwnershipV2],
    token_activities_v2: &[TokenActivityV2],
    current_token_v2_metadata: &[CurrentTokenV2Metadata],
) -> Result<(), diesel::result::Error> {
    insert_collections_v2(conn, collections_v2).await?;
    insert_token_datas_v2(conn, token_datas_v2).await?;
    insert_token_ownerships_v2(conn, token_ownerships_v2).await?;
    insert_current_collections_v2(conn, current_collections_v2).await?;
    insert_current_token_datas_v2(conn, current_token_datas_v2).await?;
    insert_current_token_ownerships_v2(conn, current_token_ownerships_v2).await?;
    insert_token_activities_v2(conn, token_activities_v2).await?;
    insert_current_token_v2_metadatas(conn, current_token_v2_metadata).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    collections_v2: Vec<CollectionV2>,
    token_datas_v2: Vec<TokenDataV2>,
    token_ownerships_v2: Vec<TokenOwnershipV2>,
    current_collections_v2: Vec<CurrentCollectionV2>,
    current_token_datas_v2: Vec<CurrentTokenDataV2>,
    current_token_ownerships_v2: Vec<CurrentTokenOwnershipV2>,
    token_activities_v2: Vec<TokenActivityV2>,
    current_token_v2_metadata: Vec<CurrentTokenV2Metadata>,
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
                &collections_v2,
                &token_datas_v2,
                &token_ownerships_v2,
                &current_collections_v2,
                &current_token_datas_v2,
                &current_token_ownerships_v2,
                &token_activities_v2,
                &current_token_v2_metadata,
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
                        let collections_v2 = clean_data_for_db(collections_v2, true);
                        let token_datas_v2 = clean_data_for_db(token_datas_v2, true);
                        let token_ownerships_v2 = clean_data_for_db(token_ownerships_v2, true);
                        let current_collections_v2 =
                            clean_data_for_db(current_collections_v2, true);
                        let current_token_datas_v2 =
                            clean_data_for_db(current_token_datas_v2, true);
                        let current_token_ownerships_v2 =
                            clean_data_for_db(current_token_ownerships_v2, true);
                        let token_activities_v2 = clean_data_for_db(token_activities_v2, true);
                        let current_token_v2_metadata =
                            clean_data_for_db(current_token_v2_metadata, true);

                        insert_to_db_impl(
                            pg_conn,
                            &collections_v2,
                            &token_datas_v2,
                            &token_ownerships_v2,
                            &current_collections_v2,
                            &current_token_datas_v2,
                            &current_token_ownerships_v2,
                            &token_activities_v2,
                            &current_token_v2_metadata,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_collections_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[CollectionV2],
) -> Result<(), diesel::result::Error> {
    use schema::collections_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CollectionV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::collections_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_token_datas_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[TokenDataV2],
) -> Result<(), diesel::result::Error> {
    use schema::token_datas_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), TokenDataV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_datas_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_token_ownerships_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[TokenOwnershipV2],
) -> Result<(), diesel::result::Error> {
    use schema::token_ownerships_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), TokenOwnershipV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_ownerships_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_collections_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentCollectionV2],
) -> Result<(), diesel::result::Error> {
    use schema::current_collections_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentCollectionV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_collections_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
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
        ).await?;
    }
    Ok(())
}

async fn insert_current_token_datas_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenDataV2],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_datas_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenDataV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_datas_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
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
        ).await?;
    }
    Ok(())
}

async fn insert_current_token_ownerships_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenOwnershipV2],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_ownerships_v2::dsl::*;

    let chunks = get_chunks(
        items_to_insert.len(),
        CurrentTokenOwnershipV2::field_count(),
    );

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_ownerships_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
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
        ).await?;
    }
    Ok(())
}

async fn insert_token_activities_v2(
    conn: &mut MyDbConnection,
    items_to_insert: &[TokenActivityV2],
) -> Result<(), diesel::result::Error> {
    use schema::token_activities_v2::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), TokenActivityV2::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_activities_v2::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, event_index))
                .do_update()
                .set((
                    entry_function_id_str.eq(excluded(entry_function_id_str)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_token_v2_metadatas(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenV2Metadata],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_v2_metadata::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenV2Metadata::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_v2_metadata::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((object_address, resource_type))
                .do_update()
                .set((
                    data.eq(excluded(data)),
                    state_key_hash.eq(excluded(state_key_hash)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE current_token_v2_metadata.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
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
        let mut conn = self.get_conn().await;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        // Token V2 processing which includes token v1
        let (
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_ownerships_v2,
            current_token_datas_v2,
            token_activities_v2,
            current_token_v2_metadata,
        ) = parse_v2_token(&transactions, &table_handle_to_owner, &mut conn).await;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_ownerships_v2,
            current_token_datas_v2,
            token_activities_v2,
            current_token_v2_metadata,
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

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: &TableHandleToOwner,
    conn: &mut PgPoolConnection<'_>,
) -> (
    Vec<CollectionV2>,
    Vec<TokenDataV2>,
    Vec<TokenOwnershipV2>,
    Vec<CurrentCollectionV2>,
    Vec<CurrentTokenDataV2>,
    Vec<CurrentTokenOwnershipV2>,
    Vec<TokenActivityV2>,
    Vec<CurrentTokenV2Metadata>,
) {
    // Token V2 and V1 combined
    let mut collections_v2 = vec![];
    let mut token_datas_v2 = vec![];
    let mut token_ownerships_v2 = vec![];
    let mut token_activities_v2 = vec![];
    let mut current_collections_v2: HashMap<CurrentCollectionV2PK, CurrentCollectionV2> =
        HashMap::new();
    let mut current_token_datas_v2: HashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        HashMap::new();
    let mut current_token_ownerships_v2: HashMap<
        CurrentTokenOwnershipV2PK,
        CurrentTokenOwnershipV2,
    > = HashMap::new();
    // Tracks prior ownership in case a token gets burned
    let mut prior_nft_ownership: HashMap<String, NFTOwnershipV2> = HashMap::new();
    // Get Metadata for token v2 by object
    // We want to persist this through the entire batch so that even if a token is burned,
    // we can still get the object core metadata for it
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = HashMap::new();
    // Basically token properties
    let mut current_token_v2_metadata: HashMap<CurrentTokenV2MetadataPK, CurrentTokenV2Metadata> =
        HashMap::new();

    // Code above is inefficient (multiple passthroughs) so I'm approaching TokenV2 with a cleaner code structure
    for txn in transactions {
        let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
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
            let mut tokens_burned: TokenV2Burned = HashSet::new();

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
                                property_map: None,
                                transfer_event: None,
                                token: None,
                                fungible_asset_metadata: None,
                                fungible_asset_supply: None,
                                fungible_asset_store: None,
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
                    }
                }
            }

            // Pass through events to get the burn events and token activities v2
            // This needs to be here because we need the metadata above for token activities
            // and burn / transfer events need to come before the next section
            for (index, event) in user_txn.events.iter().enumerate() {
                if let Some(burn_event) = BurnEvent::from_event(event, txn_version).unwrap() {
                    tokens_burned.insert(burn_event.get_token_address());
                }
                if let Some(transfer_event) = TransferEvent::from_event(event, txn_version).unwrap()
                {
                    if let Some(aggregated_data) =
                        token_v2_metadata_helper.get_mut(&transfer_event.get_object_address())
                    {
                        // we don't want index to be 0 otherwise we might have collision with write set change index
                        let index = if index == 0 {
                            user_txn.events.len()
                        } else {
                            index
                        };
                        aggregated_data.transfer_event = Some((index as i64, transfer_event));
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
                            if let Some(inner) = TokenOwnershipV2::get_nft_v2_from_token_data(
                                &token_data,
                                &token_v2_metadata_helper,
                            )
                            .unwrap()
                            {
                                let (
                                    nft_ownership,
                                    current_nft_ownership,
                                    from_nft_ownership,
                                    from_current_nft_ownership,
                                ) = inner;
                                token_ownerships_v2.push(nft_ownership);
                                // this is used to persist latest owner for burn event handling
                                prior_nft_ownership.insert(
                                    current_nft_ownership.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: current_nft_ownership.token_data_id.clone(),
                                        owner_address: current_nft_ownership.owner_address.clone(),
                                        is_soulbound: current_nft_ownership.is_soulbound_v2,
                                    },
                                );
                                current_token_ownerships_v2.insert(
                                    (
                                        current_nft_ownership.token_data_id.clone(),
                                        current_nft_ownership.property_version_v1.clone(),
                                        current_nft_ownership.owner_address.clone(),
                                        current_nft_ownership.storage_id.clone(),
                                    ),
                                    current_nft_ownership,
                                );
                                // Add the previous owner of the token transfer
                                if let Some(from_nft_ownership) = from_nft_ownership {
                                    let from_current_nft_ownership =
                                        from_current_nft_ownership.unwrap();
                                    token_ownerships_v2.push(from_nft_ownership);
                                    current_token_ownerships_v2.insert(
                                        (
                                            from_current_nft_ownership.token_data_id.clone(),
                                            from_current_nft_ownership.property_version_v1.clone(),
                                            from_current_nft_ownership.owner_address.clone(),
                                            from_current_nft_ownership.storage_id.clone(),
                                        ),
                                        from_current_nft_ownership,
                                    );
                                }
                            }
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
                                &tokens_burned,
                            )
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
                            current_token_ownerships_v2.insert(
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
                            current_token_ownerships_v2.insert(
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

    (
        collections_v2,
        token_datas_v2,
        token_ownerships_v2,
        current_collections_v2,
        current_token_datas_v2,
        current_token_ownerships_v2,
        token_activities_v2,
        current_token_v2_metadata,
    )
}
