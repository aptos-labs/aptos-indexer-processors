// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessorName, ProcessorStorageTrait};
use crate::{
    models::token_models::{
        collection_datas::{CollectionData, CurrentCollectionData},
        nft_points::NftPoints,
        token_activities::TokenActivity,
        token_claims::CurrentTokenPendingClaim,
        token_datas::{CurrentTokenData, TokenData},
        token_ownerships::{CurrentTokenOwnership, TokenOwnership},
        tokens::{
            CurrentTokenOwnershipPK, CurrentTokenPendingClaimPK, TableMetadataForToken, Token,
            TokenDataIdHash,
        },
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
        PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_processor_sdk::processor::{ProcessingResult, ProcessorTrait};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenProcessorConfig {
    pub nft_points_contract: Option<String>,
}

pub struct TokenProcessor {
    connection_pool: PgDbPool,
    config: TokenProcessorConfig,
}

impl TokenProcessor {
    pub fn new(connection_pool: PgDbPool, config: TokenProcessorConfig) -> Self {
        Self {
            connection_pool,
            config,
        }
    }
}

impl Debug for TokenProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TokenTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    basic_token_transaction_lists: (&[Token], &[TokenOwnership], &[TokenData], &[CollectionData]),
    basic_token_current_lists: (
        &[CurrentTokenOwnership],
        &[CurrentTokenData],
        &[CurrentCollectionData],
    ),
    token_activities: &[TokenActivity],
    current_token_claims: &[CurrentTokenPendingClaim],
    nft_points: &[NftPoints],
) -> Result<(), diesel::result::Error> {
    let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
    let (current_token_ownerships, current_token_datas, current_collection_datas) =
        basic_token_current_lists;
    insert_tokens(conn, tokens).await?;
    insert_token_datas(conn, token_datas).await?;
    insert_token_ownerships(conn, token_ownerships).await?;
    insert_collection_datas(conn, collection_datas).await?;
    insert_current_token_ownerships(conn, current_token_ownerships).await?;
    insert_current_token_datas(conn, current_token_datas).await?;
    insert_current_collection_datas(conn, current_collection_datas).await?;
    insert_token_activities(conn, token_activities).await?;
    insert_current_token_claims(conn, current_token_claims).await?;
    insert_nft_points(conn, nft_points).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    basic_token_transaction_lists: (
        Vec<Token>,
        Vec<TokenOwnership>,
        Vec<TokenData>,
        Vec<CollectionData>,
    ),
    basic_token_current_lists: (
        Vec<CurrentTokenOwnership>,
        Vec<CurrentTokenData>,
        Vec<CurrentCollectionData>,
    ),
    token_activities: Vec<TokenActivity>,
    current_token_claims: Vec<CurrentTokenPendingClaim>,
    nft_points: Vec<NftPoints>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
    let (current_token_ownerships, current_token_datas, current_collection_datas) =
        basic_token_current_lists;
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            Box::pin(insert_to_db_impl(
                pg_conn,
                (&tokens, &token_ownerships, &token_datas, &collection_datas),
                (
                    &current_token_ownerships,
                    &current_token_datas,
                    &current_collection_datas,
                ),
                &token_activities,
                &current_token_claims,
                &nft_points,
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
                        let tokens = clean_data_for_db(tokens, true);
                        let token_datas = clean_data_for_db(token_datas, true);
                        let token_ownerships = clean_data_for_db(token_ownerships, true);
                        let collection_datas = clean_data_for_db(collection_datas, true);
                        let current_token_ownerships =
                            clean_data_for_db(current_token_ownerships, true);
                        let current_token_datas = clean_data_for_db(current_token_datas, true);
                        let current_collection_datas =
                            clean_data_for_db(current_collection_datas, true);
                        let token_activities = clean_data_for_db(token_activities, true);
                        let current_token_claims = clean_data_for_db(current_token_claims, true);
                        let nft_points = clean_data_for_db(nft_points, true);

                        insert_to_db_impl(
                            pg_conn,
                            (&tokens, &token_ownerships, &token_datas, &collection_datas),
                            (
                                &current_token_ownerships,
                                &current_token_datas,
                                &current_collection_datas,
                            ),
                            &token_activities,
                            &current_token_claims,
                            &nft_points,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_tokens(
    conn: &mut MyDbConnection,
    tokens_to_insert: &[Token],
) -> Result<(), diesel::result::Error> {
    use schema::tokens::dsl::*;

    let chunks = get_chunks(tokens_to_insert.len(), Token::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tokens::table)
                .values(&tokens_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, property_version, transaction_version))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_token_ownerships(
    conn: &mut MyDbConnection,
    token_ownerships_to_insert: &[TokenOwnership],
) -> Result<(), diesel::result::Error> {
    use schema::token_ownerships::dsl::*;

    let chunks = get_chunks(
        token_ownerships_to_insert.len(),
        TokenOwnership::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_ownerships::table)
                .values(&token_ownerships_to_insert[start_ind..end_ind])
                .on_conflict((
                    token_data_id_hash,
                    property_version,
                    transaction_version,
                    table_handle,
                ))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_token_datas(
    conn: &mut MyDbConnection,
    token_datas_to_insert: &[TokenData],
) -> Result<(), diesel::result::Error> {
    use schema::token_datas::dsl::*;

    let chunks = get_chunks(token_datas_to_insert.len(), TokenData::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_datas::table)
                .values(&token_datas_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, transaction_version))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_collection_datas(
    conn: &mut MyDbConnection,
    collection_datas_to_insert: &[CollectionData],
) -> Result<(), diesel::result::Error> {
    use schema::collection_datas::dsl::*;

    let chunks = get_chunks(
        collection_datas_to_insert.len(),
        CollectionData::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::collection_datas::table)
                .values(&collection_datas_to_insert[start_ind..end_ind])
                .on_conflict((collection_data_id_hash, transaction_version))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_token_ownerships(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenOwnership],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_ownerships::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenOwnership::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_ownerships::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, property_version, owner_address))
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    amount.eq(excluded(amount)),
                    token_properties.eq(excluded(token_properties)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    table_type.eq(excluded(table_type)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE current_token_ownerships.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

async fn insert_current_token_datas(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenData],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_datas::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenData::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_datas::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(token_data_id_hash)
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    maximum.eq(excluded(maximum)),
                    supply.eq(excluded(supply)),
                    largest_property_version.eq(excluded(largest_property_version)),
                    metadata_uri.eq(excluded(metadata_uri)),
                    payee_address.eq(excluded(payee_address)),
                    royalty_points_numerator.eq(excluded(royalty_points_numerator)),
                    royalty_points_denominator.eq(excluded(royalty_points_denominator)),
                    maximum_mutable.eq(excluded(maximum_mutable)),
                    uri_mutable.eq(excluded(uri_mutable)),
                    description_mutable.eq(excluded(description_mutable)),
                    properties_mutable.eq(excluded(properties_mutable)),
                    royalty_mutable.eq(excluded(royalty_mutable)),
                    default_properties.eq(excluded(default_properties)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    description.eq(excluded(description)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE current_token_datas.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

async fn insert_current_collection_datas(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentCollectionData],
) -> Result<(), diesel::result::Error> {
    use schema::current_collection_datas::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentCollectionData::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_collection_datas::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(collection_data_id_hash)
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    description.eq(excluded(description)),
                    metadata_uri.eq(excluded(metadata_uri)),
                    supply.eq(excluded(supply)),
                    maximum.eq(excluded(maximum)),
                    maximum_mutable.eq(excluded(maximum_mutable)),
                    uri_mutable.eq(excluded(uri_mutable)),
                    description_mutable.eq(excluded(description_mutable)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    table_handle.eq(excluded(table_handle)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            Some(" WHERE current_collection_datas.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

async fn insert_token_activities(
    conn: &mut MyDbConnection,
    items_to_insert: &[TokenActivity],
) -> Result<(), diesel::result::Error> {
    use schema::token_activities::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), TokenActivity::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_activities::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((
                    transaction_version,
                    event_account_address,
                    event_creation_number,
                    event_sequence_number,
                ))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_token_claims(
    conn: &mut MyDbConnection,
    items_to_insert: &[CurrentTokenPendingClaim],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_pending_claims::dsl::*;

    let chunks = get_chunks(
        items_to_insert.len(),
        CurrentTokenPendingClaim::field_count(),
    );

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_pending_claims::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((
                    token_data_id_hash, property_version, from_address, to_address
                ))
                .do_update()
                .set((
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    amount.eq(excluded(amount)),
                    table_handle.eq(excluded(table_handle)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    inserted_at.eq(excluded(inserted_at)),
                    token_data_id.eq(excluded(token_data_id)),
                    collection_id.eq(excluded(collection_id)),
                )),
            Some(" WHERE current_token_pending_claims.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

async fn insert_nft_points(
    conn: &mut MyDbConnection,
    items_to_insert: &[NftPoints],
) -> Result<(), diesel::result::Error> {
    use schema::nft_points::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), NftPoints::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::nft_points::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(transaction_version)
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for TokenProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::TokenProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u8>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        // Token V1 only, this section will be deprecated soon
        let mut all_tokens = vec![];
        let mut all_token_ownerships = vec![];
        let mut all_token_datas = vec![];
        let mut all_collection_datas = vec![];
        let mut all_token_activities = vec![];

        // Hashmap key will be the PK of the table, we do not want to send duplicates writes to the db within a batch
        let mut all_current_token_ownerships: HashMap<
            CurrentTokenOwnershipPK,
            CurrentTokenOwnership,
        > = HashMap::new();
        let mut all_current_token_datas: HashMap<TokenDataIdHash, CurrentTokenData> =
            HashMap::new();
        let mut all_current_collection_datas: HashMap<TokenDataIdHash, CurrentCollectionData> =
            HashMap::new();
        let mut all_current_token_claims: HashMap<
            CurrentTokenPendingClaimPK,
            CurrentTokenPendingClaim,
        > = HashMap::new();

        // This is likely temporary
        let mut all_nft_points = vec![];

        for txn in &transactions {
            let (
                mut tokens,
                mut token_ownerships,
                mut token_datas,
                mut collection_datas,
                current_token_ownerships,
                current_token_datas,
                current_collection_datas,
                current_token_claims,
            ) = Token::from_transaction(txn, &table_handle_to_owner, &mut conn).await;
            all_tokens.append(&mut tokens);
            all_token_ownerships.append(&mut token_ownerships);
            all_token_datas.append(&mut token_datas);
            all_collection_datas.append(&mut collection_datas);
            // Given versions will always be increasing here (within a single batch), we can just override current values
            all_current_token_ownerships.extend(current_token_ownerships);
            all_current_token_datas.extend(current_token_datas);
            all_current_collection_datas.extend(current_collection_datas);

            // Track token activities
            let mut activities = TokenActivity::from_transaction(txn);
            all_token_activities.append(&mut activities);

            // claims
            all_current_token_claims.extend(current_token_claims);

            // NFT points
            let nft_points_txn =
                NftPoints::from_transaction(txn, self.config.nft_points_contract.clone());
            if let Some(nft_points) = nft_points_txn {
                all_nft_points.push(nft_points);
            }
        }

        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut all_current_token_ownerships = all_current_token_ownerships
            .into_values()
            .collect::<Vec<CurrentTokenOwnership>>();
        let mut all_current_token_datas = all_current_token_datas
            .into_values()
            .collect::<Vec<CurrentTokenData>>();
        let mut all_current_collection_datas = all_current_collection_datas
            .into_values()
            .collect::<Vec<CurrentCollectionData>>();
        let mut all_current_token_claims = all_current_token_claims
            .into_values()
            .collect::<Vec<CurrentTokenPendingClaim>>();

        // Sort by PK
        all_current_token_ownerships.sort_by(|a, b| {
            (&a.token_data_id_hash, &a.property_version, &a.owner_address).cmp(&(
                &b.token_data_id_hash,
                &b.property_version,
                &b.owner_address,
            ))
        });
        all_current_token_datas.sort_by(|a, b| a.token_data_id_hash.cmp(&b.token_data_id_hash));
        all_current_collection_datas
            .sort_by(|a, b| a.collection_data_id_hash.cmp(&b.collection_data_id_hash));
        all_current_token_claims.sort_by(|a, b| {
            (
                &a.token_data_id_hash,
                &a.property_version,
                &a.from_address,
                &a.to_address,
            )
                .cmp(&(
                    &b.token_data_id_hash,
                    &b.property_version,
                    &b.from_address,
                    &a.to_address,
                ))
        });

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            (
                all_tokens,
                all_token_ownerships,
                all_token_datas,
                all_collection_datas,
            ),
            (
                all_current_token_ownerships,
                all_current_token_datas,
                all_current_collection_datas,
            ),
            all_token_activities,
            all_current_token_claims,
            all_nft_points,
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
}

#[async_trait]
impl ProcessorStorageTrait for TokenProcessor {
    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
