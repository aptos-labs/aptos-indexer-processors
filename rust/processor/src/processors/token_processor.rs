// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
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
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, query_dsl::filter_dsl::FilterDsl};
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TokenProcessorConfig {
    pub nft_points_contract: Option<String>,
}

pub struct TokenProcessor {
    db_writer: crate::db_writer::DbWriter,
    config: TokenProcessorConfig,
}

impl std::fmt::Debug for TokenProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool().state();
        write!(
            f,
            "{:} {{ connections: {:?}  idle_connections: {:?} }}",
            self.name(),
            state.connections,
            state.idle_connections
        )
    }
}

impl TokenProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter, config: TokenProcessorConfig) -> Self {
        Self { db_writer, config }
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    (tokens, token_ownerships, token_datas, collection_datas): (
        Vec<Token>,
        Vec<TokenOwnership>,
        Vec<TokenData>,
        Vec<CollectionData>,
    ),
    (current_token_ownerships, current_token_datas, current_collection_datas): (
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
    let t = db_writer.send_in_chunks("tokens", tokens);
    let to = db_writer.send_in_chunks("token_ownerships", token_ownerships);
    let td = db_writer.send_in_chunks("token_datas", token_datas);
    let cd = db_writer.send_in_chunks("collection_datas", collection_datas);
    let cto = db_writer.send_in_chunks("current_token_ownerships", current_token_ownerships);
    let ctd = db_writer.send_in_chunks("current_token_datas", current_token_datas);
    let ccd = db_writer.send_in_chunks("current_collection_datas", current_collection_datas);
    let ta = db_writer.send_in_chunks("token_activities", token_activities);
    let ctc = db_writer.send_in_chunks("current_token_pending_claims", current_token_claims);
    let np = db_writer.send_in_chunks("nft_points", nft_points);

    tokio::join!(t, to, td, cd, cto, ctd, ccd, ta, ctc, np);

    Ok(())
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<Token> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::tokens::dsl::*;

        let query = diesel::insert_into(schema::tokens::table)
            .values(self)
            .on_conflict((token_data_id_hash, property_version, transaction_version))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<TokenOwnership> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::token_ownerships::dsl::*;

        let query = diesel::insert_into(schema::token_ownerships::table)
            .values(self)
            .on_conflict((
                token_data_id_hash,
                property_version,
                transaction_version,
                table_handle,
            ))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<TokenData> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::token_datas::dsl::*;

        let query = diesel::insert_into(schema::token_datas::table)
            .values(self)
            .on_conflict((token_data_id_hash, transaction_version))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<CollectionData> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::collection_datas::dsl::*;

        let query = diesel::insert_into(schema::collection_datas::table)
            .values(self)
            .on_conflict((collection_data_id_hash, transaction_version))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<CurrentTokenOwnership> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::current_token_ownerships::dsl::*;

        let query = diesel::insert_into(schema::current_token_ownerships::table)
            .values(self)
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
            ))
            .filter(last_transaction_version.le(excluded(last_transaction_version)));
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<CurrentTokenData> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::current_token_datas::dsl::*;

        let query = diesel::insert_into(schema::current_token_datas::table)
            .values(self)
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
            ))
            .filter(last_transaction_version.le(excluded(last_transaction_version)));
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<CurrentCollectionData> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::current_collection_datas::dsl::*;

        let query = diesel::insert_into(schema::current_collection_datas::table)
            .values(self)
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
            ))
            .filter(last_transaction_version.le(excluded(last_transaction_version)));
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<TokenActivity> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::token_activities::dsl::*;

        let query = diesel::insert_into(schema::token_activities::table)
            .values(self)
            .on_conflict((
                transaction_version,
                event_account_address,
                event_creation_number,
                event_sequence_number,
            ))
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<CurrentTokenPendingClaim> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::current_token_pending_claims::dsl::*;

        let query = diesel::insert_into(schema::current_token_pending_claims::table)
            .values(self)
            .on_conflict((
                token_data_id_hash,
                property_version,
                from_address,
                to_address,
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
            ))
            .filter(last_transaction_version.le(excluded(last_transaction_version)));
        crate::db_writer::execute_with_better_error(conn, query).await
    }
}

#[async_trait::async_trait]
impl crate::db_writer::DbExecutable for Vec<NftPoints> {
    async fn execute_query(
        &'_ self,
        conn: crate::utils::database::PgDbPool,
    ) -> diesel::QueryResult<usize> {
        use crate::schema::nft_points::dsl::*;

        let query = diesel::insert_into(schema::nft_points::table)
            .values(self)
            .on_conflict(transaction_version)
            .do_nothing();
        crate::db_writer::execute_with_better_error(conn, query).await
    }
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
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

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
        let mut all_current_token_ownerships: AHashMap<
            CurrentTokenOwnershipPK,
            CurrentTokenOwnership,
        > = AHashMap::new();
        let mut all_current_token_datas: AHashMap<TokenDataIdHash, CurrentTokenData> =
            AHashMap::new();
        let mut all_current_collection_datas: AHashMap<TokenDataIdHash, CurrentCollectionData> =
            AHashMap::new();
        let mut all_current_token_claims: AHashMap<
            CurrentTokenPendingClaimPK,
            CurrentTokenPendingClaim,
        > = AHashMap::new();

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
            self.db_writer(),
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

        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_channel_insertion_duration_in_secs,
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

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
