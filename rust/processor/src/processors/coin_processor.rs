// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        coin_models::{
            coin_activities::CoinActivity,
            coin_balances::{CoinBalance, CurrentCoinBalance},
            coin_infos::CoinInfo,
        },
        fungible_asset_models::v2_fungible_asset_activities::CurrentCoinBalancePK,
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::{bail, Context};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tracing::error;

pub struct CoinProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl CoinProcessor {
    pub fn new(connection_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for CoinProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "CoinTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    coin_activities: &[CoinActivity],
    coin_infos: &[CoinInfo],
    coin_balances: &[CoinBalance],
    current_coin_balances: &[CurrentCoinBalance],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let ca = execute_in_chunks(
        conn.clone(),
        insert_coin_activities_query,
        coin_activities,
        get_config_table_chunk_size::<CoinActivity>("coin_activities", per_table_chunk_sizes),
    );
    let ci = execute_in_chunks(
        conn.clone(),
        insert_coin_infos_query,
        coin_infos,
        get_config_table_chunk_size::<CoinInfo>("coin_infos", per_table_chunk_sizes),
    );
    let cb = execute_in_chunks(
        conn.clone(),
        insert_coin_balances_query,
        coin_balances,
        get_config_table_chunk_size::<CoinBalance>("coin_balances", per_table_chunk_sizes),
    );
    let ccb = execute_in_chunks(
        conn.clone(),
        insert_current_coin_balances_query,
        current_coin_balances,
        get_config_table_chunk_size::<CurrentCoinBalance>(
            "current_coin_balances",
            per_table_chunk_sizes,
        ),
    );

    let (ca_res, ci_res, cb_res, ccb_res) = tokio::join!(ca, ci, cb, ccb);
    for res in [ca_res, ci_res, cb_res, ccb_res] {
        res?;
    }
    Ok(())
}

fn insert_coin_activities_query(
    items_to_insert: Vec<CoinActivity>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::coin_activities::dsl::*;

    (
        diesel::insert_into(schema::coin_activities::table)
            .values(items_to_insert)
            .on_conflict((
                transaction_version,
                event_account_address,
                event_creation_number,
                event_sequence_number,
            ))
            .do_update()
            .set((
                entry_function_id_str.eq(excluded(entry_function_id_str)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

fn insert_coin_infos_query(
    items_to_insert: Vec<CoinInfo>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::coin_infos::dsl::*;

    (
        diesel::insert_into(schema::coin_infos::table)
            .values(items_to_insert)
            .on_conflict(coin_type_hash)
            .do_update()
            .set((
                transaction_version_created.eq(excluded(transaction_version_created)),
                creator_address.eq(excluded(creator_address)),
                name.eq(excluded(name)),
                symbol.eq(excluded(symbol)),
                decimals.eq(excluded(decimals)),
                transaction_created_timestamp.eq(excluded(transaction_created_timestamp)),
                supply_aggregator_table_handle.eq(excluded(supply_aggregator_table_handle)),
                supply_aggregator_table_key.eq(excluded(supply_aggregator_table_key)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE coin_infos.transaction_version_created >= EXCLUDED.transaction_version_created "),
    )
}

fn insert_coin_balances_query(
    items_to_insert: Vec<CoinBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::coin_balances::dsl::*;

    (
        diesel::insert_into(schema::coin_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, owner_address, coin_type_hash))
            .do_nothing(),
        None,
    )
}

fn insert_current_coin_balances_query(
    items_to_insert: Vec<CurrentCoinBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_coin_balances::dsl::*;

    (
        diesel::insert_into(schema::current_coin_balances::table)
            .values(items_to_insert)
            .on_conflict((owner_address, coin_type_hash))
            .do_update()
            .set((
                amount.eq(excluded(amount)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_coin_balances.last_transaction_version <= excluded.last_transaction_version "),
    )
}

#[async_trait]
impl ProcessorTrait for CoinProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::CoinProcessor.into()
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

        let (
            all_coin_activities,
            all_coin_infos,
            all_coin_balances,
            all_current_coin_balances,
        ) = tokio::task::spawn_blocking(move || {
            let mut all_coin_activities = vec![];
            let mut all_coin_balances = vec![];
            let mut all_coin_infos: AHashMap<String, CoinInfo> = AHashMap::new();
            let mut all_current_coin_balances: AHashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
                AHashMap::new();

            for txn in &transactions {
                let (mut coin_activities, mut coin_balances, coin_infos, current_coin_balances) =
                    CoinActivity::from_transaction(txn);
                all_coin_activities.append(&mut coin_activities);
                all_coin_balances.append(&mut coin_balances);
                // For coin infos, we only want to keep the first version, so insert only if key is not present already
                for (key, value) in coin_infos {
                    all_coin_infos.entry(key).or_insert(value);
                }
                all_current_coin_balances.extend(current_coin_balances);
            }
            let mut all_coin_infos = all_coin_infos.into_values().collect::<Vec<CoinInfo>>();
            let mut all_current_coin_balances = all_current_coin_balances
                .into_values()
                .collect::<Vec<CurrentCoinBalance>>();

            // Sort by PK
            all_coin_infos.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
            all_current_coin_balances.sort_by(|a, b| {
                (&a.owner_address, &a.coin_type).cmp(&(&b.owner_address, &b.coin_type))
            });

            (
                all_coin_activities,
                all_coin_infos,
                all_coin_balances,
                all_current_coin_balances,
            )
        })
        .await
        .context("spawn_blocking for CoinProcessor thread failed")?;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &all_coin_activities,
            &all_coin_infos,
            &all_coin_balances,
            &all_current_coin_balances,
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
            Err(err) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    "[Parser] Error inserting transactions to db: {:?}",
                    err
                );
                bail!(format!("Error inserting transactions to db. Processor {}. Start {}. End {}. Error {:?}", self.name(), start_version, end_version, err))
            },
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
