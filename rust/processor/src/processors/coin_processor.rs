// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    diesel::ExpressionMethods,
    models::{
        coin_models::{
            coin_activities::CoinActivity,
            coin_balances::{CoinBalance, CurrentCoinBalance},
            coin_infos::CoinInfo,
            coin_supply::CoinSupply,
        },
        fungible_asset_models::v2_fungible_asset_activities::CurrentCoinBalancePK,
    },
    schema,
};
use ahash::AHashMap;
use anyhow::Context;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::upsert::excluded, query_builder::QueryFragment, query_dsl::filter_dsl::FilterDsl,
};

pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";

pub struct CoinProcessor {
    db_writer: crate::db_writer::DbWriter,
}

impl std::fmt::Debug for CoinProcessor {
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

impl CoinProcessor {
    pub fn new(db_writer: crate::db_writer::DbWriter) -> Self {
        Self { db_writer }
    }
}

async fn insert_to_db(
    db_writer: &crate::db_writer::DbWriter,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    coin_activities: Vec<CoinActivity>,
    coin_infos: Vec<CoinInfo>,
    coin_balances: Vec<CoinBalance>,
    current_coin_balances: Vec<CurrentCoinBalance>,
    coin_supply: Vec<CoinSupply>,
) {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Finished parsing, sending to DB",
    );

    let ca = db_writer.send_in_chunks(
        "coin_activities",
        coin_activities,
        insert_coin_activities_query,
    );
    let ci = db_writer.send_in_chunks("coin_infos", coin_infos, insert_coin_infos_query);
    let cb = db_writer.send_in_chunks("coin_balances", coin_balances, insert_coin_balances_query);
    let ccb = db_writer.send_in_chunks(
        "current_coin_balances",
        current_coin_balances,
        insert_current_coin_balances_query,
    );

    let cs = db_writer.send_in_chunks("coin_supply", coin_supply, insert_coin_supplys_query);

    tokio::join!(ca, ci, cb, ccb, cs);
}

pub fn insert_coin_activities_query(
    items_to_insert: &[CoinActivity],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::coin_activities::dsl::*;

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
        ))
}

pub fn insert_coin_infos_query(
    items_to_insert: &[CoinInfo],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::coin_infos::dsl::*;

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
        ))
        .filter(transaction_version_created.ge(excluded(transaction_version_created)))
}

pub fn insert_coin_balances_query(
    items_to_insert: &[CoinBalance],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::coin_balances::dsl::*;

    diesel::insert_into(schema::coin_balances::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, owner_address, coin_type_hash))
        .do_nothing()
}

pub fn insert_current_coin_balances_query(
    items_to_insert: &[CurrentCoinBalance],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::current_coin_balances::dsl::*;

    diesel::insert_into(schema::current_coin_balances::table)
        .values(items_to_insert)
        .on_conflict((owner_address, coin_type_hash))
        .do_update()
        .set((
            amount.eq(excluded(amount)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(last_transaction_version.le(excluded(last_transaction_version)))
}

pub fn insert_coin_supplys_query(
    items_to_insert: &[CoinSupply],
) -> impl QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryId + Sync + Send + '_ {
    use crate::schema::coin_supply::dsl::*;

    diesel::insert_into(schema::coin_supply::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, coin_type_hash))
        .do_nothing()
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
            all_coin_supply,
        ) = tokio::task::spawn_blocking(move || {
            let mut all_coin_activities = vec![];
            let mut all_coin_balances = vec![];
            let mut all_coin_infos: AHashMap<String, CoinInfo> = AHashMap::new();
            let mut all_current_coin_balances: AHashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
                AHashMap::new();
            let mut all_coin_supply = vec![];

            for txn in &transactions {
                let (
                    mut coin_activities,
                    mut coin_balances,
                    coin_infos,
                    current_coin_balances,
                    mut coin_supply,
                ) = CoinActivity::from_transaction(txn);
                all_coin_activities.append(&mut coin_activities);
                all_coin_balances.append(&mut coin_balances);
                all_coin_supply.append(&mut coin_supply);
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
                all_coin_supply,
            )
        })
        .await
        .context("spawn_blocking for CoinProcessor thread failed")?;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        insert_to_db(
            self.db_writer(),
            self.name(),
            start_version,
            end_version,
            all_coin_activities,
            all_coin_infos,
            all_coin_balances,
            all_current_coin_balances,
            all_coin_supply,
        )
        .await;

        let db_channel_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_channel_insertion_duration_in_secs,
            last_transaction_timestamp,
        })
    }

    fn db_writer(&self) -> &crate::db_writer::DbWriter {
        &self.db_writer
    }
}
