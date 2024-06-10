// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::poke_models::Poke,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, PgDbPool},
        util::parse_timestamp,
    },
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
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
pub struct PokeProcessorConfig {
    // TODO: Use AccountAddress when that type is readily usable without pulling in a
    // million deps.
    pub module_address: String,
}

pub struct PokeProcessor {
    connection_pool: PgDbPool,
    config: PokeProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl PokeProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        config: PokeProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
    ) -> Self {
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for PokeProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "PokeProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: PgDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    pokes: &[Poke],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    execute_in_chunks(
        conn.clone(),
        insert_pokes_query,
        pokes,
        get_config_table_chunk_size::<Poke>("current_pokes", per_table_chunk_sizes),
    )
    .await?;

    Ok(())
}

/// Insert or update pokes.
fn insert_pokes_query(
    items_to_insert: Vec<Poke>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_pokes::dsl::*;

    (
        diesel::insert_into(schema::current_pokes::table)
            .values(items_to_insert)
            .on_conflict((initial_poker_address, recipient_poker_address))
            .do_update()
            .set((
                // Only update the following fields, the others never change.
                initial_poker_is_next_poker.eq(excluded(initial_poker_is_next_poker)),
                last_poke_at.eq(excluded(last_poke_at)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                times_poked.eq(excluded(times_poked)),
            )),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for PokeProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::PokeProcessor.into()
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

        let mut all_pokes: Vec<Poke> = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
            let txn_data = match txn.txn_data.as_ref() {
                Some(data) => data,
                None => {
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["PokeProcessor"])
                        .inc();
                    continue;
                },
            };
            let raw_events = match txn_data {
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => continue,
            };
            let txn_pokes: Vec<Poke> = raw_events
                .iter()
                // Filter out None.
                .filter_map(|e| {
                    Poke::from_event(e, &self.config.module_address, txn_version, txn_timestamp)
                        .transpose()
                })
                .collect::<anyhow::Result<Vec<Poke>>>()?;
            all_pokes.extend(txn_pokes);
        }

        let mut keyed_pokes = AHashMap::new();

        // Add all pokes to a map keyed by the pair. This way we only have the most
        // recent event for each pair.
        for poke in all_pokes.into_iter() {
            let key = MapKey {
                initial_poker_address: poke.initial_poker_address.to_string(),
                recipient_poker_address: poke.recipient_poker_address.to_string(),
            };
            keyed_pokes.insert(key, poke);
        }

        let pokes = keyed_pokes.into_values().collect::<Vec<Poke>>();

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            pokes.as_slice(),
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

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct MapKey {
    pub initial_poker_address: String,
    pub recipient_poker_address: String,
}
