// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::events_models::events::EventModel,
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use diesel::{result::Error, PgConnection};
use field_count::FieldCount;
use std::fmt::Debug;
use tracing::error;

pub const NAME: &str = "events_processor";
pub struct EventsProcessor {
    connection_pool: PgDbPool,
}

impl EventsProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for EventsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "EventsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db_impl(
    conn: &mut PgConnection,
    events: &[EventModel],
) -> Result<(), diesel::result::Error> {
    insert_events(conn, events)?;
    Ok(())
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    events: Vec<EventModel>,
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
        .run::<_, Error, _>(|pg_conn| insert_to_db_impl(pg_conn, &events))
    {
        Ok(_) => Ok(()),
        Err(_) => {
            let events = clean_data_for_db(events, true);

            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| insert_to_db_impl(pg_conn, &events))
        },
    }
}

fn insert_events(
    conn: &mut PgConnection,
    items_to_insert: &[EventModel],
) -> Result<(), diesel::result::Error> {
    use schema::events::dsl::*;
    let chunks = get_chunks(items_to_insert.len(), EventModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::events::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, event_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn();
        let mut events = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
            events.extend(txn_events);
        }

        let tx_result = insert_to_db(&mut conn, self.name(), start_version, end_version, events);
        match tx_result {
            Ok(_) => Ok((start_version, end_version)),
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
