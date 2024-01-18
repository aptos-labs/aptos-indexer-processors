// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::nft_marketplace_models::nft_marketplace_activities::NftMarketplaceActivity,
    schema::{self},
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::{get_entry_function_from_user_request, get_clean_payload},
    },
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::result::Error;
use field_count::FieldCount;
use std::fmt::Debug;
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
) -> Result<(), diesel::result::Error> {
    insert_nft_marketplace_activities(conn, nft_marketplace_activities).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    nft_marketplace_activities: Vec<NftMarketplaceActivity>,
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

                        insert_to_db_impl(
                            pg_conn,
                            &nft_marketplace_activities,
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
        let nft_marketplace_activities = parse_transactions(&transactions).await;
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        // Start insert to db
        let db_insertion_start = std::time::Instant::now();
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            nft_marketplace_activities,
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
) -> Vec<NftMarketplaceActivity> {
    let mut nft_marketplace_activities = vec![];
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

            // Loop through the events
            for (index, event) in inner.events.iter().enumerate() {
                if let Some(marketplace_activity) =
                    NftMarketplaceActivity::from_event(
                        event,
                        txn_version,
                        index as i64,
                        &get_entry_function_from_user_request(inner.request.as_ref().unwrap()),
                        &coin_type,
                        txn_timestamp,
                    )
                    .expect("Failed to parse event!")
                {
                    nft_marketplace_activities.push(marketplace_activity);
                }
            }
        }
    }
    nft_marketplace_activities
}
