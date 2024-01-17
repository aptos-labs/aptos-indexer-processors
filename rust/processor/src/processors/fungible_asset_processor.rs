// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        fungible_asset_models::{
            v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
            v2_fungible_asset_balances::{
                CurrentFungibleAssetBalance, CurrentFungibleAssetMapping, FungibleAssetBalance,
            },
            v2_fungible_asset_utils::{FeeStatement, FungibleAssetMetadata, FungibleAssetStore},
            v2_fungible_metadata::{FungibleAssetMetadataMapping, FungibleAssetMetadataModel},
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
        token_v2_models::v2_token_utils::TokenV2,
    },
    schema,
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
            PgPoolConnection,
        },
        util::{get_entry_function_from_user_request, standardize_address},
    },
};
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";
pub struct FungibleAssetProcessor {
    connection_pool: PgDbPool,
}

impl FungibleAssetProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for FungibleAssetProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "FungibleAssetTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    fungible_asset_activities: &[FungibleAssetActivity],
    fungible_asset_metadata: &[FungibleAssetMetadataModel],
    fungible_asset_balances: &[FungibleAssetBalance],
    current_fungible_asset_balances: &[CurrentFungibleAssetBalance],
) -> Result<(), diesel::result::Error> {
    insert_fungible_asset_activities(conn, fungible_asset_activities).await?;
    insert_fungible_asset_metadata(conn, fungible_asset_metadata).await?;
    insert_fungible_asset_balances(conn, fungible_asset_balances).await?;
    insert_current_fungible_asset_balances(conn, current_fungible_asset_balances).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    fungible_asset_activities: Vec<FungibleAssetActivity>,
    fungible_asset_metadata: Vec<FungibleAssetMetadataModel>,
    fungible_asset_balances: Vec<FungibleAssetBalance>,
    current_fungible_asset_balances: Vec<CurrentFungibleAssetBalance>,
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
                &fungible_asset_activities,
                &fungible_asset_metadata,
                &fungible_asset_balances,
                &current_fungible_asset_balances,
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
                        let fungible_asset_activities =
                            clean_data_for_db(fungible_asset_activities, true);
                        let fungible_asset_metadata =
                            clean_data_for_db(fungible_asset_metadata, true);
                        let fungible_asset_balances =
                            clean_data_for_db(fungible_asset_balances, true);
                        let current_fungible_asset_balances =
                            clean_data_for_db(current_fungible_asset_balances, true);

                        insert_to_db_impl(
                            pg_conn,
                            &fungible_asset_activities,
                            &fungible_asset_metadata,
                            &fungible_asset_balances,
                            &current_fungible_asset_balances,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_fungible_asset_activities(
    conn: &mut MyDbConnection,
    item_to_insert: &[FungibleAssetActivity],
) -> Result<(), diesel::result::Error> {
    use schema::fungible_asset_activities::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), FungibleAssetActivity::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::fungible_asset_activities::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, event_index))
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_fungible_asset_metadata(
    conn: &mut MyDbConnection,
    item_to_insert: &[FungibleAssetMetadataModel],
) -> Result<(), diesel::result::Error> {
    use schema::fungible_asset_metadata::dsl::*;

    let chunks = get_chunks(
        item_to_insert.len(),
        FungibleAssetMetadataModel::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::fungible_asset_metadata::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict(asset_type)
                .do_update()
                .set(
                    (
                        creator_address.eq(excluded(creator_address)),
                        name.eq(excluded(name)),
                        symbol.eq(excluded(symbol)),
                        decimals.eq(excluded(decimals)),
                        icon_uri.eq(excluded(icon_uri)),
                        project_uri.eq(excluded(project_uri)),
                        last_transaction_version.eq(excluded(last_transaction_version)),
                        last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                        supply_aggregator_table_handle_v1.eq(excluded(supply_aggregator_table_handle_v1)),
                        supply_aggregator_table_key_v1.eq(excluded(supply_aggregator_table_key_v1)),
                        token_standard.eq(excluded(token_standard)),
                        inserted_at.eq(excluded(inserted_at)),
                    )
                ),
            Some(" WHERE fungible_asset_metadata.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

async fn insert_fungible_asset_balances(
    conn: &mut MyDbConnection,
    item_to_insert: &[FungibleAssetBalance],
) -> Result<(), diesel::result::Error> {
    use schema::fungible_asset_balances::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), FungibleAssetBalance::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::fungible_asset_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_update()
                .set((
                    is_frozen.eq(excluded(is_frozen)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_current_fungible_asset_balances(
    conn: &mut MyDbConnection,
    item_to_insert: &[CurrentFungibleAssetBalance],
) -> Result<(), diesel::result::Error> {
    use schema::current_fungible_asset_balances::dsl::*;

    let chunks: Vec<(usize, usize)> = get_chunks(
        item_to_insert.len(),
        CurrentFungibleAssetBalance::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_fungible_asset_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict(storage_id)
                .do_update()
                .set(
                    (
                        owner_address.eq(excluded(owner_address)),
                        asset_type.eq(excluded(asset_type)),
                        is_primary.eq(excluded(is_primary)),
                        is_frozen.eq(excluded(is_frozen)),
                        amount.eq(excluded(amount)),
                        last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                        last_transaction_version.eq(excluded(last_transaction_version)),
                        token_standard.eq(excluded(token_standard)),
                        inserted_at.eq(excluded(inserted_at)),
                    )
                ),
            Some(" WHERE current_fungible_asset_balances.last_transaction_version <= excluded.last_transaction_version "),
        ).await?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for FungibleAssetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::FungibleAssetProcessor.into()
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
            fungible_asset_activities,
            fungible_asset_metadata,
            fungible_asset_balances,
            current_fungible_asset_balances,
        ) = parse_v2_coin(&transactions, &mut conn).await;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            fungible_asset_activities,
            fungible_asset_metadata,
            fungible_asset_balances,
            current_fungible_asset_balances,
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
                    "[Parser] Error inserting transactions to db: {:?}",
                    err
                );
                bail!(format!("Error inserting transactions to db. Processor {}. Start {}. End {}. Error {:?}", self.name(), start_version, end_version, err))
            },
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

/// V2 coin is called fungible assets and this flow includes all data from V1 in coin_processor
async fn parse_v2_coin(
    transactions: &[Transaction],
    conn: &mut PgPoolConnection<'_>,
) -> (
    Vec<FungibleAssetActivity>,
    Vec<FungibleAssetMetadataModel>,
    Vec<FungibleAssetBalance>,
    Vec<CurrentFungibleAssetBalance>,
) {
    let mut fungible_asset_activities = vec![];
    let mut fungible_asset_balances = vec![];
    let mut current_fungible_asset_balances: CurrentFungibleAssetMapping = HashMap::new();
    let mut fungible_asset_metadata: FungibleAssetMetadataMapping = HashMap::new();

    // Get Metadata for fungible assets by object
    let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = HashMap::new();

    for txn in transactions {
        let txn_version = txn.version as i64;
        let block_height = txn.block_height as i64;
        let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        let txn_timestamp = txn
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");

        let default = vec![];
        let (events, user_request, entry_function_id_str) = match txn_data {
            TxnData::BlockMetadata(tx_inner) => (&tx_inner.events, None, None),
            TxnData::Genesis(tx_inner) => (&tx_inner.events, None, None),
            TxnData::User(tx_inner) => {
                let user_request = tx_inner
                    .request
                    .as_ref()
                    .expect("Sends is not present in user txn");
                let entry_function_id_str = get_entry_function_from_user_request(user_request);
                (&tx_inner.events, Some(user_request), entry_function_id_str)
            },
            _ => (&default, None, None),
        };

        // This is because v1 events (deposit/withdraw) don't have coin type so the only way is to match
        // the event to the resource using the event guid
        let mut event_to_v1_coin_type: EventToCoinType = HashMap::new();

        // First loop to get all objects
        // Need to do a first pass to get all the objects
        for wsc in transaction_info.changes.iter() {
            if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                if let Some(object) =
                    ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                {
                    fungible_asset_object_helper.insert(
                        standardize_address(&wr.address.to_string()),
                        ObjectAggregatedData {
                            object,
                            fungible_asset_metadata: None,
                            fungible_asset_store: None,
                            token: None,
                            // The following structs are unused in this processor
                            aptos_collection: None,
                            fixed_supply: None,
                            unlimited_supply: None,
                            property_map: None,
                            transfer_event: None,
                            fungible_asset_supply: None,
                        },
                    );
                }
            }
        }
        // Loop to get the metadata relevant to parse v1 and v2.
        // As an optimization, we also handle v1 balances in the process
        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some((balance, current_balance, event_to_coin)) =
                    FungibleAssetBalance::get_v1_from_write_resource(
                        write_resource,
                        index as i64,
                        txn_version,
                        txn_timestamp,
                    )
                    .unwrap()
                {
                    fungible_asset_balances.push(balance);
                    current_fungible_asset_balances
                        .insert(current_balance.storage_id.clone(), current_balance.clone());
                    event_to_v1_coin_type.extend(event_to_coin);
                }
                // Fill the v2 object metadata
                let address = standardize_address(&write_resource.address.to_string());
                if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                    if let Some(fungible_asset_metadata) =
                        FungibleAssetMetadata::from_write_resource(write_resource, txn_version)
                            .unwrap()
                    {
                        aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                    }
                    if let Some(fungible_asset_store) =
                        FungibleAssetStore::from_write_resource(write_resource, txn_version)
                            .unwrap()
                    {
                        aggregated_data.fungible_asset_store = Some(fungible_asset_store);
                    }
                    if let Some(token) =
                        TokenV2::from_write_resource(write_resource, txn_version).unwrap()
                    {
                        aggregated_data.token = Some(token);
                    }
                }
            }
        }

        // The artificial gas event, only need for v1
        if let Some(req) = user_request {
            let fee_statement = events.iter().find_map(|event| {
                let event_type = event.type_str.as_str();
                FeeStatement::from_event(event_type, &event.data, txn_version)
            });
            let gas_event = FungibleAssetActivity::get_gas_event(
                transaction_info,
                req,
                &entry_function_id_str,
                txn_version,
                txn_timestamp,
                block_height,
                fee_statement,
            );
            fungible_asset_activities.push(gas_event);
        }

        // Loop to handle events and collect additional metadata from events for v2
        for (index, event) in events.iter().enumerate() {
            if let Some(v1_activity) = FungibleAssetActivity::get_v1_from_event(
                event,
                txn_version,
                block_height,
                txn_timestamp,
                &entry_function_id_str,
                &event_to_v1_coin_type,
                index as i64,
            )
            .unwrap_or_else(|e| {
                tracing::error!(
                    transaction_version = txn_version,
                    index = index,
                    error = ?e,
                    "[Parser] error parsing fungible asset activity v1");
                panic!("[Parser] error parsing fungible asset activity v1");
            }) {
                fungible_asset_activities.push(v1_activity);
            }
            if let Some(v2_activity) = FungibleAssetActivity::get_v2_from_event(
                event,
                txn_version,
                block_height,
                txn_timestamp,
                index as i64,
                &entry_function_id_str,
                &fungible_asset_object_helper,
                conn,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!(
                    transaction_version = txn_version,
                    index = index,
                    error = ?e,
                    "[Parser] error parsing fungible asset activity v2");
                panic!("[Parser] error parsing fungible asset activity v2");
            }) {
                fungible_asset_activities.push(v2_activity);
            }
        }

        // Loop to handle all the other changes
        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some(fa_metadata) = FungibleAssetMetadataModel::get_v1_from_write_resource(
                    write_resource,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                            error = ?e,
                        "[Parser] error parsing fungible metadata v1");
                    panic!("[Parser] error parsing fungible metadata v1");
                }) {
                    fungible_asset_metadata.insert(fa_metadata.asset_type.clone(), fa_metadata);
                }
                if let Some(fa_metadata) = FungibleAssetMetadataModel::get_v2_from_write_resource(
                    write_resource,
                    txn_version,
                    txn_timestamp,
                    &fungible_asset_object_helper,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                            error = ?e,
                        "[Parser] error parsing fungible metadata v2");
                    panic!("[Parser] error parsing fungible metadata v2");
                }) {
                    fungible_asset_metadata.insert(fa_metadata.asset_type.clone(), fa_metadata);
                }
                if let Some((balance, curr_balance)) =
                    FungibleAssetBalance::get_v2_from_write_resource(
                        write_resource,
                        index as i64,
                        txn_version,
                        txn_timestamp,
                        &fungible_asset_object_helper,
                        conn,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                            error = ?e,
                        "[Parser] error parsing fungible balance v2");
                        panic!("[Parser] error parsing fungible balance v2");
                    })
                {
                    fungible_asset_balances.push(balance);
                    current_fungible_asset_balances
                        .insert(curr_balance.storage_id.clone(), curr_balance);
                }
            }
        }
    }

    // Boilerplate after this
    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut fungible_asset_metadata = fungible_asset_metadata
        .into_values()
        .collect::<Vec<FungibleAssetMetadataModel>>();
    let mut current_fungible_asset_balances = current_fungible_asset_balances
        .into_values()
        .collect::<Vec<CurrentFungibleAssetBalance>>();
    // Sort by PK
    fungible_asset_metadata.sort_by(|a, b| a.asset_type.cmp(&b.asset_type));
    current_fungible_asset_balances.sort_by(|a, b| a.storage_id.cmp(&b.storage_id));

    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        current_fungible_asset_balances,
    )
}
