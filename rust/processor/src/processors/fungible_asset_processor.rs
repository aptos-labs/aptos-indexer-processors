// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::{
        coin_models::coin_supply::CoinSupply,
        fungible_asset_models::{
            v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
            v2_fungible_asset_balances::{
                CurrentFungibleAssetBalance, CurrentFungibleAssetMapping,
                CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
            },
            v2_fungible_asset_utils::{
                ConcurrentFungibleAssetBalance, ConcurrentFungibleAssetSupply, FeeStatement,
                FungibleAssetMetadata, FungibleAssetStore, FungibleAssetSupply,
            },
            v2_fungible_metadata::{FungibleAssetMetadataMapping, FungibleAssetMetadataModel},
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
    },
    gap_detectors::ProcessingResult,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        util::{get_entry_function_from_user_request, standardize_address},
    },
    worker::TableFlags,
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tracing::error;

pub struct FungibleAssetProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    deprecated_tables: TableFlags,
}

impl FungibleAssetProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        per_table_chunk_sizes: AHashMap<String, usize>,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
            deprecated_tables,
        }
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

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    fungible_asset_activities: &[FungibleAssetActivity],
    fungible_asset_metadata: &[FungibleAssetMetadataModel],
    fungible_asset_balances: &[FungibleAssetBalance],
    current_fungible_asset_balances: &[CurrentFungibleAssetBalance],
    current_unified_fungible_asset_balances: (
        &[CurrentUnifiedFungibleAssetBalance],
        &[CurrentUnifiedFungibleAssetBalance],
    ),
    coin_supply: &[CoinSupply],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let faa = execute_in_chunks(
        conn.clone(),
        insert_fungible_asset_activities_query,
        fungible_asset_activities,
        get_config_table_chunk_size::<FungibleAssetActivity>(
            "fungible_asset_activities",
            per_table_chunk_sizes,
        ),
    );
    let fam = execute_in_chunks(
        conn.clone(),
        insert_fungible_asset_metadata_query,
        fungible_asset_metadata,
        get_config_table_chunk_size::<FungibleAssetMetadataModel>(
            "fungible_asset_metadata",
            per_table_chunk_sizes,
        ),
    );
    let fab = execute_in_chunks(
        conn.clone(),
        insert_fungible_asset_balances_query,
        fungible_asset_balances,
        get_config_table_chunk_size::<FungibleAssetBalance>(
            "fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cfab = execute_in_chunks(
        conn.clone(),
        insert_current_fungible_asset_balances_query,
        current_fungible_asset_balances,
        get_config_table_chunk_size::<CurrentFungibleAssetBalance>(
            "current_fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cufab_v1 = execute_in_chunks(
        conn.clone(),
        insert_current_unified_fungible_asset_balances_v1_query,
        current_unified_fungible_asset_balances.0,
        get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
            "current_unified_fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cufab_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_unified_fungible_asset_balances_v2_query,
        current_unified_fungible_asset_balances.1,
        get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
            "current_unified_fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cs = execute_in_chunks(
        conn,
        insert_coin_supply_query,
        coin_supply,
        get_config_table_chunk_size::<CoinSupply>("coin_supply", per_table_chunk_sizes),
    );
    let (faa_res, fam_res, fab_res, cfab_res, cufab1_res, cufab2_res, cs_res) =
        tokio::join!(faa, fam, fab, cfab, cufab_v1, cufab_v2, cs);
    for res in [
        faa_res, fam_res, fab_res, cfab_res, cufab1_res, cufab2_res, cs_res,
    ] {
        res?;
    }

    Ok(())
}

fn insert_fungible_asset_activities_query(
    items_to_insert: Vec<FungibleAssetActivity>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::fungible_asset_activities::dsl::*;

    (
        diesel::insert_into(schema::fungible_asset_activities::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_nothing(),
        None,
    )
}

fn insert_fungible_asset_metadata_query(
    items_to_insert: Vec<FungibleAssetMetadataModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::fungible_asset_metadata::dsl::*;

    (
        diesel::insert_into(schema::fungible_asset_metadata::table)
            .values(items_to_insert)
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
                    is_token_v2.eq(excluded(is_token_v2)),
                    supply_v2.eq(excluded(supply_v2)),
                    maximum_v2.eq(excluded(maximum_v2)),
                )
            ),
        Some(" WHERE fungible_asset_metadata.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_fungible_asset_balances_query(
    items_to_insert: Vec<FungibleAssetBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::fungible_asset_balances::dsl::*;

    (
        diesel::insert_into(schema::fungible_asset_balances::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_fungible_asset_balances_query(
    items_to_insert: Vec<CurrentFungibleAssetBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_fungible_asset_balances::dsl::*;

    (
        diesel::insert_into(schema::current_fungible_asset_balances::table)
            .values(items_to_insert)
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
    )
}

fn insert_current_unified_fungible_asset_balances_v1_query(
    items_to_insert: Vec<CurrentUnifiedFungibleAssetBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_unified_fungible_asset_balances_to_be_renamed::dsl::*;

    (
        diesel::insert_into(schema::current_unified_fungible_asset_balances_to_be_renamed::table)
            .values(items_to_insert)
            .on_conflict(storage_id)
            .do_update()
            .set(
                (
                    owner_address.eq(excluded(owner_address)),
                    asset_type_v1.eq(excluded(asset_type_v1)),
                    is_frozen.eq(excluded(is_frozen)),
                    amount_v1.eq(excluded(amount_v1)),
                    last_transaction_timestamp_v1.eq(excluded(last_transaction_timestamp_v1)),
                    last_transaction_version_v1.eq(excluded(last_transaction_version_v1)),
                    inserted_at.eq(excluded(inserted_at)),
                )
            ),
        Some(" WHERE current_unified_fungible_asset_balances_to_be_renamed.last_transaction_version_v1 IS NULL \
        OR current_unified_fungible_asset_balances_to_be_renamed.last_transaction_version_v1 <= excluded.last_transaction_version_v1"),
    )
}

fn insert_current_unified_fungible_asset_balances_v2_query(
    items_to_insert: Vec<CurrentUnifiedFungibleAssetBalance>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_unified_fungible_asset_balances_to_be_renamed::dsl::*;
    (
        diesel::insert_into(schema::current_unified_fungible_asset_balances_to_be_renamed::table)
            .values(items_to_insert)
            .on_conflict(storage_id)
            .do_update()
            .set(
                (
                    owner_address.eq(excluded(owner_address)),
                    asset_type_v2.eq(excluded(asset_type_v2)),
                    is_primary.eq(excluded(is_primary)),
                    is_frozen.eq(excluded(is_frozen)),
                    amount_v2.eq(excluded(amount_v2)),
                    last_transaction_timestamp_v2.eq(excluded(last_transaction_timestamp_v2)),
                    last_transaction_version_v2.eq(excluded(last_transaction_version_v2)),
                    inserted_at.eq(excluded(inserted_at)),
                )
            ),
        Some(" WHERE current_unified_fungible_asset_balances_to_be_renamed.last_transaction_version_v2 IS NULL \
        OR current_unified_fungible_asset_balances_to_be_renamed.last_transaction_version_v2 <= excluded.last_transaction_version_v2 "),
    )
}

fn insert_coin_supply_query(
    items_to_insert: Vec<CoinSupply>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::coin_supply::dsl::*;

    (
        diesel::insert_into(schema::coin_supply::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, coin_type_hash))
            .do_nothing(),
        None,
    )
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
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (
            fungible_asset_activities,
            fungible_asset_metadata,
            mut fungible_asset_balances,
            mut current_fungible_asset_balances,
            current_unified_fungible_asset_balances,
            mut coin_supply,
        ) = parse_v2_coin(&transactions).await;

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let (coin_balance, fa_balance): (Vec<_>, Vec<_>) = current_unified_fungible_asset_balances
            .into_iter()
            .partition(|x| x.is_primary.is_none());

        if self
            .deprecated_tables
            .contains(TableFlags::FUNGIBLE_ASSET_BALANCES)
        {
            fungible_asset_balances.clear();
        }

        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_FUNGIBLE_ASSET_BALANCES)
        {
            current_fungible_asset_balances.clear();
        }

        if self.deprecated_tables.contains(TableFlags::COIN_SUPPLY) {
            coin_supply.clear();
        }

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &fungible_asset_activities,
            &fungible_asset_metadata,
            &fungible_asset_balances,
            &current_fungible_asset_balances,
            (&coin_balance, &fa_balance),
            &coin_supply,
            &self.per_table_chunk_sizes,
        )
        .await;
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version,
                    end_version,
                    processing_duration_in_secs,
                    db_insertion_duration_in_secs,
                    last_transaction_timestamp,
                },
            )),
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

/// V2 coin is called fungible assets and this flow includes all data from V1 in coin_processor
async fn parse_v2_coin(
    transactions: &[Transaction],
) -> (
    Vec<FungibleAssetActivity>,
    Vec<FungibleAssetMetadataModel>,
    Vec<FungibleAssetBalance>,
    Vec<CurrentFungibleAssetBalance>,
    Vec<CurrentUnifiedFungibleAssetBalance>,
    Vec<CoinSupply>,
) {
    let mut fungible_asset_activities = vec![];
    let mut fungible_asset_balances = vec![];
    let mut all_coin_supply = vec![];
    let mut current_fungible_asset_balances: CurrentFungibleAssetMapping = AHashMap::new();
    let mut fungible_asset_metadata: FungibleAssetMetadataMapping = AHashMap::new();

    // Get Metadata for fungible assets by object
    let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

    for txn in transactions {
        let txn_version = txn.version as i64;
        let block_height = txn.block_height as i64;
        let txn_data = match txn.txn_data.as_ref() {
            Some(data) => data,
            None => {
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["FungibleAssetProcessor"])
                    .inc();
                continue;
            },
        };
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        let txn_timestamp = txn
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        #[allow(deprecated)]
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");
        let txn_epoch = txn.epoch as i64;

        let default = vec![];
        let (events, user_request, entry_function_id_str) = match txn_data {
            TxnData::BlockMetadata(tx_inner) => (&tx_inner.events, None, None),
            TxnData::Validator(tx_inner) => (&tx_inner.events, None, None),
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
        let mut event_to_v1_coin_type: EventToCoinType = AHashMap::new();

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
                            ..ObjectAggregatedData::default()
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
                    if let Some(fungible_asset_supply) =
                        FungibleAssetSupply::from_write_resource(write_resource, txn_version)
                            .unwrap()
                    {
                        aggregated_data.fungible_asset_supply = Some(fungible_asset_supply);
                    }
                    if let Some(concurrent_fungible_asset_supply) =
                        ConcurrentFungibleAssetSupply::from_write_resource(
                            write_resource,
                            txn_version,
                        )
                        .unwrap()
                    {
                        aggregated_data.concurrent_fungible_asset_supply =
                            Some(concurrent_fungible_asset_supply);
                    }
                    if let Some(concurrent_fungible_asset_balance) =
                        ConcurrentFungibleAssetBalance::from_write_resource(
                            write_resource,
                            txn_version,
                        )
                        .unwrap()
                    {
                        aggregated_data.concurrent_fungible_asset_balance =
                            Some(concurrent_fungible_asset_balance);
                    }
                    if let Some(untransferable) =
                        Untransferable::from_write_resource(write_resource, txn_version).unwrap()
                    {
                        aggregated_data.untransferable = Some(untransferable);
                    }
                }
            } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap() {
                if let Some((balance, current_balance, event_to_coin)) =
                    FungibleAssetBalance::get_v1_from_delete_resource(
                        delete_resource,
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
            match wsc.change.as_ref().unwrap() {
                Change::WriteResource(write_resource) => {
                    if let Some(fa_metadata) =
                        FungibleAssetMetadataModel::get_v1_from_write_resource(
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
                        })
                    {
                        fungible_asset_metadata.insert(fa_metadata.asset_type.clone(), fa_metadata);
                    }
                    if let Some(fa_metadata) =
                        FungibleAssetMetadataModel::get_v2_from_write_resource(
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
                        })
                    {
                        fungible_asset_metadata.insert(fa_metadata.asset_type.clone(), fa_metadata);
                    }
                    if let Some((balance, curr_balance)) =
                        FungibleAssetBalance::get_v2_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                            &fungible_asset_object_helper,
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
                },
                Change::WriteTableItem(table_item) => {
                    if let Some(coin_supply) = CoinSupply::from_write_table_item(
                        table_item,
                        txn_version,
                        txn_timestamp,
                        txn_epoch,
                    )
                    .unwrap()
                    {
                        all_coin_supply.push(coin_supply);
                    }
                },
                _ => {},
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

    // Process the unified balance
    let current_unified_fungible_asset_balances = current_fungible_asset_balances
        .iter()
        .map(CurrentUnifiedFungibleAssetBalance::from)
        .collect::<Vec<_>>();
    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        current_fungible_asset_balances,
        current_unified_fungible_asset_balances,
        all_coin_supply,
    )
}
