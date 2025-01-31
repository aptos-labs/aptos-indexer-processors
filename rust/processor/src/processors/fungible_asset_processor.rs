// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::{
        common::models::{
            fungible_asset_models::{
                raw_v2_fungible_asset_activities::{
                    FungibleAssetActivityConvertible, RawFungibleAssetActivity,
                },
                raw_v2_fungible_asset_balances::{
                    CurrentUnifiedFungibleAssetBalanceConvertible,
                    RawCurrentUnifiedFungibleAssetBalance, RawFungibleAssetBalance,
                },
                raw_v2_fungible_asset_to_coin_mappings::{
                    FungibleAssetToCoinMappingConvertible, FungibleAssetToCoinMappings,
                    FungibleAssetToCoinMappingsForDB, RawFungibleAssetToCoinMapping,
                },
                raw_v2_fungible_metadata::{
                    FungibleAssetMetadataConvertible, FungibleAssetMetadataMapping,
                    RawFungibleAssetMetadataModel,
                },
            },
            object_models::v2_object_utils::{
                ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
            },
        },
        postgres::models::{
            coin_models::coin_supply::CoinSupply,
            fungible_asset_models::{
                v2_fungible_asset_activities::{EventToCoinType, FungibleAssetActivity},
                v2_fungible_asset_balances::{
                    CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
                },
                v2_fungible_asset_to_coin_mappings::FungibleAssetToCoinMapping,
                v2_fungible_asset_utils::FeeStatement,
                v2_fungible_metadata::FungibleAssetMetadataModel,
            },
            resources::{FromWriteResource, V2FungibleAssetResource},
        },
    },
    gap_detectors::ProcessingResult,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        table_flags::TableFlags,
        util::{get_entry_function_from_user_request, standardize_address},
    },
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{
    dsl::sql,
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    sql_types::{Nullable, Text},
    ExpressionMethods,
};
use rayon::prelude::*;
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
    (current_unified_fab_v1, current_unified_fab_v2): (
        &[CurrentUnifiedFungibleAssetBalance],
        &[CurrentUnifiedFungibleAssetBalance],
    ),
    coin_supply: &[CoinSupply],
    fungible_asset_to_coin_mapping: &[FungibleAssetToCoinMapping],
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
    let cufab_v1 = execute_in_chunks(
        conn.clone(),
        insert_current_unified_fungible_asset_balances_v1_query,
        current_unified_fab_v1,
        get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
            "current_unified_fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cufab_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_unified_fungible_asset_balances_v2_query,
        current_unified_fab_v2,
        get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
            "current_unified_fungible_asset_balances",
            per_table_chunk_sizes,
        ),
    );
    let cs = execute_in_chunks(
        conn.clone(),
        insert_coin_supply_query,
        coin_supply,
        get_config_table_chunk_size::<CoinSupply>("coin_supply", per_table_chunk_sizes),
    );
    let ctfm = execute_in_chunks(
        conn,
        insert_fungible_asset_to_coin_mappings_query,
        &fungible_asset_to_coin_mapping,
        get_config_table_chunk_size::<FungibleAssetToCoinMapping>(
            "fungible_asset_to_coin_mappings",
            per_table_chunk_sizes,
        ),
    );
    let (faa_res, fam_res, cufab1_res, cufab2_res, cs_res, ctfm_res) =
        tokio::join!(faa, fam, cufab_v1, cufab_v2, cs, ctfm);
    for res in [faa_res, fam_res, cufab1_res, cufab2_res, cs_res, ctfm_res] {
        res?;
    }

    Ok(())
}

pub fn insert_fungible_asset_activities_query(
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
            .do_update()
            .set(storage_id.eq(excluded(storage_id))),
        None,
    )
}

pub fn insert_fungible_asset_metadata_query(
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

pub fn insert_fungible_asset_balances_query(
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

pub fn insert_current_unified_fungible_asset_balances_v1_query(
    items_to_insert: Vec<CurrentUnifiedFungibleAssetBalance>,
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
                    asset_type_v1.eq(excluded(asset_type_v1)),
                    is_frozen.eq(excluded(is_frozen)),
                    amount_v1.eq(excluded(amount_v1)),
                    last_transaction_timestamp_v1.eq(excluded(last_transaction_timestamp_v1)),
                    last_transaction_version_v1.eq(excluded(last_transaction_version_v1)),
                    inserted_at.eq(excluded(inserted_at)),
                )
            ),
        Some(" WHERE current_fungible_asset_balances.last_transaction_version_v1 IS NULL \
        OR current_fungible_asset_balances.last_transaction_version_v1 <= excluded.last_transaction_version_v1"),
    )
}

pub fn insert_current_unified_fungible_asset_balances_v2_query(
    items_to_insert: Vec<CurrentUnifiedFungibleAssetBalance>,
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
                    // This guarantees that asset_type_v1 will not be overridden to null
                    asset_type_v1.eq(sql::<Nullable<Text>>("COALESCE(EXCLUDED.asset_type_v1, current_fungible_asset_balances.asset_type_v1)")),
                    asset_type_v2.eq(excluded(asset_type_v2)),
                    is_primary.eq(excluded(is_primary)),
                    is_frozen.eq(excluded(is_frozen)),
                    amount_v2.eq(excluded(amount_v2)),
                    last_transaction_timestamp_v2.eq(excluded(last_transaction_timestamp_v2)),
                    last_transaction_version_v2.eq(excluded(last_transaction_version_v2)),
                    inserted_at.eq(excluded(inserted_at)),
                )
            ),
        Some(" WHERE current_fungible_asset_balances.last_transaction_version_v2 IS NULL \
        OR current_fungible_asset_balances.last_transaction_version_v2 <= excluded.last_transaction_version_v2 "),
    )
}

pub fn insert_coin_supply_query(
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

pub fn insert_fungible_asset_to_coin_mappings_query(
    items_to_insert: Vec<FungibleAssetToCoinMapping>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::fungible_asset_to_coin_mappings::dsl::*;

    (
        diesel::insert_into(schema::fungible_asset_to_coin_mappings::table)
            .values(items_to_insert)
            .on_conflict(coin_type)
            .do_update()
            .set((
                fungible_asset_metadata_address.eq(excluded(fungible_asset_metadata_address)),
                last_transaction_version.eq(excluded(last_transaction_version)),
            )),
        Some(" WHERE fungible_asset_to_coin_mappings.last_transaction_version <= excluded.last_transaction_version "),
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
        let last_transaction_timestamp = transactions.last().unwrap().timestamp;

        let (
            raw_fungible_asset_activities,
            raw_fungible_asset_metadata,
            _raw_fungible_asset_balances,
            (raw_current_unified_fab_v1, raw_current_unified_fab_v2),
            mut coin_supply,
            fa_to_coin_mappings,
        ) = parse_v2_coin(&transactions, None).await;

        let postgres_fungible_asset_activities: Vec<FungibleAssetActivity> =
            raw_fungible_asset_activities
                .into_iter()
                .map(FungibleAssetActivity::from_raw)
                .collect();

        let postgres_fungible_asset_metadata: Vec<FungibleAssetMetadataModel> =
            raw_fungible_asset_metadata
                .into_iter()
                .map(FungibleAssetMetadataModel::from_raw)
                .collect();

        let mut postgres_current_unified_fab_v1: Vec<CurrentUnifiedFungibleAssetBalance> =
            raw_current_unified_fab_v1
                .into_iter()
                .map(CurrentUnifiedFungibleAssetBalance::from_raw)
                .collect();
        let mut postgres_current_unified_fab_v2: Vec<CurrentUnifiedFungibleAssetBalance> =
            raw_current_unified_fab_v2
                .into_iter()
                .map(CurrentUnifiedFungibleAssetBalance::from_raw)
                .collect();

        let postgres_fa_to_coin_mappings: Vec<FungibleAssetToCoinMapping> = fa_to_coin_mappings
            .into_iter()
            .map(FungibleAssetToCoinMapping::from_raw)
            .collect();

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        // if flag turned on we need to not include any value in the table
        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES)
        {
            postgres_current_unified_fab_v1.clear();
            postgres_current_unified_fab_v2.clear();
        }

        if self.deprecated_tables.contains(TableFlags::COIN_SUPPLY) {
            coin_supply.clear();
        }

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &postgres_fungible_asset_activities,
            &postgres_fungible_asset_metadata,
            (
                &postgres_current_unified_fab_v1,
                &postgres_current_unified_fab_v2,
            ),
            &coin_supply,
            &postgres_fa_to_coin_mappings,
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

/// Gets coin to fungible asset mappings from transactions by looking at CoinInfo
/// This is very similar code to part of parse_v2_coin
pub async fn get_fa_to_coin_mapping(transactions: &[Transaction]) -> FungibleAssetToCoinMappings {
    // First collect all metadata from transactions
    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut kv_mapping: FungibleAssetToCoinMappings = AHashMap::new();

            let txn_version = txn.version as i64;
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(fa_metadata) =
                        RawFungibleAssetMetadataModel::get_v1_from_write_resource(
                            wr,
                            index as i64,
                            txn_version,
                            NaiveDateTime::default(), // placeholder
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
                        let fa_to_coin_mapping =
                            RawFungibleAssetToCoinMapping::from_raw_fungible_asset_metadata(
                                &fa_metadata,
                            );
                        kv_mapping.insert(
                            fa_to_coin_mapping.fungible_asset_metadata_address.clone(),
                            fa_to_coin_mapping.coin_type.clone(),
                        );
                    }
                }
            }
            kv_mapping
        })
        .collect();
    let mut kv_mapping: FungibleAssetToCoinMappings = AHashMap::new();
    for mapping in data {
        kv_mapping.extend(mapping);
    }
    kv_mapping
}

/// TODO: After the migration is complete, we can move this to common models folder
/// V2 coin is called fungible assets and this flow includes all data from V1 in coin_processor
pub async fn parse_v2_coin(
    transactions: &[Transaction],
    // This mapping is only applied to SDK processor. The old processor will use the hardcoded mapping
    // METADATA_TO_COIN_TYPE_MAPPING
    persisted_fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
) -> (
    Vec<RawFungibleAssetActivity>,
    Vec<RawFungibleAssetMetadataModel>,
    Vec<RawFungibleAssetBalance>,
    (
        Vec<RawCurrentUnifiedFungibleAssetBalance>,
        Vec<RawCurrentUnifiedFungibleAssetBalance>,
    ),
    Vec<CoinSupply>,
    Vec<RawFungibleAssetToCoinMapping>,
) {
    let mut fungible_asset_activities: Vec<RawFungibleAssetActivity> = vec![];
    let mut fungible_asset_balances: Vec<RawFungibleAssetBalance> = vec![];
    let mut all_coin_supply: Vec<CoinSupply> = vec![];
    let mut fungible_asset_metadata: FungibleAssetMetadataMapping = AHashMap::new();
    let mut coin_to_fa_mappings: FungibleAssetToCoinMappingsForDB = AHashMap::new();

    let data: Vec<_> = transactions
        .par_iter()
        .map(|txn| {
            let mut fungible_asset_activities = vec![];
            let mut fungible_asset_metadata = AHashMap::new();
            let mut fungible_asset_balances = vec![];
            let mut all_coin_supply = vec![];
            let mut coin_to_fa_mappings: FungibleAssetToCoinMappingsForDB = AHashMap::new();

            // Get Metadata for fungible assets by object address
            let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            if txn.txn_data.is_none() {
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["FungibleAssetProcessor"])
                    .inc();
                return (
                    fungible_asset_activities,
                    fungible_asset_metadata,
                    fungible_asset_balances,
                    all_coin_supply,
                    coin_to_fa_mappings,
                );
            }
            let txn_data = txn.txn_data.as_ref().unwrap();
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!")
                .seconds;
            #[allow(deprecated)]
            let txn_timestamp = NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
                .expect("Txn Timestamp is invalid!");
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
            // When coinstore is deleted we have no way of getting the mapping but hoping that there is
            // only 1 coinstore deletion by owner address. This is a mapping between owner address and deleted coin type
            // This is not ideal as we're assuming that there is only 1 coinstore deletion by owner address, this should be
            // replaced by an event (although we still need to keep this mapping because blockchain)
            let mut address_to_deleted_coin_type: AHashMap<String, String> = AHashMap::new();
            // Loop 1: to get all object addresses
            // Need to do a first pass to get all the object addresses and insert them into the helper
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
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
            // Loop 2: Get the metadata relevant to parse v1 coin and v2 fungible asset.
            // As an optimization, we also handle v1 balances in the process
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                    if let Some((balance, event_to_coin)) =
                        RawFungibleAssetBalance::get_v1_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        fungible_asset_balances.push(balance);
                        event_to_v1_coin_type.extend(event_to_coin);
                    }
                    // Fill the v2 fungible_asset_object_helper. This is used to track which objects exist at each object address.
                    // The data will be used to reconstruct the full data in Loop 4.
                    let address = standardize_address(&write_resource.address.to_string());
                    if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                        if let Some(v2_fungible_asset_resource) =
                            V2FungibleAssetResource::from_write_resource(write_resource).unwrap()
                        {
                            match v2_fungible_asset_resource {
                                V2FungibleAssetResource::FungibleAssetMetadata(
                                    fungible_asset_metadata,
                                ) => {
                                    aggregated_data.fungible_asset_metadata =
                                        Some(fungible_asset_metadata);
                                },
                                V2FungibleAssetResource::FungibleAssetStore(
                                    fungible_asset_store,
                                ) => {
                                    aggregated_data.fungible_asset_store =
                                        Some(fungible_asset_store);
                                },
                                V2FungibleAssetResource::FungibleAssetSupply(
                                    fungible_asset_supply,
                                ) => {
                                    aggregated_data.fungible_asset_supply =
                                        Some(fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetSupply(
                                    concurrent_fungible_asset_supply,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_supply =
                                        Some(concurrent_fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetBalance(
                                    concurrent_fungible_asset_balance,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_balance =
                                        Some(concurrent_fungible_asset_balance);
                                },
                            }
                        }
                    }
                } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap()
                {
                    if let Some((balance, single_deleted_coin_type)) =
                        RawFungibleAssetBalance::get_v1_from_delete_resource(
                            delete_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        fungible_asset_balances.push(balance);
                        address_to_deleted_coin_type.extend(single_deleted_coin_type);
                    }
                }
            }

            // The artificial gas event, only need for v1
            if let Some(req) = user_request {
                let fee_statement = events.iter().find_map(|event| {
                    let event_type = event.type_str.as_str();
                    FeeStatement::from_event(event_type, &event.data, txn_version)
                });
                let gas_event = RawFungibleAssetActivity::get_gas_event(
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

            // Loop 3 to handle events and collect additional metadata from events for v2
            for (index, event) in events.iter().enumerate() {
                if let Some(v1_activity) = RawFungibleAssetActivity::get_v1_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    &entry_function_id_str,
                    &event_to_v1_coin_type,
                    index as i64,
                    &address_to_deleted_coin_type,
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
                if let Some(v2_activity) = RawFungibleAssetActivity::get_v2_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &fungible_asset_object_helper,
                )
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

            // Loop 4 to handle write set changes for metadata, balance, and v1 supply
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(write_resource) => {
                        if let Some(fa_metadata) =
                            RawFungibleAssetMetadataModel::get_v1_from_write_resource(
                                write_resource,
                                index as i64,
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
                            let asset_type = fa_metadata.asset_type.clone();
                            fungible_asset_metadata.insert(asset_type.clone(), fa_metadata.clone());
                            let coin_to_fa_mapping =
                                RawFungibleAssetToCoinMapping::from_raw_fungible_asset_metadata(
                                    &fa_metadata,
                                );
                            coin_to_fa_mappings.insert(asset_type, coin_to_fa_mapping);
                        }
                        if let Some(fa_metadata) =
                            RawFungibleAssetMetadataModel::get_v2_from_write_resource(
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
                            fungible_asset_metadata
                                .insert(fa_metadata.asset_type.clone(), fa_metadata);
                        }
                        if let Some(balance) = RawFungibleAssetBalance::get_v2_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                            &fungible_asset_object_helper,
                        )
                        .unwrap_or_else(|e| {
                            tracing::error!(
                                    transaction_version = txn_version,
                                    index = index,
                                    error = ?e,
                                    "[Parser] error parsing fungible balance v2");
                            panic!("[Parser] error parsing fungible balance v2");
                        }) {
                            fungible_asset_balances.push(balance);
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
            (
                fungible_asset_activities,
                fungible_asset_metadata,
                fungible_asset_balances,
                all_coin_supply,
                coin_to_fa_mappings,
            )
        })
        .collect();

    for (faa, fam, fab, acs, ctfm) in data {
        fungible_asset_activities.extend(faa);
        fungible_asset_balances.extend(fab);
        all_coin_supply.extend(acs);
        fungible_asset_metadata.extend(fam);
        coin_to_fa_mappings.extend(ctfm);
    }

    // Now we need to convert fab into current_unified_fungible_asset_balances v1 and v2
    let (current_unified_fab_v1, current_unified_fab_v2) =
        RawCurrentUnifiedFungibleAssetBalance::from_fungible_asset_balances(
            &fungible_asset_balances,
            persisted_fa_to_coin_mapping,
        );

    // Boilerplate after this
    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut fungible_asset_metadata = fungible_asset_metadata
        .into_values()
        .collect::<Vec<RawFungibleAssetMetadataModel>>();
    let mut current_unified_fab_v1 = current_unified_fab_v1
        .into_values()
        .collect::<Vec<RawCurrentUnifiedFungibleAssetBalance>>();
    let mut current_unified_fab_v2 = current_unified_fab_v2
        .into_values()
        .collect::<Vec<RawCurrentUnifiedFungibleAssetBalance>>();
    let mut coin_to_fa_mappings = coin_to_fa_mappings
        .into_values()
        .collect::<Vec<RawFungibleAssetToCoinMapping>>();

    // Sort by PK
    fungible_asset_metadata.sort_by(|a, b| a.asset_type.cmp(&b.asset_type));
    current_unified_fab_v1.sort_by(|a, b| a.storage_id.cmp(&b.storage_id));
    current_unified_fab_v2.sort_by(|a, b| a.storage_id.cmp(&b.storage_id));
    coin_to_fa_mappings.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        (current_unified_fab_v1, current_unified_fab_v2),
        all_coin_supply,
        coin_to_fa_mappings,
    )
}
