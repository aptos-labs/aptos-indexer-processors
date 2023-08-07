// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::{
        coin_models::{
            account_transactions::AccountTransaction,
            coin_activities::CoinActivity,
            coin_balances::{CoinBalance, CurrentCoinBalance},
            coin_infos::CoinInfo,
            coin_supply::CoinSupply,
            v2_fungible_asset_activities::{
                CurrentCoinBalancePK, EventToCoinType, FungibleAssetActivity,
            },
            v2_fungible_asset_balances::{
                CurrentFungibleAssetBalance, CurrentFungibleAssetMapping, FungibleAssetBalance,
            },
            v2_fungible_asset_utils::{
                FungibleAssetAggregatedData, FungibleAssetAggregatedDataMapping,
                FungibleAssetMetadata, FungibleAssetStore,
            },
            v2_fungible_metadata::{FungibleAssetMetadataMapping, FungibleAssetMetadataModel},
        },
        token_models::v2_token_utils::{ObjectWithMetadata, TokenV2},
    },
    schema,
    utils::{
        database::{
            clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
        },
        util::{get_entry_function_from_user_request, standardize_address},
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change, Transaction,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;

pub const NAME: &str = "coin_processor";
pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";
pub struct CoinTransactionProcessor {
    connection_pool: PgDbPool,
}

impl CoinTransactionProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for CoinTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "CoinTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db_impl(
    conn: &mut PgConnection,
    coin_activities: &[CoinActivity],
    coin_infos: &[CoinInfo],
    coin_balances: &[CoinBalance],
    current_coin_balances: &[CurrentCoinBalance],
    coin_supply: &[CoinSupply],
    account_transactions: &[AccountTransaction],
    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        current_fungible_asset_balances,
    ): (
        &[FungibleAssetActivity],
        &[FungibleAssetMetadataModel],
        &[FungibleAssetBalance],
        &[CurrentFungibleAssetBalance],
    ),
) -> Result<(), diesel::result::Error> {
    insert_coin_activities(conn, coin_activities)?;
    insert_coin_infos(conn, coin_infos)?;
    insert_coin_balances(conn, coin_balances)?;
    insert_current_coin_balances(conn, current_coin_balances)?;
    insert_coin_supply(conn, coin_supply)?;
    insert_account_transactions(conn, account_transactions)?;
    insert_fungible_asset_activities(conn, fungible_asset_activities)?;
    insert_fungible_asset_metadata(conn, fungible_asset_metadata)?;
    insert_fungible_asset_balances(conn, fungible_asset_balances)?;
    insert_current_fungible_asset_balances(conn, current_fungible_asset_balances)?;
    Ok(())
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    coin_activities: Vec<CoinActivity>,
    coin_infos: Vec<CoinInfo>,
    coin_balances: Vec<CoinBalance>,
    current_coin_balances: Vec<CurrentCoinBalance>,
    coin_supply: Vec<CoinSupply>,
    account_transactions: Vec<AccountTransaction>,
    (
        fungible_asset_activities,
        fungible_asset_metadata,
        fungible_asset_balances,
        current_fungible_asset_balances,
    ): (
        Vec<FungibleAssetActivity>,
        Vec<FungibleAssetMetadataModel>,
        Vec<FungibleAssetBalance>,
        Vec<CurrentFungibleAssetBalance>,
    ),
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
            insert_to_db_impl(
                pg_conn,
                &coin_activities,
                &coin_infos,
                &coin_balances,
                &current_coin_balances,
                &coin_supply,
                &account_transactions,
                (
                    &fungible_asset_activities,
                    &fungible_asset_metadata,
                    &fungible_asset_balances,
                    &current_fungible_asset_balances,
                ),
            )
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let coin_activities = clean_data_for_db(coin_activities, true);
                let coin_infos = clean_data_for_db(coin_infos, true);
                let coin_balances = clean_data_for_db(coin_balances, true);
                let current_coin_balances = clean_data_for_db(current_coin_balances, true);
                let coin_supply = clean_data_for_db(coin_supply, true);
                let account_transactions = clean_data_for_db(account_transactions, true);
                let fungible_asset_activities = clean_data_for_db(fungible_asset_activities, true);
                let fungible_asset_metadata = clean_data_for_db(fungible_asset_metadata, true);
                let fungible_asset_balances = clean_data_for_db(fungible_asset_balances, true);
                let current_fungible_asset_balances =
                    clean_data_for_db(current_fungible_asset_balances, true);

                insert_to_db_impl(
                    pg_conn,
                    &coin_activities,
                    &coin_infos,
                    &coin_balances,
                    &current_coin_balances,
                    &coin_supply,
                    &account_transactions,
                    (
                        &fungible_asset_activities,
                        &fungible_asset_metadata,
                        &fungible_asset_balances,
                        &current_fungible_asset_balances,
                    ),
                )
            }),
    }
}

fn insert_coin_activities(
    conn: &mut PgConnection,
    item_to_insert: &[CoinActivity],
) -> Result<(), diesel::result::Error> {
    use schema::coin_activities::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinActivity::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_activities::table)
                .values(&item_to_insert[start_ind..end_ind])
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
        )?;
    }
    Ok(())
}

fn insert_coin_infos(
    conn: &mut PgConnection,
    item_to_insert: &[CoinInfo],
) -> Result<(), diesel::result::Error> {
    use schema::coin_infos::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinInfo::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_infos::table)
                .values(&item_to_insert[start_ind..end_ind])
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
        )?;
    }
    Ok(())
}

fn insert_coin_balances(
    conn: &mut PgConnection,
    item_to_insert: &[CoinBalance],
) -> Result<(), diesel::result::Error> {
    use schema::coin_balances::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinBalance::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, owner_address, coin_type_hash))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_coin_balances(
    conn: &mut PgConnection,
    item_to_insert: &[CurrentCoinBalance],
) -> Result<(), diesel::result::Error> {
    use schema::current_coin_balances::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentCoinBalance::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_coin_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((owner_address, coin_type_hash))
                .do_update()
                .set((
                    amount.eq(excluded(amount)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                    inserted_at.eq(excluded(inserted_at)),
                )),
                Some(" WHERE current_coin_balances.last_transaction_version <= excluded.last_transaction_version "),
            )?;
    }
    Ok(())
}

fn insert_coin_supply(
    conn: &mut PgConnection,
    item_to_insert: &[CoinSupply],
) -> Result<(), diesel::result::Error> {
    use schema::coin_supply::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinSupply::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_supply::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, coin_type_hash))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_account_transactions(
    conn: &mut PgConnection,
    item_to_insert: &[AccountTransaction],
) -> Result<(), diesel::result::Error> {
    use schema::account_transactions::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), AccountTransaction::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::account_transactions::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, account_address))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_fungible_asset_activities(
    conn: &mut PgConnection,
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
        )?;
    }
    Ok(())
}

fn insert_fungible_asset_metadata(
    conn: &mut PgConnection,
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
        )?;
    }
    Ok(())
}

fn insert_fungible_asset_balances(
    conn: &mut PgConnection,
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
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_fungible_asset_balances(
    conn: &mut PgConnection,
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
        )?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for CoinTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn();

        let mut all_coin_activities = vec![];
        let mut all_coin_balances = vec![];
        let mut all_coin_infos: HashMap<String, CoinInfo> = HashMap::new();
        let mut all_current_coin_balances: HashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
            HashMap::new();
        let mut all_coin_supply = vec![];

        let mut account_transactions = HashMap::new();

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

            account_transactions.extend(AccountTransaction::from_transaction(txn));
        }
        let mut all_coin_infos = all_coin_infos.into_values().collect::<Vec<CoinInfo>>();
        let mut all_current_coin_balances = all_current_coin_balances
            .into_values()
            .collect::<Vec<CurrentCoinBalance>>();
        let mut account_transactions = account_transactions
            .into_values()
            .collect::<Vec<AccountTransaction>>();

        // Sort by PK
        all_coin_infos.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
        all_current_coin_balances.sort_by(|a, b| {
            (&a.owner_address, &a.coin_type).cmp(&(&b.owner_address, &b.coin_type))
        });
        account_transactions.sort_by(|a, b| {
            (&a.transaction_version, &a.account_address)
                .cmp(&(&b.transaction_version, &b.account_address))
        });

        let (
            fungible_asset_activities,
            fungible_asset_metadata,
            fungible_asset_balances,
            current_fungible_asset_balances,
        ) = parse_v2_coin(&transactions, &mut conn);

        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            all_coin_activities,
            all_coin_infos,
            all_coin_balances,
            all_current_coin_balances,
            all_coin_supply,
            account_transactions,
            (
                fungible_asset_activities,
                fungible_asset_metadata,
                fungible_asset_balances,
                current_fungible_asset_balances,
            ),
        );
        match tx_result {
            Ok(_) => Ok((start_version, end_version)),
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

/// V2 coin is called fungible assets and this flow should replace the v1 flow above.
/// V2 includes all data from V1.
fn parse_v2_coin(
    transactions: &[Transaction],
    conn: &mut PgPoolConnection,
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
    let mut fungible_asset_object_helper: FungibleAssetAggregatedDataMapping = HashMap::new();

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
                        FungibleAssetAggregatedData {
                            object,
                            fungible_asset_metadata: None,
                            fungible_asset_store: None,
                            token: None,
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

        // Loop to handle events and collect additional metadata from events for v2
        for (index, event) in events.iter().enumerate() {
            // The artificial gas event, only need for v1
            if let Some(req) = user_request {
                let gas_event = FungibleAssetActivity::get_gas_event(
                    transaction_info,
                    req,
                    &entry_function_id_str,
                    txn_version,
                    txn_timestamp,
                    block_height,
                );
                fungible_asset_activities.push(gas_event);
            }
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
