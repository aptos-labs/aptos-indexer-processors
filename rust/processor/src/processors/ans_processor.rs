// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::utils::util::{parse_timestamp, standardize_address};
use crate::{
    models::ans_models::ans_lookup::{
        AddressToPrimaryNameRecord, AnsLookup, AnsLookupPK, CurrentAnsLookup,
        CurrentAnsLookupDataQuery, CurrentAnsLookupPK, NameRecordKeyV1, NameRecordV1,
    },
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::Transaction;
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, DeleteTableItem,
    WriteTableItem,
};
use async_trait::async_trait;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection};
use field_count::FieldCount;
use std::fmt::Debug;
use std::{cmp, collections::HashMap};
use tracing::error;

pub const NAME: &str = "ans_processor";
pub struct AnsTransactionProcessor {
    connection_pool: PgDbPool,
    ans_primary_names_table_handle: Option<String>,
    ans_name_records_table_handle: Option<String>,
}

impl AnsTransactionProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        ans_primary_names_table_handle: Option<String>,
        ans_name_records_table_handle: Option<String>,
    ) -> Self {
        tracing::info!(
            ans_primary_names_table_handle = ans_primary_names_table_handle,
            ans_name_records_table_handle = ans_name_records_table_handle,
            "init AnsProcessor"
        );
        Self {
            connection_pool,
            ans_primary_names_table_handle,
            ans_name_records_table_handle,
        }
    }
}

impl Debug for AnsTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AnsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for AnsTransactionProcessor {
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

        let (all_current_ans_lookups, all_ans_lookups) = parse_ans(
            &transactions,
            self.ans_name_records_table_handle.clone(),
            self.ans_primary_names_table_handle.clone(),
            &mut conn,
        );

        // Sort ans lookup values for postgres insert
        let mut all_current_ans_lookups = all_current_ans_lookups
            .into_values()
            .collect::<Vec<CurrentAnsLookup>>();
        let mut all_ans_lookups = all_ans_lookups.into_values().collect::<Vec<AnsLookup>>();
        all_current_ans_lookups.sort_by(|a: &CurrentAnsLookup, b| {
            a.domain.cmp(&b.domain).then(a.subdomain.cmp(&b.subdomain))
        });
        all_ans_lookups.sort_by(|a: &AnsLookup, b| {
            (&a.transaction_version, &a.domain, &a.subdomain).cmp(&(
                &b.transaction_version,
                &b.domain,
                &b.subdomain,
            ))
        });

        // Insert values to db
        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            all_current_ans_lookups,
            all_ans_lookups,
        );

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

fn parse_ans(
    transactions: &[Transaction],
    maybe_ans_primary_names_table_handle: Option<String>,
    maybe_ans_name_records_table_handle: Option<String>,
    conn: &mut PgPoolConnection,
) -> (
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    HashMap<AnsLookupPK, AnsLookup>,
) {
    let mut all_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> = HashMap::new();
    let mut all_ans_lookups: HashMap<AnsLookupPK, AnsLookup> = HashMap::new();

    // Map to track all the ans records in this transaction. This is used to lookup a previous ANS record
    // that is in the same transaction batch.
    let mut address_to_primary_name_records: AddressToPrimaryNameRecord = HashMap::new();

    for transaction in transactions {
        // ANS lookups
        let mut txn_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> =
            HashMap::new();
        let mut txn_ans_lookups: HashMap<AnsLookupPK, AnsLookup> = HashMap::new();

        let txn_version = transaction.version as i64;
        let txn_timestamp = parse_timestamp(transaction.timestamp.as_ref().unwrap(), txn_version);
        let txn_data = transaction
            .txn_data
            .as_ref()
            .expect("Txn Data doesn't exit!");

        // Extracts from user transactions. Other transactions won't have any ANS changes
        if let (
            TxnData::User(_),
            Some(ans_primary_names_table_handle),
            Some(ans_name_records_table_handle),
        ) = (
            txn_data,
            maybe_ans_primary_names_table_handle.clone(),
            maybe_ans_name_records_table_handle.clone(),
        ) {
            let transaction_info = transaction
                .info
                .as_ref()
                .expect("Transaction info doesn't exist!");

            // Extract name record changes
            for wsc in &transaction_info.changes {
                let res = match wsc.change.as_ref().unwrap() {
                    // TODO: Add ANS v2 indexing
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        from_name_record_table_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            txn_timestamp,
                            &txn_current_ans_lookups,
                            conn,
                        )
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        from_name_record_table_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                            txn_timestamp,
                        )
                    },
                    _ => continue,
                };
                match res {
                    Ok((current_ans_lookups, ans_lookups)) => {
                        txn_current_ans_lookups.extend(current_ans_lookups);
                        txn_ans_lookups.extend(ans_lookups);
                    },
                    Err(e) => {
                        tracing::error!("Failed to parse name record table item. Error: {:?}", e);
                        continue;
                    },
                }
            }

            // Extract primary name reverse lookup changes
            for wsc in &transaction_info.changes {
                let res = match wsc.change.as_ref().unwrap() {
                    // TODO: Add ANS v2 indexing
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        from_primary_name_record_table_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            txn_timestamp,
                            &address_to_primary_name_records,
                            conn,
                        )
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        from_primary_name_record_table_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                            txn_timestamp,
                            &address_to_primary_name_records,
                            conn,
                        )
                    },
                    _ => continue,
                };

                match res {
                    Ok((current_ans_lookups, ans_lookups)) => {
                        for (record_key, name_record) in current_ans_lookups {
                            // Name record may already exist in current_ans_lookup, so need to upsert here
                            txn_current_ans_lookups
                                .entry(record_key.clone())
                                .and_modify(|e| {
                                    e.registered_address = name_record.registered_address.clone();
                                    // name_record.expiration_timestamp may be zero if primary name wsc occurs before name record wsc,
                                    // so return the larger of the two timestamps
                                    e.expiration_timestamp = cmp::max(
                                        name_record.expiration_timestamp,
                                        e.expiration_timestamp,
                                    );
                                    // name_record.is_primary may be false here, so use OR to preserve is_primary status
                                    e.is_primary = name_record.is_primary || e.is_primary;
                                    e.is_deleted = name_record.is_deleted;
                                })
                                .or_insert(name_record);
                        }
                        for (record_key, name_record) in ans_lookups {
                            txn_ans_lookups
                                .entry(record_key.clone())
                                .and_modify(|e| {
                                    e.registered_address = name_record.registered_address.clone();
                                    // name_record.expiration_timestamp may be zero if primary name wsc occurs before name record wsc,
                                    // so return the larger of the two timestamps
                                    e.expiration_timestamp = cmp::max(
                                        name_record.expiration_timestamp,
                                        e.expiration_timestamp,
                                    );
                                    // name_record.is_primary may be false here, so use OR to preserve is_primary status
                                    e.is_primary = name_record.is_primary || e.is_primary;
                                    e.is_deleted = name_record.is_deleted;
                                })
                                .or_insert(name_record);
                        }
                    },
                    Err(e) => {
                        tracing::error!(
                            "Failed to parse primary name reverse lookup table item. Error: {:?}",
                            e
                        );
                        continue;
                    },
                }
            }
        }
        all_current_ans_lookups.extend(txn_current_ans_lookups);
        all_ans_lookups.extend(txn_ans_lookups);

        // Create address_to_primary_name_records from all_current_ans_lookups. We have to recreate this every time because
        // registered_address may be None in the current_ans_lookups, so we don't know which value to delete from the map.
        address_to_primary_name_records = all_current_ans_lookups
            .iter()
            .filter_map(|(_, v)| {
                if v.registered_address.is_some() {
                    Some((v.registered_address.clone().unwrap(), v.clone()))
                } else {
                    None
                }
            })
            .collect();
    }
    (all_current_ans_lookups, all_ans_lookups)
}

// Parse the primary name reverse record from write table item.
// The table key is the target address the primary name points to.
// The table value data has the domain and subdomain of the primary name.
fn from_primary_name_record_table_write_table_item_v1(
    write_table_item: &WriteTableItem,
    txn_version: i64,
    txn_timestamp: chrono::NaiveDateTime,
    address_to_primary_name_record: &AddressToPrimaryNameRecord,
    conn: &mut PgPoolConnection,
) -> anyhow::Result<(
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    HashMap<AnsLookupPK, AnsLookup>,
)> {
    let mut txn_current_ans_lookups = HashMap::new();
    let mut txn_ans_lookups = HashMap::new();

    let table_item_data = write_table_item.data.as_ref().unwrap();
    let key_type: &str = table_item_data.key_type.as_str();
    let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
        tracing::error!(
            transaction_version = txn_version,
            data = table_item_data.key,
            "Failed to parse address from ANS reverse lookup table item",
        );
        panic!();
    });
    let value_type: &str = table_item_data.value_type.as_str();
    let value = &table_item_data.value;
    let primary_name_record: NameRecordKeyV1 = serde_json::from_str(value).unwrap_or_else(|e| {
        tracing::error!(
            transaction_version = txn_version,
            data = value,
            "Failed to parse NameRecordKeyV1 from ANS reverse lookup table item",
        );
        panic!();
    });

    // Case: when a primary name points to a new address, we need to set the previous primary name record's to non-primary.
    // Lookup the previous primary name record by target address. It may not exist.
    if let Some(mut previous_primary_record) =
        lookup_name_record_by_primary_address(address_to_primary_name_record, key, conn)
    {
        // Update the old primary name record
        previous_primary_record.is_primary = false;
        previous_primary_record.last_transaction_version = txn_version;
        txn_current_ans_lookups.insert(
            (
                previous_primary_record.domain.clone(),
                previous_primary_record.subdomain.clone(),
            ),
            previous_primary_record.clone(),
        );
        txn_ans_lookups.insert(
            (
                txn_version,
                previous_primary_record.domain.clone(),
                previous_primary_record.subdomain.clone(),
            ),
            AnsLookup {
                transaction_version: txn_version,
                domain: previous_primary_record.domain,
                subdomain: previous_primary_record.subdomain,
                registered_address: previous_primary_record.registered_address,
                expiration_timestamp: previous_primary_record.expiration_timestamp,
                token_name: previous_primary_record.token_name,
                is_primary: false,
                is_deleted: previous_primary_record.is_deleted,
                transaction_timestamp: txn_timestamp,
            },
        );
    }

    // If the name record doesn't exist yet, create it and insert it to current_ans_lookups.
    // This is possible if the primary name record wsc happens before the name record wsc.
    txn_current_ans_lookups.insert(
        (
            primary_name_record.get_domain().clone(),
            primary_name_record.get_subdomain().clone(),
        ),
        CurrentAnsLookup {
            domain: primary_name_record.get_domain().clone(),
            subdomain: primary_name_record.get_subdomain().clone(),
            registered_address: Some(String::from(key)),
            last_transaction_version: txn_version,
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: primary_name_record.get_token_name(),
            is_primary: true,
            is_deleted: false,
        },
    );
    txn_ans_lookups.insert(
        (
            txn_version,
            primary_name_record.get_domain().clone(),
            primary_name_record.get_subdomain().clone(),
        ),
        AnsLookup {
            transaction_version: txn_version,
            domain: primary_name_record.get_domain(),
            subdomain: primary_name_record.get_subdomain(),
            registered_address: Some(String::from(key)),
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: primary_name_record.get_token_name(),
            is_primary: true,
            is_deleted: false,
            transaction_timestamp: txn_timestamp,
        },
    );

    Ok((txn_current_ans_lookups, txn_ans_lookups))
}

pub fn from_primary_name_record_table_delete_table_item_v1(
    delete_table_item: &DeleteTableItem,
    txn_version: i64,
    txn_timestamp: chrono::NaiveDateTime,
    address_to_primary_name_record: &AddressToPrimaryNameRecord,
    conn: &mut PgPoolConnection,
) -> anyhow::Result<(
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    HashMap<AnsLookupPK, AnsLookup>,
)> {
    let mut txn_current_ans_lookups = HashMap::new();
    let mut txn_ans_lookups = HashMap::new();

    let table_item_data = delete_table_item.data.as_ref().unwrap();
    let key_type = table_item_data.key_type.as_str();
    let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
        panic!(
            "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
            txn_version, key_type, table_item_data.key, e
        )
    });
    let registered_address = standardize_address(key);

    // Need to lookup previous name record to update the row's is_primary = false
    let mut previous_name_record = match lookup_name_record_by_primary_address(
        address_to_primary_name_record,
        &registered_address,
        conn,
    ) {
        Some(ans_record) => ans_record,
        None => {
            tracing::error!(
                transaction_version = txn_version,
                lookup_key = &registered_address,
                "Failed to get ans record for registered address. You probably should backfill db."
            );
            return Ok((HashMap::new(), HashMap::new()));
        },
    };
    // Update the lookup's name record with the new data and return
    previous_name_record.is_primary = false;
    previous_name_record.last_transaction_version = txn_version;
    txn_current_ans_lookups.insert(
        (
            previous_name_record.domain.clone(),
            previous_name_record.subdomain.clone(),
        ),
        previous_name_record.clone(),
    );
    txn_ans_lookups.insert(
        (
            txn_version,
            previous_name_record.domain.clone(),
            previous_name_record.subdomain.clone(),
        ),
        AnsLookup {
            transaction_version: txn_version,
            domain: previous_name_record.domain,
            subdomain: previous_name_record.subdomain,
            registered_address: previous_name_record.registered_address,
            expiration_timestamp: previous_name_record.expiration_timestamp,
            token_name: previous_name_record.token_name,
            is_primary: false,
            is_deleted: previous_name_record.is_deleted,
            transaction_timestamp: txn_timestamp,
        },
    );

    Ok((txn_current_ans_lookups, txn_ans_lookups))
}

pub fn from_name_record_table_write_table_item_v1(
    write_table_item: &WriteTableItem,
    txn_version: i64,
    txn_timestamp: chrono::NaiveDateTime,
    current_ans_lookups: &HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    conn: &mut PgPoolConnection,
) -> anyhow::Result<(
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    HashMap<AnsLookupPK, AnsLookup>,
)> {
    let mut txn_current_ans_lookups = HashMap::new();
    let mut txn_ans_lookups = HashMap::new();

    // Parse name record.
    // The table key has the domain and subdomain.
    // The table value data has the metadata (expiration, property version, target address).
    let table_item_data = write_table_item.data.as_ref().unwrap();
    let key_type = table_item_data.key_type.as_str();
    let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
        .unwrap_or_else(|e| {
            panic!(
                "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                txn_version, key_type, table_item_data.key, e
            )
        });
    let value_type = table_item_data.value_type.as_str();
    let name_record: NameRecordV1 =
        serde_json::from_str(&table_item_data.value).unwrap_or_else(|e| {
            panic!(
                "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                txn_version, value_type, table_item_data.value, e
            )
        });

    // Need to lookup the previous domain to persist is_primary flag:
    // 1. bob.apt -> is_primary = true
    // 2. Transaction updates bob.apt's metadata (like renewing the domain)
    // 3. is_primary should be the same in the inserted row
    if let Some(mut previous_name_record) = lookup_name_record_by_domain_and_subdomain(
        &name_record_key.get_domain(),
        &name_record_key.get_subdomain(),
        current_ans_lookups,
        conn,
    ) {
        // If domain exists, modify the existing DB item
        previous_name_record.expiration_timestamp = name_record.get_expiration_time();
        previous_name_record.registered_address = name_record.get_target_address();
        previous_name_record.last_transaction_version = txn_version;
        txn_current_ans_lookups.insert(
            (
                previous_name_record.domain.clone(),
                previous_name_record.subdomain.clone(),
            ),
            previous_name_record.clone(),
        );
        txn_ans_lookups.insert(
            (
                txn_version,
                previous_name_record.domain.clone(),
                previous_name_record.subdomain.clone(),
            ),
            AnsLookup {
                transaction_version: txn_version,
                domain: previous_name_record.domain,
                subdomain: previous_name_record.subdomain,
                registered_address: previous_name_record.registered_address,
                expiration_timestamp: previous_name_record.expiration_timestamp,
                token_name: previous_name_record.token_name,
                is_primary: previous_name_record.is_primary,
                is_deleted: previous_name_record.is_deleted,
                transaction_timestamp: txn_timestamp,
            },
        );
    } else {
        // If it doesn't exist, create a new DB item
        txn_current_ans_lookups.insert(
            (
                name_record_key.get_domain().clone(),
                name_record_key.get_subdomain().clone(),
            ),
            CurrentAnsLookup {
                domain: name_record_key.get_domain(),
                subdomain: name_record_key.get_subdomain(),
                registered_address: name_record.get_target_address(),
                last_transaction_version: txn_version,
                expiration_timestamp: name_record.get_expiration_time(),
                token_name: name_record_key.get_token_name(),
                is_primary: false,
                is_deleted: false,
            },
        );
        txn_ans_lookups.insert(
            (
                txn_version,
                name_record_key.get_domain().clone(),
                name_record_key.get_subdomain().clone(),
            ),
            AnsLookup {
                transaction_version: txn_version,
                domain: name_record_key.get_domain(),
                subdomain: name_record_key.get_subdomain(),
                registered_address: name_record.get_target_address(),
                expiration_timestamp: name_record.get_expiration_time(),
                token_name: name_record_key.get_token_name(),
                is_primary: false,
                is_deleted: false,
                transaction_timestamp: txn_timestamp,
            },
        );
    }
    Ok((txn_current_ans_lookups, txn_ans_lookups))
}

pub fn from_name_record_table_delete_table_item_v1(
    delete_table_item: &DeleteTableItem,
    txn_version: i64,
    txn_timestamp: chrono::NaiveDateTime,
) -> anyhow::Result<(
    HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    HashMap<AnsLookupPK, AnsLookup>,
)> {
    let mut txn_current_ans_lookups = HashMap::new();
    let mut txn_ans_lookups = HashMap::new();

    let table_item_data = delete_table_item.data.as_ref().unwrap();
    let key_type = table_item_data.key_type.as_str();
    let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
        .unwrap_or_else(|e| {
            panic!(
                "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                txn_version, key_type, table_item_data.key, e
            )
        });
    txn_current_ans_lookups.insert(
        (
            name_record_key.get_domain().clone(),
            name_record_key.get_subdomain().clone(),
        ),
        CurrentAnsLookup {
            domain: name_record_key.get_domain().clone(),
            subdomain: name_record_key.get_subdomain().clone(),
            registered_address: None,
            last_transaction_version: txn_version,
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: name_record_key.get_token_name(),
            is_primary: false,
            is_deleted: true,
        },
    );
    txn_ans_lookups.insert(
        (
            txn_version,
            name_record_key.get_domain().clone(),
            name_record_key.get_subdomain().clone(),
        ),
        AnsLookup {
            transaction_version: txn_version,
            domain: name_record_key.get_domain(),
            subdomain: name_record_key.get_subdomain(),
            registered_address: None,
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: name_record_key.get_token_name(),
            is_primary: false,
            is_deleted: true,
            transaction_timestamp: txn_timestamp,
        },
    );
    Ok((txn_current_ans_lookups, txn_ans_lookups))
}

fn lookup_name_record_by_primary_address(
    address_to_primary_name_record: &AddressToPrimaryNameRecord,
    registered_address: &str,
    conn: &mut PgPoolConnection,
) -> Option<CurrentAnsLookup> {
    // Check if the registered_address is in the address_to_primary_name_record map
    let maybe_primary_record = address_to_primary_name_record.get(registered_address);
    if let Some(primary_record) = maybe_primary_record {
        return Some(primary_record.clone());
    }

    // If the name record is not found, it means that it belongs to a different transaction batch and we should
    // lookup the registered_address in DB.
    let mut retried = 0;
    while retried < QUERY_RETRIES {
        retried += 1;
        match CurrentAnsLookupDataQuery::get_name_record_by_primary_address(
            conn,
            registered_address,
        ) {
            Ok(ans_record) => {
                return Some(CurrentAnsLookup {
                    domain: ans_record.domain,
                    subdomain: ans_record.subdomain,
                    registered_address: ans_record.registered_address,
                    last_transaction_version: ans_record.last_transaction_version,
                    expiration_timestamp: ans_record.expiration_timestamp,
                    token_name: ans_record.token_name,
                    is_primary: ans_record.is_primary,
                    is_deleted: ans_record.is_deleted,
                });
            },
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
            },
        }
    }
    None
}

fn lookup_name_record_by_domain_and_subdomain(
    domain: &str,
    subdomain: &str,
    current_ans_lookups: &HashMap<CurrentAnsLookupPK, CurrentAnsLookup>,
    conn: &mut PgPoolConnection,
) -> Option<CurrentAnsLookup> {
    // Check if the domain is in current_ans_lookups
    let maybe_name_record = current_ans_lookups.get(&(domain.to_string(), subdomain.to_string()));
    if let Some(name_record) = maybe_name_record {
        return Some(name_record.clone());
    }

    let mut retried = 0;
    while retried < QUERY_RETRIES {
        retried += 1;
        match CurrentAnsLookupDataQuery::get_name_record_by_domain_and_subdomain(
            conn, domain, subdomain,
        ) {
            Ok(ans_record) => {
                return Some(CurrentAnsLookup {
                    domain: ans_record.domain,
                    subdomain: ans_record.subdomain,
                    registered_address: ans_record.registered_address,
                    last_transaction_version: ans_record.last_transaction_version,
                    expiration_timestamp: ans_record.expiration_timestamp,
                    token_name: ans_record.token_name,
                    is_primary: ans_record.is_primary,
                    is_deleted: ans_record.is_deleted,
                });
            },
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
            },
        }
    }
    None
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    current_ans_lookups: Vec<CurrentAnsLookup>,
    ans_lookups: Vec<AnsLookup>,
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
            insert_to_db_impl(pg_conn, &current_ans_lookups, &ans_lookups)
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let current_ans_lookups = clean_data_for_db(current_ans_lookups, true);
                let ans_lookups = clean_data_for_db(ans_lookups, true);

                insert_to_db_impl(pg_conn, &current_ans_lookups, &ans_lookups)
            }),
    }
}

fn insert_to_db_impl(
    conn: &mut PgConnection,
    current_ans_lookups: &[CurrentAnsLookup],
    ans_lookups: &[AnsLookup],
) -> Result<(), diesel::result::Error> {
    insert_current_ans_lookups(conn, current_ans_lookups)?;
    insert_ans_lookups(conn, ans_lookups)?;
    Ok(())
}

fn insert_current_ans_lookups(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentAnsLookup],
) -> Result<(), diesel::result::Error> {
    use schema::current_ans_lookup::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentAnsLookup::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
                conn,
                diesel::insert_into(schema::current_ans_lookup::table)
                    .values(&items_to_insert[start_ind..end_ind])
                    .on_conflict((domain, subdomain))
                    .do_update()
                    .set((
                        registered_address.eq(excluded(registered_address)),
                        expiration_timestamp.eq(excluded(expiration_timestamp)),
                        last_transaction_version.eq(excluded(last_transaction_version)),
                        inserted_at.eq(excluded(inserted_at)),
                        token_name.eq(excluded(token_name)),
                        is_primary.eq(excluded(is_primary)),
                        is_deleted.eq(excluded(is_deleted)),
                    )),
                    Some(" WHERE current_ans_lookup.last_transaction_version <= excluded.last_transaction_version "),
                )?;
    }
    Ok(())
}

fn insert_ans_lookups(
    conn: &mut PgConnection,
    items_to_insert: &[AnsLookup],
) -> Result<(), diesel::result::Error> {
    use schema::ans_lookup::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), AnsLookup::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::ans_lookup::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, domain, subdomain))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}
