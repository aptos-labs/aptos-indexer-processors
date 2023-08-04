// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS};
use crate::{
    schema::current_ans_lookup,
    utils::{
        database::PgPoolConnection,
        util::{
            bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
        },
    },
};
use anyhow::Context;
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, DeleteTableItem,
    Transaction as TransactionPB, WriteTableItem,
};
use bigdecimal::BigDecimal;
use diesel::{prelude::*, sql_query, sql_types::Text};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::{cmp, collections::HashMap};

type Domain = String;
type Subdomain = String;
// Hashmap for looking up previous name records by target address
pub type AddressToPrimaryNameRecord = HashMap<String, CurrentAnsLookup>;
// PK of current_ans_lookup, i.e. domain and subdomain name
pub type CurrentAnsLookupPK = (Domain, Subdomain);

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    QueryableByName,
)]
#[diesel(primary_key(domain, subdomain))]
#[diesel(table_name = current_ans_lookup)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsLookup {
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_primary: bool,
    pub is_deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OptionalString {
    vec: Vec<String>,
}

impl OptionalString {
    fn get_string(&self) -> Option<String> {
        if self.vec.is_empty() {
            None
        } else {
            Some(self.vec[0].clone())
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NameRecordKeyV1 {
    domain_name: String,
    subdomain_name: OptionalString,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NameRecordV1 {
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_sec: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    property_version: BigDecimal,
    target_address: OptionalString,
}

impl CurrentAnsLookup {
    pub fn from_transaction(
        transaction: &TransactionPB,
        address_to_primary_name_record: &mut AddressToPrimaryNameRecord,
        maybe_ans_primary_names_table_handle: Option<String>,
        maybe_ans_name_records_table_handle: Option<String>,
        conn: &mut PgPoolConnection,
    ) -> HashMap<CurrentAnsLookupPK, Self> {
        let mut current_ans_lookups: HashMap<CurrentAnsLookupPK, Self> = HashMap::new();
        let txn_version = transaction.version as i64;
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
            maybe_ans_primary_names_table_handle,
            maybe_ans_name_records_table_handle,
        ) {
            let transaction_info = transaction
                .info
                .as_ref()
                .expect("Transaction info doesn't exist!");

            // Extract primary names and name records
            for wsc in &transaction_info.changes {
                if let Ok(name_records) = match wsc.change.as_ref().unwrap() {
                    WriteSetChangeEnum::WriteTableItem(write_table_item) => {
                        Self::from_write_table_item(
                            write_table_item,
                            txn_version,
                            address_to_primary_name_record,
                            ans_primary_names_table_handle.as_str(),
                            ans_name_records_table_handle.as_str(),
                            conn,
                        )
                    },
                    WriteSetChangeEnum::DeleteTableItem(delete_table_item) => {
                        Self::from_delete_table_item(
                            delete_table_item,
                            txn_version,
                            address_to_primary_name_record,
                            ans_primary_names_table_handle.as_str(),
                            ans_name_records_table_handle.as_str(),
                            conn,
                        )
                    },
                    _ => Ok(HashMap::new()),
                } {
                    for (record_key, name_record) in name_records {
                        // Upsert name record or primary name record into current_ans_lookup
                        current_ans_lookups
                            .entry(record_key.clone())
                            .and_modify(|e| {
                                e.registered_address = name_record.registered_address.clone();
                                // name_record.expiration_timestamp may be zero if primary name wsc occurs before name record wsc,
                                // so return the larger of the two timestamps
                                e.expiration_timestamp = cmp::max(
                                    name_record.expiration_timestamp,
                                    e.expiration_timestamp,
                                );
                                // name_record.is_primary may be false here if primary name wsc occurs before name record wsc,
                                // so use OR to preserve is_primary status
                                e.is_primary = name_record.is_primary || e.is_primary;
                                e.is_deleted = name_record.is_deleted;
                            })
                            .or_insert(name_record);

                        // For each txn batch, we maintain a mapping of address -> historical primary name record to help with
                        // lookups when a primary name record gets deleted.
                        let current_name_record = current_ans_lookups
                            .get(&record_key.clone())
                            .unwrap()
                            .clone();
                        if current_name_record.is_primary {
                            let registered_address = current_name_record
                                .registered_address
                                .as_ref()
                                .unwrap()
                                .clone();
                            address_to_primary_name_record
                                .insert(registered_address, current_name_record);
                        }
                    }
                } else {
                    continue;
                }
            }
        }

        current_ans_lookups
    }

    // Parse the following from write table item
    // 1. When a primary name reverse lookup is created or updated
    // 2. When a name record is created or updated
    pub fn from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        ans_primary_names_table_handle: &str,
        ans_name_records_table_handle: &str,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let table_handle = table_item.handle.as_str();
        let mut current_ans_lookup = HashMap::new();

        if table_handle == ans_primary_names_table_handle {
            // Parse the primary name reverse record.
            // The table key is the target address the primary name points to.
            // The table value data has the domain and subdomain of the primary name.
            let table_item_data = table_item.data.as_ref().unwrap();
            let key_type = table_item_data.key_type.as_str();
            let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
                panic!(
                    "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                    txn_version, key_type, table_item_data.key, e
                )
            });
            let target_address = standardize_address(key);
            let value_type = table_item_data.value_type.as_str();
            let value = &table_item_data.value;
            let primary_name_record: NameRecordKeyV1 =
                serde_json::from_str(value).unwrap_or_else(|e| {
                    panic!(
                        "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                        txn_version, value_type, value, e
                    )
                });
            let subdomain = primary_name_record
                .subdomain_name
                .get_string()
                .unwrap_or_default();
            let mut token_name = format!("{}.apt", &primary_name_record.domain_name);
            if !subdomain.is_empty() {
                token_name = format!("{}.{}", &subdomain, token_name);
            }
            // When a primary name gets updated, we need to set the previous primary name record is_primary = false.
            // Lookup the previous primary name record by target address. It may not exist.
            let maybe_primary_record = match Self::get_primary_record_from_address(
                address_to_primary_name_record,
                &target_address,
                conn,
            )
            .context(format!(
                "Failed to get ans record for registered address {}, txn version {}",
                target_address, txn_version
            )) {
                Ok(maybe_ans_record) => maybe_ans_record,
                Err(_) => {
                    tracing::error!(
                                transaction_version = txn_version,
                                lookup_key = &table_handle,
                                "Failed to get ans record for registered address. You probably should backfill db."
                            );
                    return Ok(HashMap::new());
                },
            };
            // Update the old primary name record
            if let Some(mut primary_record) = maybe_primary_record {
                primary_record.is_primary = false;
                primary_record.last_transaction_version = txn_version;
                current_ans_lookup.insert(
                    (
                        primary_record.domain.clone(),
                        primary_record.subdomain.clone(),
                    ),
                    primary_record,
                );
            }
            // Create the new primary name record
            current_ans_lookup.insert(
                (primary_name_record.domain_name.clone(), subdomain.clone()),
                Self {
                    domain: primary_name_record.domain_name,
                    subdomain,
                    registered_address: Some(target_address),
                    last_transaction_version: txn_version,
                    // There's no expiration time in the primary name reverse lookup so default to zero
                    expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                        .unwrap_or_default(),
                    token_name,
                    is_primary: true,
                    is_deleted: false,
                },
            );
        } else if table_handle == ans_name_records_table_handle {
            // Parse name record.
            // The table key has the domain and subdomain.
            // The table value data has the metadata (expiration, property version, target address).
            let table_item_data = table_item.data.as_ref().unwrap();
            let key_type = table_item_data.key_type.as_str();
            let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
                .unwrap_or_else(|e| {
                    panic!(
                        "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                        txn_version, key_type, table_item_data.key, e
                    )
                });
            let value_type = table_item_data.value_type.as_str();
            let name_record: NameRecordV1 = serde_json::from_str(&table_item_data.value)
                .unwrap_or_else(|e| {
                    panic!(
                        "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                        txn_version, value_type, table_item_data.value, e
                    )
                });
            let subdomain = name_record_key
                .subdomain_name
                .get_string()
                .unwrap_or_default();
            let mut token_name = format!("{}.apt", &name_record_key.domain_name);
            if !subdomain.is_empty() {
                token_name = format!("{}.{}", &subdomain, token_name);
            }
            let expiration_timestamp = parse_timestamp_secs(
                bigdecimal_to_u64(&name_record.expiration_time_sec),
                txn_version,
            );
            let mut target_address = name_record.target_address.get_string().unwrap_or_default();
            if !target_address.is_empty() {
                target_address = standardize_address(&target_address);
            }
            current_ans_lookup.insert(
                (name_record_key.domain_name.clone(), subdomain.clone()),
                Self {
                    domain: name_record_key.domain_name,
                    subdomain,
                    registered_address: Some(target_address),
                    last_transaction_version: txn_version,
                    expiration_timestamp,
                    token_name,
                    is_primary: false,
                    is_deleted: false,
                },
            );
        }
        Ok(current_ans_lookup)
    }

    // Parse the following from delete table item:
    // 1. When a primary name reverse record is deleted
    // 2. When a name record is deleted
    pub fn from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        address_to_name_record: &AddressToPrimaryNameRecord,
        ans_primary_names_table_handle: &str,
        ans_name_records_table_handle: &str,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let table_handle = table_item.handle.as_str();
        let mut current_ans_lookup: HashMap<CurrentAnsLookupPK, Self> = HashMap::new();

        // Delete name record
        if table_handle == ans_primary_names_table_handle {
            let table_item_data = table_item.data.as_ref().unwrap();
            let key_type = table_item_data.key_type.as_str();
            let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
                panic!(
                    "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                    txn_version, key_type, table_item_data.key, e
                )
            });
            let registered_address = standardize_address(key);

            // Need to lookup previous name record to get the domain, subdomain, and metadata associated with the deleted
            // reverse lookup record. Do a lookup with the registered_address.
            let mut primary_record = match Self::get_primary_record_from_address(
                address_to_name_record,
                &registered_address,
                conn,
            )
            .context(format!(
                "Failed to get ans record for registered address {}, txn version {}",
                registered_address, txn_version
            )) {
                Ok(Some(ans_record)) => ans_record,
                Ok(None) | Err(_) => {
                    tracing::error!(
                        transaction_version = txn_version,
                        lookup_key = &table_handle,
                        "Failed to get ans record for registered address. You probably should backfill db."
                    );
                    return Ok(HashMap::new());
                },
            };
            // Update the lookup's name record with the new data and return
            primary_record.is_primary = false;
            primary_record.last_transaction_version = txn_version;
            current_ans_lookup.insert(
                (
                    primary_record.domain.clone(),
                    primary_record.subdomain.clone(),
                ),
                primary_record,
            );
        } else if table_handle == ans_name_records_table_handle {
            let table_item_data = table_item.data.as_ref().unwrap();
            let key_type = table_item_data.key_type.as_str();
            let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
                .unwrap_or_else(|e| {
                    panic!(
                        "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                        txn_version, key_type, table_item_data.key, e
                    )
                });
            let subdomain = name_record_key
                .subdomain_name
                .get_string()
                .unwrap_or_default();
            let mut token_name = format!("{}.apt", &name_record_key.domain_name);
            if !subdomain.is_empty() {
                token_name = format!("{}.{}", &subdomain, token_name);
            }
            current_ans_lookup.insert(
                (name_record_key.domain_name.clone(), subdomain.clone()),
                Self {
                    domain: name_record_key.domain_name,
                    subdomain,
                    registered_address: None,
                    last_transaction_version: txn_version,
                    expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                        .unwrap_or_default(),
                    token_name,
                    is_primary: false,
                    is_deleted: true,
                },
            );
        }
        Ok(current_ans_lookup)
    }

    fn get_primary_record_from_address(
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        registered_address: &str,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<Option<Self>> {
        // Check if the registered_address is in the address_to_primary_name_record map
        let maybe_primary_record = address_to_primary_name_record.get(registered_address);
        if let Some(primary_record) = maybe_primary_record {
            return Ok(Some(primary_record.clone()));
        }

        // If the name record is not found, it means that it belongs to a different transaction batch and we should
        // lookup the registered_address in DB.
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match Self::get_by_registered_address(conn, registered_address) {
                Ok(ans_record) => return Ok(Some(ans_record)),
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        Ok(None)
    }

    fn get_by_registered_address(
        conn: &mut PgPoolConnection,
        registered_address: &str,
    ) -> anyhow::Result<Self> {
        let mut res: Vec<Option<Self>> =
            sql_query("SELECT * FROM current_ans_lookup WHERE registered_address = $1")
                .bind::<Text, _>(registered_address)
                .get_results(conn)?;
        res.pop()
            .context("ans record result empty")?
            .context("ans record result empty")
    }
}
