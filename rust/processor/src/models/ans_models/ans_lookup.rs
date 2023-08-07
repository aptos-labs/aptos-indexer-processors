// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    schema::current_ans_lookup,
    utils::{
        database::PgPoolConnection,
        util::{
            bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
        },
    },
};
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, DeleteTableItem,
    Transaction as TransactionPB, WriteTableItem,
};
use bigdecimal::BigDecimal;
use diesel::{prelude::*, ExpressionMethods};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::{cmp, collections::HashMap};

pub const QUERY_RETRIES: u32 = 5;
pub const QUERY_RETRY_DELAY_MS: u64 = 500;

type Domain = String;
type Subdomain = String;
// Hashmap for looking up previous name records by target address
pub type AddressToPrimaryNameRecord = HashMap<String, CurrentAnsLookup>;
// PK of current_ans_lookup, i.e. domain and subdomain name
pub type CurrentAnsLookupPK = (Domain, Subdomain);

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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

/// Need a separate struct for queryable because we don't want to define the inserted_at column (letting DB fill)
#[derive(Debug, Identifiable, Queryable)]
#[diesel(primary_key(domain, subdomain))]
#[diesel(table_name = current_ans_lookup)]
pub struct CurrentAnsLookupDataQuery {
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_primary: bool,
    pub is_deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OptionalString {
    vec: Vec<String>,
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
        current_ans_lookups: &HashMap<CurrentAnsLookupPK, Self>,
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        maybe_ans_primary_names_table_handle: Option<String>,
        maybe_ans_name_records_table_handle: Option<String>,
        conn: &mut PgPoolConnection,
    ) -> HashMap<CurrentAnsLookupPK, Self> {
        let mut new_current_ans_lookups: HashMap<CurrentAnsLookupPK, Self> = HashMap::new();

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

            // Extract name record changes
            for wsc in &transaction_info.changes {
                let current_ans_lookups_res = match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        Self::from_name_record_table_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            current_ans_lookups,
                            conn,
                        )
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_name_records_table_handle.as_str() {
                            continue;
                        }
                        Self::from_name_record_table_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                        )
                    },
                    _ => continue,
                };
                match current_ans_lookups_res {
                    Ok(ans_lookups) => new_current_ans_lookups.extend(ans_lookups),
                    Err(e) => {
                        tracing::error!("Failed to parse name record table item. Error: {:?}", e);
                        continue;
                    },
                }
            }

            // Extract primary name reverse lookup changes
            for wsc in &transaction_info.changes {
                let current_ans_lookup_res = match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteTableItem(write_table_item) => {
                        let table_handle = write_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        Self::from_primary_name_record_table_write_table_item_v1(
                            write_table_item,
                            txn_version,
                            address_to_primary_name_record,
                            conn,
                        )
                    },
                    WriteSetChange::DeleteTableItem(delete_table_item) => {
                        let table_handle = delete_table_item.handle.as_str();
                        if table_handle != ans_primary_names_table_handle.as_str() {
                            continue;
                        }
                        Self::from_primary_name_record_table_delete_table_item_v1(
                            delete_table_item,
                            txn_version,
                            address_to_primary_name_record,
                            conn,
                        )
                    },
                    _ => continue,
                };

                match current_ans_lookup_res {
                    Ok(ans_lookups) => {
                        for (record_key, name_record) in ans_lookups {
                            // Name record may already exist in current_ans_lookup, so need to upsert here
                            new_current_ans_lookups
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
        new_current_ans_lookups
    }

    // Parse the primary name reverse record from write table item.
    // The table key is the target address the primary name points to.
    // The table value data has the domain and subdomain of the primary name.
    pub fn from_primary_name_record_table_write_table_item_v1(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let mut new_current_ans_lookups = HashMap::new();

        let table_item_data = write_table_item.data.as_ref().unwrap();
        let key_type = table_item_data.key_type.as_str();
        let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
            panic!(
                "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                txn_version, key_type, table_item_data.key, e
            )
        });
        let value_type: &str = table_item_data.value_type.as_str();
        let value = &table_item_data.value;
        let primary_name_record: NameRecordKeyV1 =
            serde_json::from_str(value).unwrap_or_else(|e| {
                panic!(
                    "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                    txn_version, value_type, value, e
                )
            });

        // Case: when a primary name points to a new address, we need to set the previous primary name record's to non-primary.
        // Lookup the previous primary name record by target address. It may not exist.
        if let Some(mut previous_primary_record) =
            Self::lookup_name_record_by_primary_address(address_to_primary_name_record, key, conn)
        {
            // Update the old primary name record
            previous_primary_record.is_primary = false;
            previous_primary_record.last_transaction_version = txn_version;
            new_current_ans_lookups.insert(
                (
                    previous_primary_record.domain.clone(),
                    previous_primary_record.subdomain.clone(),
                ),
                previous_primary_record,
            );
        }

        // If the name record doesn't exist yet, create it and insert it to current_ans_lookups.
        // This is possible if the primary name record wsc happens before the name record wsc.
        new_current_ans_lookups.insert(
            (
                primary_name_record.get_domain().clone(),
                primary_name_record.get_subdomain().clone(),
            ),
            Self {
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

        Ok(new_current_ans_lookups)
    }

    pub fn from_primary_name_record_table_delete_table_item_v1(
        delete_table_item: &DeleteTableItem,
        txn_version: i64,
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let mut new_current_ans_lookups = HashMap::new();

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
        let mut previous_name_record = match Self::lookup_name_record_by_primary_address(
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
                return Ok(HashMap::new());
            },
        };
        // Update the lookup's name record with the new data and return
        previous_name_record.is_primary = false;
        previous_name_record.last_transaction_version = txn_version;
        new_current_ans_lookups.insert(
            (
                previous_name_record.domain.clone(),
                previous_name_record.subdomain.clone(),
            ),
            previous_name_record,
        );

        Ok(new_current_ans_lookups)
    }

    pub fn from_name_record_table_write_table_item_v1(
        write_table_item: &WriteTableItem,
        txn_version: i64,
        current_ans_lookups: &HashMap<CurrentAnsLookupPK, Self>,
        conn: &mut PgPoolConnection,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let mut new_current_ans_lookups = HashMap::new();

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
        let name_record: NameRecordV1 = serde_json::from_str(&table_item_data.value)
            .unwrap_or_else(|e| {
                panic!(
                    "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                    txn_version, value_type, table_item_data.value, e
                )
            });

        // Need to lookup the previous domain to persist is_primary flag:
        // 1. bob.apt -> is_primary = true
        // 2. Transaction updates bob.apt's metadata (like renewing the domain)
        // 3. is_primary should be the same in the inserted row
        if let Some(mut previous_name_record) = Self::lookup_name_record_by_domain_and_subdomain(
            &name_record_key.get_domain(),
            &name_record_key.get_subdomain(),
            current_ans_lookups,
            conn,
        ) {
            // If domain exists, modify the existing DB item
            previous_name_record.expiration_timestamp = name_record.get_expiration_time();
            previous_name_record.registered_address = name_record.get_target_address();
            previous_name_record.last_transaction_version = txn_version;
            new_current_ans_lookups.insert(
                (
                    previous_name_record.domain.clone(),
                    previous_name_record.subdomain.clone(),
                ),
                previous_name_record,
            );
        } else {
            // If it doesn't exist, create a new DB item
            new_current_ans_lookups.insert(
                (
                    name_record_key.get_domain().clone(),
                    name_record_key.get_subdomain().clone(),
                ),
                Self {
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
        }
        Ok(new_current_ans_lookups)
    }

    pub fn from_name_record_table_delete_table_item_v1(
        delete_table_item: &DeleteTableItem,
        txn_version: i64,
    ) -> anyhow::Result<HashMap<CurrentAnsLookupPK, Self>> {
        let mut new_current_ans_lookups = HashMap::new();

        let table_item_data = delete_table_item.data.as_ref().unwrap();
        let key_type = table_item_data.key_type.as_str();
        let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
            .unwrap_or_else(|e| {
                panic!(
                    "version {} failed! failed to parse type {}, data {:?}. Error: {:?}",
                    txn_version, key_type, table_item_data.key, e
                )
            });
        new_current_ans_lookups.insert(
            (
                name_record_key.get_domain().clone(),
                name_record_key.get_subdomain().clone(),
            ),
            Self {
                domain: name_record_key.get_domain().clone(),
                subdomain: name_record_key.get_subdomain().clone(),
                registered_address: None,
                last_transaction_version: txn_version,
                expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                    .unwrap_or_default(),
                token_name: name_record_key.get_token_name(),
                is_primary: false,
                is_deleted: true,
            },
        );
        Ok(new_current_ans_lookups)
    }

    fn lookup_name_record_by_primary_address(
        address_to_primary_name_record: &AddressToPrimaryNameRecord,
        registered_address: &str,
        conn: &mut PgPoolConnection,
    ) -> Option<Self> {
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
        current_ans_lookups: &HashMap<CurrentAnsLookupPK, Self>,
        conn: &mut PgPoolConnection,
    ) -> Option<Self> {
        // Check if the domain is in current_ans_lookups
        let maybe_name_record =
            current_ans_lookups.get(&(domain.to_string(), subdomain.to_string()));
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
}

impl CurrentAnsLookupDataQuery {
    pub fn get_name_record_by_domain_and_subdomain(
        conn: &mut PgPoolConnection,
        domain: &str,
        subdomain: &str,
    ) -> diesel::QueryResult<Self> {
        current_ans_lookup::table
            .filter(current_ans_lookup::domain.eq(domain))
            .filter(current_ans_lookup::subdomain.eq(subdomain))
            .first::<Self>(conn)
    }

    pub fn get_name_record_by_primary_address(
        conn: &mut PgPoolConnection,
        registered_address: &str,
    ) -> diesel::QueryResult<Self> {
        current_ans_lookup::table
            .filter(current_ans_lookup::registered_address.eq(registered_address))
            .filter(current_ans_lookup::is_primary.eq(true))
            .first::<Self>(conn)
    }
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

impl NameRecordKeyV1 {
    pub fn get_domain(&self) -> String {
        self.domain_name.clone()
    }

    pub fn get_subdomain(&self) -> String {
        self.subdomain_name.get_string().unwrap_or_default()
    }

    pub fn get_token_name(&self) -> String {
        let domain = self.get_domain();
        let subdomain = self.get_subdomain();
        let mut token_name = format!("{}.apt", &domain);
        if !subdomain.is_empty() {
            token_name = format!("{}.{}", &subdomain, token_name);
        }
        token_name
    }
}

impl NameRecordV1 {
    pub fn get_expiration_time(&self) -> chrono::NaiveDateTime {
        parse_timestamp_secs(bigdecimal_to_u64(&self.expiration_time_sec), 0)
    }

    pub fn get_property_version(&self) -> u64 {
        bigdecimal_to_u64(&self.property_version)
    }

    pub fn get_target_address(&self) -> Option<String> {
        let target_address = self.target_address.get_string().unwrap_or_default();
        if !target_address.is_empty() {
            Some(standardize_address(&target_address))
        } else {
            None
        }
    }
}
