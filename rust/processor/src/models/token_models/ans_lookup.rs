// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    schema::current_ans_lookup,
    utils::util::{
        bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
    },
};
use aptos_indexer_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, DeleteTableItem,
    Transaction as TransactionPB, WriteTableItem,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type Domain = String;
type Subdomain = String;
// PK of current_ans_lookup, i.e. domain and subdomain name
pub type CurrentAnsLookupPK = (Domain, Subdomain);

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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

pub enum ANSEvent {
    SetNameAddressEventV1(SetNameAddressEventV1),
    RegisterNameEventV1(RegisterNameEventV1),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetNameAddressEventV1 {
    subdomain_name: OptionalString,
    domain_name: String,
    new_address: OptionalString,
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_secs: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterNameEventV1 {
    subdomain_name: OptionalString,
    domain_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_secs: BigDecimal,
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
        maybe_ans_primary_names_table_handle: Option<String>,
        maybe_ans_name_records_table_handle: Option<String>,
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
                if let Ok(maybe_name_record) = match wsc.change.as_ref().unwrap() {
                    WriteSetChangeEnum::WriteTableItem(write_table_item) => {
                        Self::from_write_table_item(
                            write_table_item,
                            txn_version,
                            ans_primary_names_table_handle.as_str(),
                            ans_name_records_table_handle.as_str(),
                        )
                    },
                    WriteSetChangeEnum::DeleteTableItem(delete_table_item) => {
                        Self::from_delete_table_item(
                            delete_table_item,
                            txn_version,
                            ans_name_records_table_handle.as_str(),
                        )
                    },
                    _ => Ok(None),
                } {
                    if let Some(name_record) = maybe_name_record {
                        current_ans_lookups
                            .entry((name_record.domain.clone(), name_record.subdomain.clone()))
                            .and_modify(|e| {
                                e.registered_address = name_record.registered_address.clone();
                                e.expiration_timestamp = name_record.expiration_timestamp;
                                e.is_primary = name_record.is_primary || e.is_primary;
                                e.is_deleted = name_record.is_deleted;
                            })
                            .or_insert(name_record);
                    }
                } else {
                    continue;
                }
            }
        }

        current_ans_lookups
    }

    pub fn from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        ans_primary_names_table_handle: &str,
        ans_name_records_table_handle: &str,
    ) -> anyhow::Result<Option<Self>> {
        let table_handle = table_item.handle.as_str();
        let mut maybe_name_record = None;

        // Create primary name reverse lookup
        if table_handle == ans_primary_names_table_handle {
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
            maybe_name_record = Some(Self {
                domain: primary_name_record.domain_name,
                subdomain,
                registered_address: Some(target_address),
                last_transaction_version: txn_version,
                expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                    .unwrap_or_default(),
                token_name,
                is_primary: true,
                is_deleted: false,
            })
        } else if table_handle == ans_name_records_table_handle {
            // Create name record (primary + non-primary)
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
            maybe_name_record = Some(Self {
                domain: name_record_key.domain_name,
                subdomain,
                registered_address: Some(target_address),
                last_transaction_version: txn_version,
                expiration_timestamp,
                token_name,
                is_primary: false,
                is_deleted: false,
            })
        }
        Ok(maybe_name_record)
    }

    pub fn from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        ans_name_records_table_handle: &str,
    ) -> anyhow::Result<Option<Self>> {
        let table_handle = table_item.handle.as_str();
        let mut maybe_name_record = None;

        // Delete name record
        if table_handle == ans_name_records_table_handle {
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
            maybe_name_record = Some(Self {
                domain: name_record_key.domain_name,
                subdomain,
                registered_address: None,
                last_transaction_version: txn_version,
                expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                    .unwrap_or_default(),
                token_name,
                is_primary: false,
                is_deleted: true,
            })
        }
        Ok(maybe_name_record)
    }
}
