// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::ans_utils::AnsTableItem;
use crate::{
    schema::{ans_lookup, ans_primary_name, current_ans_lookup, current_ans_primary_name},
    utils::util::{get_name_from_unnested_move_type, standardize_address},
};
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type Domain = String;
type Subdomain = String;
// PK of current_ans_lookup, i.e. domain and subdomain name
type CurrentAnsLookupPK = (Domain, Subdomain);
// PK of current_ans_primary_name, i.e. registered_address
type CurrentAnsPrimaryNamePK = String;

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
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
    pub is_deleted: bool,
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_lookup)]
#[diesel(treat_none_as_null = true)]
pub struct AnsLookup {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
}

#[derive(
    Clone,
    Default,
    Debug,
    Deserialize,
    FieldCount,
    Identifiable,
    Insertable,
    Serialize,
    PartialEq,
    Eq,
)]
#[diesel(primary_key(registered_address, token_name))]
#[diesel(table_name = current_ans_primary_name)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsPrimaryName {
    pub registered_address: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index, domain, subdomain))]
#[diesel(table_name = ans_primary_name)]
#[diesel(treat_none_as_null = true)]
pub struct AnsPrimaryName {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
}

impl Ord for CurrentAnsLookup {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.domain
            .cmp(&other.domain)
            .then(self.subdomain.cmp(&other.subdomain))
    }
}

impl PartialOrd for CurrentAnsLookup {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentAnsLookup {
    pub fn pk(&self) -> CurrentAnsLookupPK {
        (self.domain.clone(), self.subdomain.clone())
    }

    // Parse name record from write table item.
    // The table key has the domain and subdomain.
    // The table value data has the metadata (expiration, property version, target address).
    pub fn parse_name_record_from_write_table_item_v1(
        write_table_item: &WriteTableItem,
        ans_v1_name_records_table_handle: &str,
        txn_version: i64,
        write_set_change_index: i64,
    ) -> anyhow::Result<Option<(Self, AnsLookup)>> {
        let table_handle = standardize_address(&write_table_item.handle.to_string());
        if table_handle == standardize_address(ans_v1_name_records_table_handle) {
            if let Some(data) = write_table_item.data.as_ref() {
                // Get the name only, e.g. 0x1::domain::Name. This will return Name
                let key_type_name = get_name_from_unnested_move_type(data.key_type.as_ref());

                if let Some(AnsTableItem::NameRecordKeyV1(name_record_key)) =
                    &AnsTableItem::from_table_item(key_type_name, &data.key, txn_version)?
                {
                    let value_type_name: &str =
                        get_name_from_unnested_move_type(data.value_type.as_ref());
                    if let Some(AnsTableItem::NameRecordV1(name_record)) =
                        &AnsTableItem::from_table_item(value_type_name, &data.value, txn_version)?
                    {
                        return Ok(Some((
                            Self {
                                domain: name_record_key.get_domain_trunc(),
                                subdomain: name_record_key.get_subdomain_trunc(),
                                registered_address: name_record.get_target_address(),
                                expiration_timestamp: name_record.get_expiration_time(),
                                token_name: name_record_key.get_token_name(),
                                last_transaction_version: txn_version,
                                is_deleted: false,
                            },
                            AnsLookup {
                                transaction_version: txn_version,
                                write_set_change_index,
                                domain: name_record_key.get_domain_trunc(),
                                subdomain: name_record_key.get_subdomain_trunc(),
                                registered_address: name_record.get_target_address(),
                                expiration_timestamp: name_record.get_expiration_time(),
                                token_name: name_record_key.get_token_name(),
                                is_deleted: false,
                            },
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    // Parse name record from delete table item.
    // This change results in marking the domain name record as deleted and setting
    // the rest of the fields to default values.
    pub fn parse_name_record_from_delete_table_item_v1(
        delete_table_item: &DeleteTableItem,
        ans_v1_name_records_table_handle: &str,
        txn_version: i64,
        write_set_change_index: i64,
    ) -> anyhow::Result<Option<(Self, AnsLookup)>> {
        let table_handle = standardize_address(&delete_table_item.handle.to_string());
        if table_handle == standardize_address(ans_v1_name_records_table_handle) {
            if let Some(data) = delete_table_item.data.as_ref() {
                let key_type_name = get_name_from_unnested_move_type(data.key_type.as_ref());

                if let Some(AnsTableItem::NameRecordKeyV1(name_record_key)) =
                    &AnsTableItem::from_table_item(key_type_name, &data.key, txn_version)?
                {
                    return Ok(Some((
                        Self {
                            domain: name_record_key.get_domain_trunc(),
                            subdomain: name_record_key.get_subdomain_trunc(),
                            registered_address: None,
                            expiration_timestamp: chrono::NaiveDateTime::default(),
                            token_name: name_record_key.get_token_name(),
                            last_transaction_version: txn_version,
                            is_deleted: true,
                        },
                        AnsLookup {
                            transaction_version: txn_version,
                            write_set_change_index,
                            domain: name_record_key.get_domain_trunc(),
                            subdomain: name_record_key.get_subdomain_trunc(),
                            registered_address: None,
                            expiration_timestamp: chrono::NaiveDateTime::default(),
                            token_name: name_record_key.get_token_name(),
                            is_deleted: true,
                        },
                    )));
                }
            }
        }
        Ok(None)
    }
}

impl Ord for CurrentAnsPrimaryName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.registered_address.cmp(&other.registered_address)
    }
}

impl PartialOrd for CurrentAnsPrimaryName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentAnsPrimaryName {
    pub fn pk(&self) -> CurrentAnsPrimaryNamePK {
        self.registered_address.clone()
    }

    // Parse the primary name reverse record from write table item.
    // The table key is the target address the primary name points to.
    // The table value data has the domain and subdomain of the primary name.
    pub fn parse_primary_name_record_from_write_table_item_v1(
        write_table_item: &WriteTableItem,
        ans_v1_primary_names_table_handle: &str,
        txn_version: i64,
        write_set_change_index: i64,
    ) -> anyhow::Result<Option<(Self, AnsPrimaryName)>> {
        let table_handle = standardize_address(&write_table_item.handle.to_string());
        if table_handle == standardize_address(ans_v1_primary_names_table_handle) {
            if let Some(data) = write_table_item.data.as_ref() {
                // Return early if key is not address type. This should not be possible but just a precaution
                // in case we input the wrong table handle
                if data.key_type != "address" {
                    return Ok(None);
                }
                let decoded_key: String = serde_json::from_str(data.key.as_str()).unwrap();
                let registered_address = standardize_address(decoded_key.as_str());
                let value_type_name = get_name_from_unnested_move_type(data.value_type.as_ref());
                if let Some(AnsTableItem::NameRecordKeyV1(name_record_key)) =
                    &AnsTableItem::from_table_item(value_type_name, &data.value, txn_version)?
                {
                    return Ok(Some((
                        Self {
                            registered_address: registered_address.clone(),
                            domain: Some(name_record_key.get_domain_trunc()),
                            subdomain: Some(name_record_key.get_subdomain_trunc()),
                            token_name: Some(name_record_key.get_token_name()),
                            last_transaction_version: txn_version,
                            is_deleted: false,
                        },
                        AnsPrimaryName {
                            transaction_version: txn_version,
                            write_set_change_index,
                            registered_address,
                            domain: Some(name_record_key.get_domain_trunc()),
                            subdomain: Some(name_record_key.get_subdomain_trunc()),
                            token_name: Some(name_record_key.get_token_name()),
                            is_deleted: false,
                        },
                    )));
                }
            }
        }
        Ok(None)
    }

    // Parse primary name from delete table item
    // We need to lookup which domain the address points to so we can mark it as non-primary.
    pub fn parse_primary_name_record_from_delete_table_item_v1(
        delete_table_item: &DeleteTableItem,
        ans_v1_primary_names_table_handle: &str,
        txn_version: i64,
        write_set_change_index: i64,
    ) -> anyhow::Result<Option<(Self, AnsPrimaryName)>> {
        let table_handle = standardize_address(&delete_table_item.handle.to_string());
        if table_handle == standardize_address(ans_v1_primary_names_table_handle) {
            if let Some(data) = delete_table_item.data.as_ref() {
                // Return early if key is not address type. This should not be possible but just a precaution
                // in case we input the wrong table handle
                if data.key_type != "address" {
                    return Ok(None);
                }
                let decoded_key: String = serde_json::from_str(data.key.as_str()).unwrap();
                let registered_address = standardize_address(decoded_key.as_str());
                return Ok(Some((
                    Self {
                        registered_address: registered_address.clone(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        last_transaction_version: txn_version,
                        is_deleted: true,
                    },
                    AnsPrimaryName {
                        transaction_version: txn_version,
                        write_set_change_index,
                        registered_address,
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        is_deleted: true,
                    },
                )));
            }
        }
        Ok(None)
    }
}
