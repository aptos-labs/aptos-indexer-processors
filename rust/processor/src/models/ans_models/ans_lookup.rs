// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    schema::{ans_lookup, ans_primary_name, current_ans_lookup, current_ans_primary_name},
    utils::{
        database::PgPoolConnection,
        util::{
            bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
        },
    },
};
use aptos_indexer_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const QUERY_RETRIES: u32 = 5;
pub const QUERY_RETRY_DELAY_MS: u64 = 500;

pub type Address = String;
pub type Domain = String;
pub type Subdomain = String;
// PK of current_ans_lookup, i.e. domain and subdomain name
pub type CurrentAnsLookupPK = (Domain, Subdomain);
// PK of current_ans_primary_names, i.e. address, domain, and subdomain name
pub type CurrentAnsPrimaryNamePK = (Address, Domain, Subdomain);
// PK of ans_lookup, i.e. transaction version, domain, and subdomain
pub type AnsLookupPK = (i64, Domain, Subdomain);

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
    pub is_deleted: bool,
}

impl CurrentAnsLookup {
    pub fn pk(&self) -> CurrentAnsLookupPK {
        (self.domain.clone(), self.subdomain.clone())
    }
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, wsc_index))]
#[diesel(table_name = ans_lookup)]
#[diesel(treat_none_as_null = true)]
pub struct AnsLookup {
    pub transaction_version: i64,
    pub wsc_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(registered_address, token_name))]
#[diesel(table_name = current_ans_primary_name)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsPrimaryName {
    pub registered_address: String,
    pub domain: String,
    pub subdomain: String,
    pub token_name: String,
    pub is_primary: bool,
    pub last_transaction_version: i64,
}

impl CurrentAnsPrimaryName {
    pub fn pk(&self) -> CurrentAnsPrimaryNamePK {
        (
            self.registered_address.clone(),
            self.domain.clone(),
            self.subdomain.clone(),
        )
    }
}

/// Need a separate struct for queryable because we don't want to define the inserted_at column (letting DB fill)
#[derive(Debug, Identifiable, Queryable)]
#[diesel(primary_key(registered_address, domain, subdomain))]
#[diesel(table_name = current_ans_primary_name)]
pub struct CurrentAnsPrimaryNameDataQuery {
    pub registered_address: String,
    pub domain: String,
    pub subdomain: String,
    pub token_name: String,
    pub is_primary: bool,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

impl CurrentAnsPrimaryNameDataQuery {
    pub fn get_name_record_by_primary_address(
        conn: &mut PgPoolConnection,
        registered_address: &str,
    ) -> diesel::QueryResult<Self> {
        current_ans_primary_name::table
            .filter(current_ans_primary_name::registered_address.eq(registered_address))
            .first::<Self>(conn)
    }
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, wsc_index, domain, subdomain))]
#[diesel(table_name = ans_primary_name)]
#[diesel(treat_none_as_null = true)]
pub struct AnsPrimaryName {
    pub transaction_version: i64,
    pub wsc_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub registered_address: String,
    pub token_name: String,
    pub is_primary: bool,
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

// Parse the primary name reverse record from write table item.
// The table key is the target address the primary name points to.
// The table value data has the domain and subdomain of the primary name.
// We also need to lookup which domain the address previoulsy pointed to,
// so we can mark it as non-primary.
pub fn parse_primary_name_record_from_write_table_item_v1(
    write_table_item: &WriteTableItem,
    txn_version: i64,
    wsc_index: i64,
    lookup_ans_primary_names: &HashMap<Address, CurrentAnsPrimaryName>,
    conn: &mut PgPoolConnection,
) -> anyhow::Result<(
    CurrentAnsPrimaryName,         // New current primary name record
    AnsPrimaryName,                // New primary name record
    Option<CurrentAnsPrimaryName>, // Old current primary name record
    Option<AnsPrimaryName>,        // Old primary name record
)> {
    let mut old_current_primary_name_record: Option<CurrentAnsPrimaryName> = None;
    let mut old_primary_name_record: Option<AnsPrimaryName> = None;

    let table_item_data = write_table_item.data.as_ref().unwrap();
    let key: &str = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
        tracing::error!(
            transaction_version = txn_version,
            data = table_item_data.key,
            e = ?e,
            "Failed to parse address from ANS reverse lookup table item",
        );
        panic!();
    });
    let registered_address = standardize_address(key);
    let value = &table_item_data.value;
    let primary_name_record: NameRecordKeyV1 = serde_json::from_str(value).unwrap_or_else(|e| {
        tracing::error!(
            transaction_version = txn_version,
            data = value,
            e = ?e,
            "Failed to parse NameRecordKeyV1 from ANS reverse lookup table item",
        );
        panic!();
    });

    // Need to lookup old primary name record to update the row's is_primary = false
    if let Some(lookup_primary_name_record) = match lookup_name_record_by_primary_address(
        lookup_ans_primary_names,
        &registered_address,
        conn,
    ) {
        Some(ans_record) => Some(ans_record),
        None => {
            tracing::error!(
                transaction_version = txn_version,
                lookup_key = &registered_address,
                "Failed to get ans record for registered address. You probably should backfill db."
            );
            None
        },
    } {
        // Update the old primary name record with is_primary = false
        old_current_primary_name_record = Some(CurrentAnsPrimaryName {
            registered_address: registered_address.clone(),
            domain: lookup_primary_name_record.domain.clone(),
            subdomain: lookup_primary_name_record.subdomain.clone(),
            token_name: lookup_primary_name_record.token_name.clone(),
            is_primary: false,
            last_transaction_version: txn_version,
        });
        old_primary_name_record = Some(AnsPrimaryName {
            transaction_version: txn_version,
            wsc_index,
            registered_address: registered_address.clone(),
            domain: lookup_primary_name_record.domain.clone(),
            subdomain: lookup_primary_name_record.subdomain.clone(),
            token_name: lookup_primary_name_record.token_name.clone(),
            is_primary: false,
        });
    }

    Ok((
        CurrentAnsPrimaryName {
            registered_address: registered_address.clone(),
            domain: primary_name_record.get_domain().clone(),
            subdomain: primary_name_record.get_subdomain().clone(),
            token_name: primary_name_record.get_token_name(),
            is_primary: true,
            last_transaction_version: txn_version,
        },
        AnsPrimaryName {
            transaction_version: txn_version,
            wsc_index,
            registered_address: registered_address.clone(),
            domain: primary_name_record.get_domain().clone(),
            subdomain: primary_name_record.get_subdomain().clone(),
            token_name: primary_name_record.get_token_name(),
            is_primary: true,
        },
        old_current_primary_name_record,
        old_primary_name_record,
    ))
}

// Parse primary name from delete table item
// We need to lookup which domain the address points to so we can mark it as non-primary.
pub fn parse_primary_name_record_from_delete_table_item_v1(
    delete_table_item: &DeleteTableItem,
    txn_version: i64,
    wsc_index: i64,
    lookup_ans_primary_names: &HashMap<Address, CurrentAnsPrimaryName>,
    conn: &mut PgPoolConnection,
) -> anyhow::Result<Option<(CurrentAnsPrimaryName, AnsPrimaryName)>> {
    let table_item_data = delete_table_item.data.as_ref().unwrap();
    let key = serde_json::from_str(&table_item_data.key).unwrap_or_else(|e| {
        tracing::error!(
            transaction_version = txn_version,
            e = ?e,
            "Failed to parse address from ANS reverse lookup table item",
        );
        panic!();
    });
    let registered_address = standardize_address(key);

    // Need to lookup old primary name record to update the row's is_primary = false
    if let Some(mut lookup_primary_name_record) = match lookup_name_record_by_primary_address(
        lookup_ans_primary_names,
        &registered_address,
        conn,
    ) {
        Some(ans_record) => Some(ans_record),
        None => {
            tracing::error!(
                transaction_version = txn_version,
                lookup_key = &registered_address,
                "Failed to get ans record for registered address. You probably should backfill db."
            );
            None
        },
    } {
        // Update the old primary name record with is_primary = false
        lookup_primary_name_record.is_primary = false;
        lookup_primary_name_record.last_transaction_version = txn_version;
        return Ok(Some((
            CurrentAnsPrimaryName {
                registered_address: registered_address.clone(),
                domain: lookup_primary_name_record.domain.clone(),
                subdomain: lookup_primary_name_record.subdomain.clone(),
                token_name: lookup_primary_name_record.token_name.clone(),
                is_primary: false,
                last_transaction_version: txn_version,
            },
            AnsPrimaryName {
                transaction_version: txn_version,
                wsc_index,
                registered_address: registered_address.clone(),
                domain: lookup_primary_name_record.domain.clone(),
                subdomain: lookup_primary_name_record.subdomain.clone(),
                token_name: lookup_primary_name_record.token_name.clone(),
                is_primary: false,
            },
        )));
    }

    Ok(None)
}

// Parse name record from write table item.
// The table key has the domain and subdomain.
// The table value data has the metadata (expiration, property version, target address).
pub fn parse_name_record_from_write_table_item_v1(
    write_table_item: &WriteTableItem,
    txn_version: i64,
    wsc_index: i64,
) -> anyhow::Result<(CurrentAnsLookup, AnsLookup)> {
    let table_item_data = write_table_item.data.as_ref().unwrap();
    let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
        .unwrap_or_else(|e| {
            tracing::error!(
                transaction_version = txn_version,
                e = ?e,
                "Failed to parse NameRecordKeyV1 from ANS name record write table item",
            );
            panic!();
        });
    let name_record: NameRecordV1 =
        serde_json::from_str(&table_item_data.value).unwrap_or_else(|e| {
            tracing::error!(
                transaction_version = txn_version,
                e = ?e,
                "Failed to parse NameRecordV1 from ANS name record write table item",
            );
            panic!();
        });
    Ok((
        CurrentAnsLookup {
            domain: name_record_key.get_domain(),
            subdomain: name_record_key.get_subdomain(),
            registered_address: name_record.get_target_address(),
            last_transaction_version: txn_version,
            expiration_timestamp: name_record.get_expiration_time(),
            token_name: name_record_key.get_token_name(),
            is_deleted: false,
        },
        AnsLookup {
            transaction_version: txn_version,
            wsc_index,
            domain: name_record_key.get_domain(),
            subdomain: name_record_key.get_subdomain(),
            registered_address: name_record.get_target_address(),
            expiration_timestamp: name_record.get_expiration_time(),
            token_name: name_record_key.get_token_name(),
            is_deleted: false,
        },
    ))
}

// Parse name record from delete table item.
// This change results in marking the domain name record as deleted and setting
// the rest of the fields to default values.
pub fn parse_name_record_from_delete_table_item_v1(
    delete_table_item: &DeleteTableItem,
    txn_version: i64,
    wsc_index: i64,
) -> anyhow::Result<(CurrentAnsLookup, AnsLookup)> {
    let table_item_data = delete_table_item.data.as_ref().unwrap();
    let name_record_key: NameRecordKeyV1 = serde_json::from_str(&table_item_data.key)
        .unwrap_or_else(|e| {
            tracing::error!(
                transaction_version = txn_version,
                e = ?e,
                "Failed to parse NameRecordKeyV1 from ANS name record delete table item",
            );
            panic!();
        });
    Ok((
        CurrentAnsLookup {
            domain: name_record_key.get_domain().clone(),
            subdomain: name_record_key.get_subdomain().clone(),
            registered_address: None,
            last_transaction_version: txn_version,
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: name_record_key.get_token_name(),
            is_deleted: true,
        },
        AnsLookup {
            transaction_version: txn_version,
            wsc_index,
            domain: name_record_key.get_domain(),
            subdomain: name_record_key.get_subdomain(),
            registered_address: None,
            // Default to zero
            expiration_timestamp: chrono::NaiveDateTime::from_timestamp_opt(0, 0)
                .unwrap_or_default(),
            token_name: name_record_key.get_token_name(),
            is_deleted: true,
        },
    ))
}

fn lookup_name_record_by_primary_address(
    lookup_ans_primary_names: &HashMap<Address, CurrentAnsPrimaryName>,
    registered_address: &str,
    conn: &mut PgPoolConnection,
) -> Option<CurrentAnsPrimaryName> {
    // Check if the current primary name record is in the current batch
    let maybe_primary_name_record = lookup_ans_primary_names.get(&registered_address.to_string());

    if let Some(primary_record) = maybe_primary_name_record {
        return Some(primary_record.clone());
    }

    // If the primary name record is not in the current batch, then it was committed in a different
    // transaction batch and we should lookup in DB.
    let mut retried = 0;
    while retried < QUERY_RETRIES {
        retried += 1;
        match CurrentAnsPrimaryNameDataQuery::get_name_record_by_primary_address(
            conn,
            registered_address,
        ) {
            Ok(ans_record) => {
                return Some(CurrentAnsPrimaryName {
                    registered_address: ans_record.registered_address,
                    domain: ans_record.domain,
                    subdomain: ans_record.subdomain,
                    token_name: ans_record.token_name,
                    is_primary: ans_record.is_primary,
                    last_transaction_version: ans_record.last_transaction_version,
                });
            },
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
            },
        }
    }
    None
}
