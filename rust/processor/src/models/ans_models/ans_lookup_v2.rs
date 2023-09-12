// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::ans_utils::{NameRecordV2, RenewNameEvent, SetReverseLookupEvent};
use crate::{
    models::token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
    schema::{ans_lookup_v2, current_ans_lookup_v2},
    utils::database::PgPoolConnection,
};
use aptos_indexer_protos::transaction::v1::WriteResource;
use diesel::{prelude::*, ExpressionMethods};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type Domain = String;
type Subdomain = String;
// PK of current_ans_lookup, i.e. domain and subdomain name
type CurrentAnsLookupV2PK = (Domain, Subdomain);

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
#[diesel(table_name = current_ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub is_primary: bool,
}

/// Need a separate struct for queryable because we don't want to define the inserted_at column (letting DB fill)
#[derive(Debug, Identifiable, Queryable)]
#[diesel(primary_key(domain, subdomain))]
#[diesel(table_name = current_ans_lookup_v2)]
pub struct CurrentAnsLookupV2Query {
    pub domain: String,
    pub subdomain: String,
    pub token_name: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub is_primary: bool,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct AnsLookupV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub domain: String,
    pub subdomain: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub is_primary: bool,
}

impl CurrentAnsLookupV2Query {
    fn get_record_by_domain_in_db(
        conn: &mut PgPoolConnection,
        domain: &str,
        subdomain: &str,
    ) -> diesel::QueryResult<Self> {
        current_ans_lookup_v2::table
            .filter(current_ans_lookup_v2::domain.eq(domain))
            .filter(current_ans_lookup_v2::subdomain.eq(subdomain))
            .first::<Self>(conn)
    }
}

impl Ord for CurrentAnsLookupV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.domain
            .cmp(&other.domain)
            .then(self.subdomain.cmp(&other.subdomain))
    }
}

impl PartialOrd for CurrentAnsLookupV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentAnsLookupV2 {
    pub fn pk(&self) -> CurrentAnsLookupV2PK {
        (self.domain.clone(), self.subdomain.clone())
    }

    pub fn get_record_by_domain(
        conn: &mut PgPoolConnection,
        domain: &str,
        subdomain: &str,
        all_current_ans_lookups_v2: &HashMap<CurrentAnsLookupV2PK, CurrentAnsLookupV2>,
    ) -> anyhow::Result<Self> {
        // Check if the name record is in all_current_ans_lookups_v2
        let maybe_current_ans_lookup_v2 =
            all_current_ans_lookups_v2.get(&(domain.to_string(), subdomain.to_string()));
        if let Some(current_ans_lookup_v2) = maybe_current_ans_lookup_v2 {
            return Ok(current_ans_lookup_v2.clone());
        }

        // If the name record is not found, then it belongs to a different txn batch
        // and we should lookup in DB.
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match CurrentAnsLookupV2Query::get_record_by_domain_in_db(conn, domain, subdomain) {
                Ok(record) => {
                    return Ok(Self {
                        domain: record.domain,
                        subdomain: record.subdomain,
                        registered_address: record.registered_address,
                        expiration_timestamp: record.expiration_timestamp,
                        token_name: record.token_name,
                        last_transaction_version: record.last_transaction_version,
                        is_deleted: record.is_deleted,
                        is_primary: record.is_primary,
                    })
                },
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        Err(anyhow::anyhow!("Failed to get name record"))
    }

    pub fn parse_name_record_from_write_resource_v2(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
        write_set_change_index: i64,
        renew_name_events: &HashMap<CurrentAnsLookupV2PK, RenewNameEvent>,
    ) -> anyhow::Result<Option<(Self, AnsLookupV2)>> {
        if let Some(inner) =
            NameRecordV2::from_write_resource(write_resource, ans_v2_contract_address, txn_version)
                .unwrap()
        {
            // If domain has a RenewNameEvent, get is_primary from the event
            let is_primary = if let Some(event) =
                renew_name_events.get(&(inner.get_domain_trunc(), inner.get_subdomain_trunc()))
            {
                event.is_primary_name
            } else {
                false
            };
            return Ok(Some((
                Self {
                    domain: inner.get_domain_trunc(),
                    subdomain: inner.get_subdomain_trunc(),
                    registered_address: inner.get_target_address(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name: inner.get_token_name(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    is_primary,
                },
                AnsLookupV2 {
                    transaction_version: txn_version,
                    write_set_change_index,
                    domain: inner.get_domain_trunc().clone(),
                    subdomain: inner.get_subdomain_trunc().clone(),
                    registered_address: inner.get_target_address().clone(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name: inner.get_token_name(),
                    is_deleted: false,
                    is_primary,
                },
            )));
        }
        Ok(None)
    }

    pub fn parse_primary_name_record_from_set_reverse_lookup_event(
        conn: &mut PgPoolConnection,
        event: &SetReverseLookupEvent,
        txn_version: i64,
        event_index: i64,
        all_current_ans_lookups_v2: &HashMap<CurrentAnsLookupV2PK, CurrentAnsLookupV2>,
    ) -> anyhow::Result<(
        Option<(Self, AnsLookupV2)>,
        Option<(Self, AnsLookupV2)>, // If the address previously had a primary name, the previous record
    )> {
        let mut current_ans_lookup_v2 = None;
        let mut previous_ans_lookup_v2 = None;

        if !event.get_curr_domain_trunc().is_empty() {
            // Set is_primary in current primary name record
            current_ans_lookup_v2 = Some((
                Self {
                    domain: event.get_curr_domain_trunc(),
                    subdomain: event.get_curr_subdomain_trunc(),
                    registered_address: Some(event.get_account_addr()),
                    expiration_timestamp: event.get_curr_expiration_time().unwrap(),
                    token_name: event.get_curr_token_name(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    is_primary: true,
                },
                AnsLookupV2 {
                    transaction_version: txn_version,
                    // To avoid collision with other wsc and events in the same txn, because each event may have 2 records
                    write_set_change_index: -(event_index * 2 + 1),
                    domain: event.get_curr_domain_trunc(),
                    subdomain: event.get_curr_subdomain_trunc(),
                    registered_address: Some(event.get_account_addr()),
                    expiration_timestamp: event.get_curr_expiration_time().unwrap(),
                    token_name: event.get_curr_token_name(),
                    is_deleted: false,
                    is_primary: true,
                },
            ));
        }

        // If address had a previous primary name, we need to unset the is_primary in the previous primary name record
        if !event.get_prev_domain_trunc().is_empty() {
            // Do a lookup to get previous domain's record
            let previous_name_record = match Self::get_record_by_domain(
                conn,
                &event.get_prev_domain_trunc(),
                &event.get_prev_subdomain_trunc(),
                all_current_ans_lookups_v2,
            ) {
                Ok(record) => record,
                Err(_) => {
                    tracing::error!(
                        transaction_version = txn_version,
                        domain = &event.get_prev_domain_trunc(),
                        subdomain = &event.get_prev_subdomain_trunc(),
                        "Failed to get name record for domain and subdomain. You probably should backfill db."
                    );
                    return Ok((current_ans_lookup_v2, None));
                },
            };

            // Check if previous name record's registered_address is the same as the event's account_addr
            // Because we don't want to overwrite the data if the previous name is already set to a new address
            if let Some(registered_address) = previous_name_record.registered_address {
                if registered_address == event.get_account_addr() {
                    previous_ans_lookup_v2 = Some((
                        Self {
                            domain: previous_name_record.domain.clone(),
                            subdomain: previous_name_record.subdomain.clone(),
                            registered_address: Some(registered_address.clone()),
                            expiration_timestamp: previous_name_record.expiration_timestamp,
                            token_name: previous_name_record.token_name.clone(),
                            last_transaction_version: txn_version,
                            is_deleted: false,
                            is_primary: false,
                        },
                        AnsLookupV2 {
                            transaction_version: txn_version,
                            // To avoid collision with other wsc and events in the same txn, because each event may have 2 records
                            write_set_change_index: -(event_index * 2 + 2),
                            domain: previous_name_record.domain.clone(),
                            subdomain: previous_name_record.subdomain.clone(),
                            registered_address: Some(registered_address.clone()),
                            expiration_timestamp: previous_name_record.expiration_timestamp,
                            token_name: previous_name_record.token_name.clone(),
                            is_deleted: false,
                            is_primary: false,
                        },
                    ));
                }
            }
        }

        Ok((current_ans_lookup_v2, previous_ans_lookup_v2))
    }
}
