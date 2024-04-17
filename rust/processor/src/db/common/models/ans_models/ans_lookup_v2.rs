// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
    ans_utils::{get_token_name, NameRecordV2, SetReverseLookupEvent, SubdomainExtV2},
};
use crate::{
    db::common::models::token_v2_models::v2_token_utils::TokenStandard,
    schema::{
        ans_lookup_v2, ans_primary_name_v2, current_ans_lookup_v2, current_ans_primary_name_v2,
    },
    utils::util::standardize_address,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::{Event, WriteResource};
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

type Domain = String;
type Subdomain = String;
type TokenStandardType = String;
type RegisteredAddress = String;
// PK of current_ans_lookup_v2
type CurrentAnsLookupV2PK = (Domain, Subdomain, TokenStandardType);
// PK of current_ans_primary_name
type CurrentAnsPrimaryNameV2PK = (RegisteredAddress, TokenStandardType);

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
#[diesel(primary_key(domain, subdomain, token_standard))]
#[diesel(table_name = current_ans_lookup_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsLookupV2 {
    pub domain: String,
    pub subdomain: String,
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub last_transaction_version: i64,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
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
    pub token_standard: String,
    pub registered_address: Option<String>,
    pub expiration_timestamp: chrono::NaiveDateTime,
    pub token_name: String,
    pub is_deleted: bool,
    pub subdomain_expiration_policy: Option<i64>,
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
#[diesel(primary_key(registered_address, token_standard))]
#[diesel(table_name = current_ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
}

#[derive(Clone, Default, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = ans_primary_name_v2)]
#[diesel(treat_none_as_null = true)]
pub struct AnsPrimaryNameV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
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
        (
            self.domain.clone(),
            self.subdomain.clone(),
            self.token_standard.clone(),
        )
    }

    pub fn get_v2_from_v1(
        v1_current_ans_lookup: CurrentAnsLookup,
        v1_ans_lookup: AnsLookup,
    ) -> (Self, AnsLookupV2) {
        (
            Self {
                domain: v1_current_ans_lookup.domain,
                subdomain: v1_current_ans_lookup.subdomain,
                token_standard: TokenStandard::V1.to_string(),
                registered_address: v1_current_ans_lookup.registered_address,
                last_transaction_version: v1_current_ans_lookup.last_transaction_version,
                expiration_timestamp: v1_current_ans_lookup.expiration_timestamp,
                token_name: v1_current_ans_lookup.token_name,
                is_deleted: v1_current_ans_lookup.is_deleted,
                subdomain_expiration_policy: None,
            },
            AnsLookupV2 {
                transaction_version: v1_ans_lookup.transaction_version,
                write_set_change_index: v1_ans_lookup.write_set_change_index,
                domain: v1_ans_lookup.domain,
                subdomain: v1_ans_lookup.subdomain,
                token_standard: TokenStandard::V1.to_string(),
                registered_address: v1_ans_lookup.registered_address,
                expiration_timestamp: v1_ans_lookup.expiration_timestamp,
                token_name: v1_ans_lookup.token_name,
                is_deleted: v1_ans_lookup.is_deleted,
                subdomain_expiration_policy: None,
            },
        )
    }

    pub fn parse_name_record_from_write_resource_v2(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
        write_set_change_index: i64,
        address_to_subdomain_ext: &AHashMap<String, SubdomainExtV2>,
    ) -> anyhow::Result<Option<(Self, AnsLookupV2)>> {
        if let Some(inner) =
            NameRecordV2::from_write_resource(write_resource, ans_v2_contract_address, txn_version)
                .unwrap()
        {
            // If this resource account has a SubdomainExt, then it's a subdomain
            let (subdomain_name, subdomain_expiration_policy) = match address_to_subdomain_ext
                .get(&standardize_address(write_resource.address.as_str()))
            {
                Some(s) => (s.get_subdomain_trunc(), Some(s.subdomain_expiration_policy)),
                None => ("".to_string(), None),
            };

            let token_name = get_token_name(
                inner.get_domain_trunc().as_str(),
                subdomain_name.clone().as_str(),
            );

            return Ok(Some((
                Self {
                    domain: inner.get_domain_trunc(),
                    subdomain: subdomain_name.clone().to_string(),
                    token_standard: TokenStandard::V2.to_string(),
                    registered_address: inner.get_target_address(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name: token_name.clone(),
                    last_transaction_version: txn_version,
                    is_deleted: false,
                    subdomain_expiration_policy,
                },
                AnsLookupV2 {
                    transaction_version: txn_version,
                    write_set_change_index,
                    domain: inner.get_domain_trunc().clone(),
                    subdomain: subdomain_name.clone().to_string(),
                    token_standard: TokenStandard::V2.to_string(),
                    registered_address: inner.get_target_address().clone(),
                    expiration_timestamp: inner.get_expiration_time(),
                    token_name,
                    is_deleted: false,
                    subdomain_expiration_policy,
                },
            )));
        }
        Ok(None)
    }
}

impl Ord for CurrentAnsPrimaryNameV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.registered_address.cmp(&other.registered_address)
    }
}

impl PartialOrd for CurrentAnsPrimaryNameV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentAnsPrimaryNameV2 {
    pub fn pk(&self) -> CurrentAnsPrimaryNameV2PK {
        (self.registered_address.clone(), self.token_standard.clone())
    }

    pub fn get_v2_from_v1(
        v1_current_primary_name: CurrentAnsPrimaryName,
        v1_primary_name: AnsPrimaryName,
    ) -> (Self, AnsPrimaryNameV2) {
        (
            Self {
                registered_address: v1_current_primary_name.registered_address,
                token_standard: TokenStandard::V1.to_string(),
                domain: v1_current_primary_name.domain,
                subdomain: v1_current_primary_name.subdomain,
                token_name: v1_current_primary_name.token_name,
                is_deleted: v1_current_primary_name.is_deleted,
                last_transaction_version: v1_current_primary_name.last_transaction_version,
            },
            AnsPrimaryNameV2 {
                transaction_version: v1_primary_name.transaction_version,
                write_set_change_index: v1_primary_name.write_set_change_index,
                registered_address: v1_primary_name.registered_address,
                token_standard: TokenStandard::V1.to_string(),
                domain: v1_primary_name.domain,
                subdomain: v1_primary_name.subdomain,
                token_name: v1_primary_name.token_name,
                is_deleted: v1_primary_name.is_deleted,
            },
        )
    }

    // Parse v2 primary name record from SetReverseLookupEvent
    pub fn parse_v2_primary_name_record_from_event(
        event: &Event,
        txn_version: i64,
        event_index: i64,
        ans_v2_contract_address: &str,
    ) -> anyhow::Result<Option<(Self, AnsPrimaryNameV2)>> {
        if let Some(set_reverse_lookup_event) =
            SetReverseLookupEvent::from_event(event, ans_v2_contract_address, txn_version).unwrap()
        {
            if set_reverse_lookup_event.get_curr_domain_trunc().is_empty() {
                // Handle case where the address's primary name is unset
                return Ok(Some((
                    Self {
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        last_transaction_version: txn_version,
                        is_deleted: true,
                    },
                    AnsPrimaryNameV2 {
                        transaction_version: txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        is_deleted: true,
                    },
                )));
            } else {
                // Handle case where the address is set to a new primary name
                return Ok(Some((
                    Self {
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: Some(set_reverse_lookup_event.get_curr_domain_trunc()),
                        subdomain: Some(set_reverse_lookup_event.get_curr_subdomain_trunc()),
                        token_name: Some(set_reverse_lookup_event.get_curr_token_name()),
                        last_transaction_version: txn_version,
                        is_deleted: false,
                    },
                    AnsPrimaryNameV2 {
                        transaction_version: txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: Some(set_reverse_lookup_event.get_curr_domain_trunc()),
                        subdomain: Some(set_reverse_lookup_event.get_curr_subdomain_trunc()),
                        token_name: Some(set_reverse_lookup_event.get_curr_token_name()),
                        is_deleted: false,
                    },
                )));
            }
        }
        Ok(None)
    }
}
