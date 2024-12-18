// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::{
        common::models::token_v2_models::v2_token_utils::TokenStandard,
        postgres::models::ans_models::{
            ans_lookup::{AnsLookup, CurrentAnsLookup},
            ans_utils::{get_token_name, NameRecordV2, SubdomainExtV2},
        },
    },
    utils::util::standardize_address,
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::WriteResource;
use serde::{Deserialize, Serialize};

type Domain = String;
type Subdomain = String;
pub type TokenStandardType = String;
// PK of current_ans_lookup_v2
type CurrentAnsLookupV2PK = (Domain, Subdomain, TokenStandardType);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawAnsLookupV2 {
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

pub trait AnsLookupV2Convertible {
    fn from_raw(raw_item: RawAnsLookupV2) -> Self;
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RawCurrentAnsLookupV2 {
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

impl Ord for RawCurrentAnsLookupV2 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.domain
            .cmp(&other.domain)
            .then(self.subdomain.cmp(&other.subdomain))
    }
}

impl PartialOrd for RawCurrentAnsLookupV2 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RawCurrentAnsLookupV2 {
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
    ) -> (Self, RawAnsLookupV2) {
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
            RawAnsLookupV2 {
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
    ) -> anyhow::Result<Option<(Self, RawAnsLookupV2)>> {
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
                RawAnsLookupV2 {
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

pub trait CurrentAnsLookupV2Convertible {
    fn from_raw(raw_item: RawCurrentAnsLookupV2) -> Self;
}
