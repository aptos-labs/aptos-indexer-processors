// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::utils::util::{
    bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
    truncate_str,
};
use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

pub const DOMAIN_LENGTH: usize = 64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NameRecordKeyV1 {
    domain_name: String,
    subdomain_name: OptionalString,
}

impl NameRecordKeyV1 {
    pub fn get_domain_trunc(&self) -> String {
        truncate_str(self.domain_name.as_str(), DOMAIN_LENGTH)
    }

    pub fn get_subdomain_trunc(&self) -> String {
        truncate_str(
            self.subdomain_name
                .get_string()
                .unwrap_or_default()
                .as_str(),
            DOMAIN_LENGTH,
        )
    }

    pub fn get_token_name(&self) -> String {
        let domain = self.get_domain_trunc();
        let subdomain = self.get_subdomain_trunc();
        let mut token_name = format!("{}.apt", &domain);
        if !subdomain.is_empty() {
            token_name = format!("{}.{}", &subdomain, token_name);
        }
        token_name
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NameRecordV1 {
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_sec: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    property_version: BigDecimal,
    target_address: OptionalString,
}

impl NameRecordV1 {
    pub fn get_expiration_time(&self) -> chrono::NaiveDateTime {
        parse_timestamp_secs(bigdecimal_to_u64(&self.expiration_time_sec), 0)
    }

    pub fn get_property_version(&self) -> u64 {
        bigdecimal_to_u64(&self.property_version)
    }

    pub fn get_target_address(&self) -> Option<String> {
        self.target_address
            .get_string()
            .map(|addr| standardize_address(&addr))
    }
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

pub enum AnsTableItem {
    NameRecordKeyV1(NameRecordKeyV1),
    NameRecordV1(NameRecordV1),
}

impl AnsTableItem {
    /// Matches based on the type name (last part of a full qualified type) instead of the fully qualified type
    /// because we already know what the table handle is
    pub fn from_table_item(
        data_type_name: &str,
        data: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        match data_type_name {
            "NameRecordKeyV1" => {
                serde_json::from_str(data).map(|inner| Some(Self::NameRecordKeyV1(inner)))
            },
            "NameRecordV1" => {
                serde_json::from_str(data).map(|inner| Some(Self::NameRecordV1(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version, data_type_name, data
        ))
    }
}
