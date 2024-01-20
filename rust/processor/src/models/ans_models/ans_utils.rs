// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    models::default_models::move_resources::MoveResource,
    utils::util::{
        bigdecimal_to_u64, deserialize_from_string, parse_timestamp_secs, standardize_address,
        truncate_str,
    },
};
use anyhow::Context;
use aptos_protos::transaction::v1::{Event, WriteResource};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

pub const DOMAIN_LENGTH: usize = 64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptionalString {
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
pub struct OptionalBigDecimal {
    vec: Vec<BigDecimalWrapper>,
}

impl OptionalBigDecimal {
    fn get_big_decimal(&self) -> Option<BigDecimal> {
        self.vec.first().map(|x| x.0.clone())
    }
}

pub fn get_token_name(domain_name: &str, subdomain_name: &str) -> String {
    let domain = truncate_str(domain_name, DOMAIN_LENGTH);
    let subdomain = truncate_str(subdomain_name, DOMAIN_LENGTH);
    let mut token_name = format!("{}.apt", &domain);
    if !subdomain.is_empty() {
        token_name = format!("{}.{}", &subdomain, token_name);
    }
    token_name
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BigDecimalWrapper(#[serde(deserialize_with = "deserialize_from_string")] pub BigDecimal);

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
        get_token_name(&domain, &subdomain)
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NameRecordV2 {
    domain_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_sec: BigDecimal,
    target_address: OptionalString,
}

impl NameRecordV2 {
    pub fn get_domain_trunc(&self) -> String {
        truncate_str(self.domain_name.as_str(), DOMAIN_LENGTH)
    }

    pub fn get_expiration_time(&self) -> chrono::NaiveDateTime {
        parse_timestamp_secs(bigdecimal_to_u64(&self.expiration_time_sec), 0)
    }

    pub fn get_target_address(&self) -> Option<String> {
        self.target_address
            .get_string()
            .map(|addr| standardize_address(&addr))
    }

    pub fn from_write_resource(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AnsWriteResource::NameRecordV2(inner)) = AnsWriteResource::from_write_resource(
            write_resource,
            ans_v2_contract_address,
            txn_version,
        )? {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubdomainExtV2 {
    subdomain_expiration_policy: BigDecimal,
    subdomain_name: String,
}

impl SubdomainExtV2 {
    pub fn get_subdomain_trunc(&self) -> String {
        truncate_str(self.subdomain_name.as_str(), DOMAIN_LENGTH)
    }

    pub fn from_write_resource(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(AnsWriteResource::SubdomainExtV2(inner)) =
            AnsWriteResource::from_write_resource(
                write_resource,
                ans_v2_contract_address,
                txn_version,
            )?
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

pub enum AnsWriteResource {
    NameRecordV2(NameRecordV2),
    SubdomainExtV2(SubdomainExtV2),
}

impl AnsWriteResource {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        let data = write_resource.data.as_str();

        match type_str.clone() {
            x if x == format!("{}::v2_1_domains::NameRecord", ans_v2_contract_address) => {
                serde_json::from_str(data).map(|inner| Some(Self::NameRecordV2(inner)))
            },
            x if x == format!("{}::v2_1_domains::SubdomainExt", ans_v2_contract_address) => {
                serde_json::from_str(data).map(|inner| Some(Self::SubdomainExtV2(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version,
            type_str.clone(),
            data
        ))
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RenewNameEvent {
    domain_name: String,
    #[serde(deserialize_with = "deserialize_from_string")]
    expiration_time_secs: BigDecimal,
    is_primary_name: bool,
    subdomain_name: OptionalString,
    target_address: OptionalString,
}

impl RenewNameEvent {
    pub fn from_event(
        event: &Event,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(V2AnsEvent::RenewNameEvent(inner)) =
            V2AnsEvent::from_event(event, ans_v2_contract_address, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetReverseLookupEvent {
    account_addr: String,
    curr_domain_name: OptionalString,
    curr_expiration_time_secs: OptionalBigDecimal,
    curr_subdomain_name: OptionalString,
    prev_domain_name: OptionalString,
    prev_expiration_time_secs: OptionalBigDecimal,
    prev_subdomain_name: OptionalString,
}

impl SetReverseLookupEvent {
    pub fn get_account_addr(&self) -> String {
        standardize_address(&self.account_addr)
    }

    pub fn get_curr_domain_trunc(&self) -> String {
        truncate_str(
            self.curr_domain_name
                .get_string()
                .unwrap_or_default()
                .as_str(),
            DOMAIN_LENGTH,
        )
    }

    pub fn get_curr_subdomain_trunc(&self) -> String {
        truncate_str(
            self.curr_subdomain_name
                .get_string()
                .unwrap_or_default()
                .as_str(),
            DOMAIN_LENGTH,
        )
    }

    pub fn get_curr_token_name(&self) -> String {
        let domain = self.get_curr_domain_trunc();
        let subdomain = self.get_curr_subdomain_trunc();
        get_token_name(&domain, &subdomain)
    }

    pub fn get_curr_expiration_time(&self) -> Option<chrono::NaiveDateTime> {
        self.curr_expiration_time_secs
            .get_big_decimal()
            .map(|x| parse_timestamp_secs(bigdecimal_to_u64(&x), 0))
    }

    pub fn get_prev_domain_trunc(&self) -> String {
        truncate_str(
            self.prev_domain_name
                .get_string()
                .unwrap_or_default()
                .as_str(),
            DOMAIN_LENGTH,
        )
    }

    pub fn get_prev_subdomain_trunc(&self) -> String {
        truncate_str(
            self.prev_subdomain_name
                .get_string()
                .unwrap_or_default()
                .as_str(),
            DOMAIN_LENGTH,
        )
    }

    pub fn get_prev_token_name(&self) -> String {
        let domain = self.get_prev_domain_trunc();
        let subdomain = self.get_prev_subdomain_trunc();
        get_token_name(&domain, &subdomain)
    }

    pub fn get_prev_expiration_time(&self) -> Option<chrono::NaiveDateTime> {
        self.prev_expiration_time_secs
            .get_big_decimal()
            .map(|x| parse_timestamp_secs(bigdecimal_to_u64(&x), 0))
    }

    pub fn from_event(
        event: &Event,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(V2AnsEvent::SetReverseLookupEvent(inner)) =
            V2AnsEvent::from_event(event, ans_v2_contract_address, txn_version).unwrap()
        {
            Ok(Some(inner))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum V2AnsEvent {
    SetReverseLookupEvent(SetReverseLookupEvent),
    RenewNameEvent(RenewNameEvent),
}

impl V2AnsEvent {
    pub fn is_event_supported(event_type: &str, ans_v2_contract_address: &str) -> bool {
        [
            format!(
                "{}::v2_1_domains::SetReverseLookupEvent",
                ans_v2_contract_address
            ),
            format!("{}::v2_1_domains::RenewNameEvent", ans_v2_contract_address),
        ]
        .contains(&event_type.to_string())
    }

    pub fn from_event(
        event: &Event,
        ans_v2_contract_address: &str,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str: String = event.type_str.clone();
        let data = event.data.as_str();

        if !Self::is_event_supported(type_str.as_str(), ans_v2_contract_address) {
            return Ok(None);
        }

        match type_str.clone() {
            x if x
                == format!(
                    "{}::v2_1_domains::SetReverseLookupEvent",
                    ans_v2_contract_address
                ) =>
            {
                serde_json::from_str(data).map(|inner| Some(Self::SetReverseLookupEvent(inner)))
            },
            x if x == format!("{}::v2_1_domains::RenewNameEvent", ans_v2_contract_address) => {
                serde_json::from_str(data).map(|inner| Some(Self::RenewNameEvent(inner)))
            },
            _ => Ok(None),
        }
        .context(format!(
            "version {} failed! failed to parse type {}, data {:?}",
            txn_version,
            type_str.clone(),
            data
        ))
    }
}
