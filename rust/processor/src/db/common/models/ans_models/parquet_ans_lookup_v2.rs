// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        ans_models::{
            ans_lookup::{AnsPrimaryName, CurrentAnsPrimaryName},
            ans_utils::SetReverseLookupEvent,
        },
        token_v2_models::v2_token_utils::TokenStandard,
    },
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::Event;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Default, Debug, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct AnsPrimaryNameV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for AnsPrimaryNameV2 {
    const TABLE_NAME: &'static str = "ans_primary_name_v2";
}

impl HasVersion for AnsPrimaryNameV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for AnsPrimaryNameV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CurrentAnsPrimaryNameV2 {
    pub registered_address: String,
    pub token_standard: String,
    pub domain: Option<String>,
    pub subdomain: Option<String>,
    pub token_name: Option<String>,
    pub is_deleted: bool,
    pub last_transaction_version: i64,
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
    pub fn get_v2_from_v1(
        v1_current_primary_name: CurrentAnsPrimaryName,
        v1_primary_name: AnsPrimaryName,
        block_timestamp: chrono::NaiveDateTime,
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
                txn_version: v1_primary_name.transaction_version,
                write_set_change_index: v1_primary_name.write_set_change_index,
                registered_address: v1_primary_name.registered_address,
                token_standard: TokenStandard::V1.to_string(),
                domain: v1_primary_name.domain,
                subdomain: v1_primary_name.subdomain,
                token_name: v1_primary_name.token_name,
                is_deleted: v1_primary_name.is_deleted,
                block_timestamp,
            },
        )
    }

    // Parse v2 primary name record from SetReverseLookupEvent
    pub fn parse_v2_primary_name_record_from_event(
        event: &Event,
        txn_version: i64,
        event_index: i64,
        ans_v2_contract_address: &str,
        block_timestamp: chrono::NaiveDateTime,
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
                        txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: None,
                        subdomain: None,
                        token_name: None,
                        is_deleted: true,
                        block_timestamp,
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
                        txn_version,
                        write_set_change_index: -(event_index + 1),
                        registered_address: set_reverse_lookup_event.get_account_addr().clone(),
                        token_standard: TokenStandard::V2.to_string(),
                        domain: Some(set_reverse_lookup_event.get_curr_domain_trunc()),
                        subdomain: Some(set_reverse_lookup_event.get_curr_subdomain_trunc()),
                        token_name: Some(set_reverse_lookup_event.get_curr_token_name()),
                        is_deleted: false,
                        block_timestamp,
                    },
                )));
            }
        }
        Ok(None)
    }
}
