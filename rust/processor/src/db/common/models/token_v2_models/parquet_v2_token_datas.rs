// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::token_utils::TokenWriteSet,
        token_v2_models::{
            v2_token_datas::CurrentTokenDataV2,
            v2_token_utils::{TokenStandard, TokenV2, TokenV2Burned},
        },
    },
    utils::util::standardize_address,
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::{DeleteResource, WriteResource, WriteTableItem};
use bigdecimal::ToPrimitive;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct TokenDataV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub largest_property_version_v1: Option<u64>,
    pub token_uri: String,
    pub token_properties: String,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub is_deleted_v2: Option<bool>,
}

impl NamedTable for TokenDataV2 {
    const TABLE_NAME: &'static str = "token_datas_v2";
}

impl HasVersion for TokenDataV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TokenDataV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl TokenDataV2 {
    // TODO: remove the useless_asref lint when new clippy nighly is released.
    #[allow(clippy::useless_asref)]
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) = &TokenV2::from_write_resource(write_resource, txn_version)? {
            let token_data_id = standardize_address(&write_resource.address.to_string());
            let mut token_name = inner.get_name_trunc();
            let is_fungible_v2;
            // Get token properties from 0x4::property_map::PropertyMap
            let mut token_properties = serde_json::Value::Null;
            if let Some(object_metadata) = object_metadatas.get(&token_data_id) {
                let fungible_asset_metadata = object_metadata.fungible_asset_metadata.as_ref();
                if fungible_asset_metadata.is_some() {
                    is_fungible_v2 = Some(true);
                } else {
                    is_fungible_v2 = Some(false);
                }
                token_properties = object_metadata
                    .property_map
                    .as_ref()
                    .map(|m| m.inner.clone())
                    .unwrap_or(token_properties);
                // In aggregator V2 name is now derived from a separate struct
                if let Some(token_identifier) = object_metadata.token_identifier.as_ref() {
                    token_name = token_identifier.get_name_trunc();
                }
            } else {
                // ObjectCore should not be missing, returning from entire function early
                return Ok(None);
            }

            let collection_id = inner.get_collection_address();
            let token_uri = inner.get_uri_trunc();

            Ok(Some(Self {
                txn_version,
                write_set_change_index,
                token_data_id: token_data_id.clone(),
                collection_id: collection_id.clone(),
                token_name: token_name.clone(),
                largest_property_version_v1: None,
                token_uri: token_uri.clone(),
                token_properties: canonical_json::to_string(&token_properties.clone())
                    .context("Failed to serialize token properties")?,
                description: inner.description.clone(),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2,
                block_timestamp: txn_timestamp,
                is_deleted_v2: None,
            }))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where token is burned but objectCore is still there
    pub async fn get_burned_nft_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        tokens_burned: &TokenV2Burned,
    ) -> anyhow::Result<Option<CurrentTokenDataV2>> {
        let token_data_id = standardize_address(&write_resource.address.to_string());
        // reminder that v1 events won't get to this codepath
        if let Some(burn_event_v2) = tokens_burned.get(&standardize_address(&token_data_id)) {
            Ok(Some(CurrentTokenDataV2 {
                token_data_id,
                collection_id: burn_event_v2.get_collection_address(),
                token_name: "".to_string(),
                maximum: None,
                supply: None,
                largest_property_version_v1: None,
                token_uri: "".to_string(),
                token_properties: serde_json::Value::Null,
                description: "".to_string(),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: Some(false),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                decimals: None,
                is_deleted_v2: Some(true),
            }))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where token is burned and objectCore is deleted
    pub async fn get_burned_nft_v2_from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        tokens_burned: &TokenV2Burned,
    ) -> anyhow::Result<Option<CurrentTokenDataV2>> {
        let token_data_id = standardize_address(&delete_resource.address.to_string());
        // reminder that v1 events won't get to this codepath
        if let Some(burn_event_v2) = tokens_burned.get(&standardize_address(&token_data_id)) {
            Ok(Some(CurrentTokenDataV2 {
                token_data_id,
                collection_id: burn_event_v2.get_collection_address(),
                token_name: "".to_string(),
                maximum: None,
                supply: None,
                largest_property_version_v1: None,
                token_uri: "".to_string(),
                token_properties: serde_json::Value::Null,
                description: "".to_string(),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: Some(false),
                last_transaction_version: txn_version,
                last_transaction_timestamp: txn_timestamp,
                decimals: None,
                is_deleted_v2: Some(true),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_token_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenData(inner)) => Some(inner),
            _ => None,
        };

        if let Some(token_data) = maybe_token_data {
            let maybe_token_data_id = match TokenWriteSet::from_table_item_type(
                table_item_data.key_type.as_str(),
                &table_item_data.key,
                txn_version,
            )? {
                Some(TokenWriteSet::TokenDataId(inner)) => Some(inner),
                _ => None,
            };
            if let Some(token_data_id_struct) = maybe_token_data_id {
                let collection_id = token_data_id_struct.get_collection_id();
                let token_data_id = token_data_id_struct.to_id();
                let token_name = token_data_id_struct.get_name_trunc();
                let token_uri = token_data.get_uri_trunc();

                return Ok(Some(Self {
                    txn_version,
                    write_set_change_index,
                    token_data_id: token_data_id.clone(),
                    collection_id: collection_id.clone(),
                    token_name: token_name.clone(),
                    largest_property_version_v1: Some(
                        token_data
                            .largest_property_version
                            .clone()
                            .to_u64()
                            .unwrap(),
                    ),
                    token_uri: token_uri.clone(),
                    token_properties: canonical_json::to_string(
                        &token_data.default_properties.clone(),
                    )
                    .context("Failed to serialize token properties")?,
                    description: token_data.description.clone(),
                    token_standard: TokenStandard::V1.to_string(),
                    is_fungible_v2: None,
                    block_timestamp: txn_timestamp,
                    is_deleted_v2: None,
                }));
            } else {
                tracing::warn!(
                    transaction_version = txn_version,
                    key_type = table_item_data.key_type,
                    key = table_item_data.key,
                    "Expecting token_data_id as key for value = token_data"
                );
            }
        }
        Ok(None)
    }
}
