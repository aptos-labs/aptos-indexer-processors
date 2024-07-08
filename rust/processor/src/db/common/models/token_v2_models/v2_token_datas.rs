// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_token_utils::{TokenStandard, TokenV2, TokenV2Burned};
use crate::{
    db::common::models::{
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::token_utils::TokenWriteSet,
    },
    schema::{current_token_datas_v2, token_datas_v2},
    utils::util::standardize_address,
};
use aptos_protos::transaction::v1::{DeleteResource, WriteResource, WriteTableItem};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// PK of current_token_datas_v2, i.e. token_data_id
pub type CurrentTokenDataV2PK = String;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = token_datas_v2)]
pub struct TokenDataV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    // Here for consistency but we don't need to actually fill it
    // pub is_deleted_v2: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id))]
#[diesel(table_name = current_token_datas_v2)]
pub struct CurrentTokenDataV2 {
    pub token_data_id: String,
    pub collection_id: String,
    pub token_name: String,
    pub maximum: Option<BigDecimal>,
    pub supply: Option<BigDecimal>,
    pub largest_property_version_v1: Option<BigDecimal>,
    pub token_uri: String,
    pub token_properties: serde_json::Value,
    pub description: String,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    // Deprecated, but still here for backwards compatibility
    pub decimals: Option<i64>,
    pub is_deleted_v2: Option<bool>,
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
    ) -> anyhow::Result<Option<(Self, CurrentTokenDataV2)>> {
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

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    token_data_id: token_data_id.clone(),
                    collection_id: collection_id.clone(),
                    token_name: token_name.clone(),
                    maximum: None,
                    supply: None,
                    largest_property_version_v1: None,
                    token_uri: token_uri.clone(),
                    token_properties: token_properties.clone(),
                    description: inner.description.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2,
                    transaction_timestamp: txn_timestamp,
                    decimals: None,
                },
                CurrentTokenDataV2 {
                    token_data_id,
                    collection_id,
                    token_name,
                    maximum: None,
                    supply: None,
                    largest_property_version_v1: None,
                    token_uri,
                    token_properties,
                    description: inner.description.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2,
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    decimals: None,
                    is_deleted_v2: Some(false),
                },
            )))
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
    ) -> anyhow::Result<Option<(Self, CurrentTokenDataV2)>> {
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

                return Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        token_data_id: token_data_id.clone(),
                        collection_id: collection_id.clone(),
                        token_name: token_name.clone(),
                        maximum: Some(token_data.maximum.clone()),
                        supply: Some(token_data.supply.clone()),
                        largest_property_version_v1: Some(
                            token_data.largest_property_version.clone(),
                        ),
                        token_uri: token_uri.clone(),
                        token_properties: token_data.default_properties.clone(),
                        description: token_data.description.clone(),
                        token_standard: TokenStandard::V1.to_string(),
                        is_fungible_v2: None,
                        transaction_timestamp: txn_timestamp,
                        decimals: None,
                    },
                    CurrentTokenDataV2 {
                        token_data_id,
                        collection_id,
                        token_name,
                        maximum: Some(token_data.maximum),
                        supply: Some(token_data.supply),
                        largest_property_version_v1: Some(token_data.largest_property_version),
                        token_uri,
                        token_properties: token_data.default_properties,
                        description: token_data.description,
                        token_standard: TokenStandard::V1.to_string(),
                        is_fungible_v2: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        decimals: None,
                        is_deleted_v2: None,
                    },
                )));
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
