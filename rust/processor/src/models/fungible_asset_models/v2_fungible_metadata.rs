// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_fungible_asset_utils::FungibleAssetMetadata;
use crate::{
    models::{
        coin_models::coin_utils::{CoinInfoType, CoinResource},
        object_models::{v2_object_utils::ObjectAggregatedDataMapping, v2_objects::Object},
        token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_metadata,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use ahash::AHashMap;
use aptos_protos::transaction::v1::WriteResource;
use diesel::sql_types::Text;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// This is the asset type
pub type FungibleAssetMetadataPK = String;
pub type FungibleAssetMetadataMapping =
    AHashMap<FungibleAssetMetadataPK, FungibleAssetMetadataModel>;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(asset_type))]
#[diesel(table_name = fungible_asset_metadata)]
pub struct FungibleAssetMetadataModel {
    pub asset_type: String,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub icon_uri: Option<String>,
    pub project_uri: Option<String>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle_v1: Option<String>,
    pub supply_aggregator_table_key_v1: Option<String>,
    pub token_standard: String,
}

#[derive(Debug, QueryableByName)]
pub struct AssetTypeFromTable {
    #[diesel(sql_type = Text)]
    pub asset_type: String,
}

impl FungibleAssetMetadataModel {
    /// Fungible asset is part of an object and we need to get the object first to get owner address
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) =
            &FungibleAssetMetadata::from_write_resource(write_resource, txn_version)?
        {
            // the new coin type
            let asset_type = standardize_address(&write_resource.address.to_string());
            if let Some(object_metadata) = object_metadatas.get(&asset_type) {
                let object = &object_metadata.object.object_core;
                // Do not write here if asset is fungible token
                if object_metadata.token.is_some() {
                    return Ok(None);
                }

                return Ok(Some(Self {
                    asset_type: asset_type.clone(),
                    creator_address: object.get_owner_address(),
                    name: inner.get_name(),
                    symbol: inner.get_symbol(),
                    decimals: inner.decimals,
                    icon_uri: Some(inner.get_icon_uri()),
                    project_uri: Some(inner.get_project_uri()),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    supply_aggregator_table_handle_v1: None,
                    supply_aggregator_table_key_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                }));
            }
        }
        Ok(None)
    }

    /// We can find v1 coin info from resources
    pub fn get_v1_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_write_resource(write_resource, txn_version)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    write_resource.type_str.as_ref(),
                    txn_version,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));
                // If asset type is too long, just ignore
                if let Some(asset_type) = coin_info_type.get_coin_type_below_max() {
                    Ok(Some(Self {
                        asset_type,
                        creator_address: coin_info_type.get_creator_address(),
                        name: inner.get_name_trunc(),
                        symbol: inner.get_symbol_trunc(),
                        decimals: inner.decimals,
                        icon_uri: None,
                        project_uri: None,
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        supply_aggregator_table_handle_v1: supply_aggregator_table_handle,
                        supply_aggregator_table_key_v1: supply_aggregator_table_key,
                        token_standard: TokenStandard::V1.to_string(),
                    }))
                } else {
                    Ok(None)
                }
            },
            _ => Ok(None),
        }
    }

    /// A fungible asset can also be a token. We will make a best effort guess at whether this is a fungible token.
    /// 1. If metadata is present without token object, then it's not a token
    /// 2. If metadata is not present, we will do a lookup in the db.
    pub async fn is_address_fungible_asset(
        _: &mut PgPoolConnection<'_>,
        _: &str,
        _: &ObjectAggregatedDataMapping,
        _: i64,
    ) -> bool {
        // Always index for testing purposes
        return true;
        // // 1. If metadata is present without token object, then it's not a token
        // if let Some(object_data) = object_aggregated_data_mapping.get(address) {
        //     if object_data.fungible_asset_metadata.is_some() {
        //         return object_data.token.is_none();
        //     }
        // }
        // // 2. If metadata is not present, we will do a lookup in the db.
        // // The object must exist in current_objects table for this processor to proceed
        // // If it doesn't exist or is null, then you probably need to backfill objects processor
        // let object = Object::get_current_object(conn, address, txn_version).await;
        // if let (Some(is_fa), Some(is_token)) = (object.is_fungible_asset, object.is_token) {
        //     return is_fa && !is_token;
        // }
        // panic!("is_fungible_asset and/or is_token is null for object_address: {}. You should probably backfill db.", address);
    }
}
