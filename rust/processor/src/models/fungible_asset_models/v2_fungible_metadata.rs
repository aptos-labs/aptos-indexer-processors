// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_fungible_asset_utils::FungibleAssetMetadata;
use crate::{
    models::{
        coin_models::coin_utils::{CoinInfoType, CoinResource},
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
        token_v2_models::v2_token_utils::TokenStandard,
    },
    schema::fungible_asset_metadata,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use anyhow::Context;
use aptos_protos::transaction::v1::WriteResource;
use diesel::{prelude::*, sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// This is the asset type
pub type FungibleAssetMetadataPK = String;
pub type FungibleAssetMetadataMapping =
    HashMap<FungibleAssetMetadataPK, FungibleAssetMetadataModel>;

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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
        fungible_asset_metadata: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        if let Some(inner) =
            &FungibleAssetMetadata::from_write_resource(write_resource, txn_version)?
        {
            // the new coin type
            let asset_type = standardize_address(&write_resource.address.to_string());
            if let Some(metadata) = fungible_asset_metadata.get(&asset_type) {
                let object = &metadata.object.object_core;
                // Do not write here if asset is fungible token
                if metadata.token.is_some() {
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
    /// 2. If metadata is not present, we will do a lookup in the db. If it's not there, it's a token
    pub async fn is_address_fungible_asset(
        conn: &mut PgPoolConnection<'_>,
        address: &str,
        fungible_asset_metadata: &ObjectAggregatedDataMapping,
    ) -> bool {
        if let Some(metadata) = fungible_asset_metadata.get(address) {
            metadata.token.is_none()
        } else {
            // Look up in the db
            Self::query_is_address_fungible_asset(conn, address).await
        }
    }

    /// Try to see if an address is a fungible asset (not a token). We'll try a few times in case there is a race condition,
    /// and if we can't find after N times, we'll assume that it's not a fungible asset.
    /// TODO: An improvement is to combine this with is_address_token. To do this well we need
    /// a k-v store
    async fn query_is_address_fungible_asset(
        conn: &mut PgPoolConnection<'_>,
        address: &str,
    ) -> bool {
        let mut retried = 0;
        while retried < QUERY_RETRIES {
            retried += 1;
            match Self::get_by_asset_type(conn, address).await {
                Ok(_) => return true,
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                },
            }
        }
        false
    }

    /// TODO: Change this to a KV store
    pub async fn get_by_asset_type(
        conn: &mut PgPoolConnection<'_>,
        address: &str,
    ) -> anyhow::Result<String> {
        let mut res: Vec<Option<AssetTypeFromTable>> =
            sql_query("SELECT asset_type FROM fungible_asset_metadata WHERE asset_type = $1")
                .bind::<Text, _>(address)
                .get_results(conn)
                .await?;
        Ok(res
            .pop()
            .context("fungible asset metadata result empty")?
            .context("fungible asset metadata result null")?
            .asset_type)
    }
}
