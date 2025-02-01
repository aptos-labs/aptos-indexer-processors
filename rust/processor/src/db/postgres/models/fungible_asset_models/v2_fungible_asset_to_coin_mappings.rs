// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::fungible_asset_models::raw_v2_fungible_asset_to_coin_mappings::{
        FungibleAssetToCoinMappingConvertible, RawFungibleAssetToCoinMapping,
    },
    schema::fungible_asset_to_coin_mappings,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(coin_type))]
#[diesel(table_name = fungible_asset_to_coin_mappings)]
pub struct FungibleAssetToCoinMapping {
    pub fungible_asset_metadata_address: String,
    pub coin_type: String,
    pub last_transaction_version: i64,
}

impl FungibleAssetToCoinMappingConvertible for FungibleAssetToCoinMapping {
    fn from_raw(raw: RawFungibleAssetToCoinMapping) -> Self {
        Self {
            fungible_asset_metadata_address: raw.fungible_asset_metadata_address,
            coin_type: raw.coin_type,
            last_transaction_version: raw.last_transaction_version,
        }
    }
}
