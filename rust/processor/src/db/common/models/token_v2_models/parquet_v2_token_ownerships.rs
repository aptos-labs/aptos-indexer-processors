// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        fungible_asset_models::parquet_v2_fungible_asset_balances::DEFAULT_AMOUNT_VALUE,
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::{token_utils::TokenWriteSet, tokens::TableHandleToOwner},
        token_v2_models::{
            parquet_v2_token_datas::TokenDataV2, v2_token_ownerships::CurrentTokenOwnershipV2,
            v2_token_utils::TokenStandard,
        },
    },
    utils::util::{ensure_not_negative, standardize_address},
};
use allocative_derive::Allocative;
use anyhow::Context;
use aptos_protos::transaction::v1::{DeleteTableItem, WriteTableItem};
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

const LEGACY_DEFAULT_PROPERTY_VERSION: u64 = 0;

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct TokenOwnershipV2 {
    pub txn_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub property_version_v1: u64,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub amount: String, // this is a string representation of a bigdecimal
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<String>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

impl NamedTable for TokenOwnershipV2 {
    const TABLE_NAME: &'static str = "token_ownerships_v2";
}

impl HasVersion for TokenOwnershipV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for TokenOwnershipV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl TokenOwnershipV2 {
    /// For nfts it's the same resources that we parse tokendatas from so we leverage the work done in there to get ownership data
    /// Vecs are returned because there could be multiple transfers and we need to document each one here.
    pub fn get_nft_v2_from_token_data(
        token_data: &TokenDataV2,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Vec<Self>> {
        let mut ownerships = vec![];
        // let mut current_ownerships = AHashMap::new();

        let object_data = object_metadatas
            .get(&token_data.token_data_id)
            .context("If token data exists objectcore must exist")?;
        let object_core = object_data.object.object_core.clone();
        let token_data_id = token_data.token_data_id.clone();
        let owner_address = object_core.get_owner_address();
        let storage_id = token_data_id.clone();

        // is_soulbound currently means if an object is completely untransferrable
        // OR if only admin can transfer. Only the former is true soulbound but
        // people might already be using it with the latter meaning so let's include both.
        let is_soulbound = if object_data.untransferable.as_ref().is_some() {
            true
        } else {
            !object_core.allow_ungated_transfer
        };
        let non_transferrable_by_owner = !object_core.allow_ungated_transfer;

        ownerships.push(Self {
            txn_version: token_data.txn_version,
            write_set_change_index: token_data.write_set_change_index,
            token_data_id: token_data_id.clone(),
            property_version_v1: LEGACY_DEFAULT_PROPERTY_VERSION,
            owner_address: Some(owner_address.clone()),
            storage_id: storage_id.clone(),
            amount: DEFAULT_AMOUNT_VALUE.clone(),
            table_type_v1: None,
            token_properties_mutated_v1: None,
            is_soulbound_v2: Some(is_soulbound),
            token_standard: TokenStandard::V2.to_string(),
            block_timestamp: token_data.block_timestamp,
            non_transferrable_by_owner: Some(non_transferrable_by_owner),
        });

        // check if token was transferred
        for (event_index, transfer_event) in &object_data.transfer_events {
            // If it's a self transfer then skip
            if transfer_event.get_to_address() == transfer_event.get_from_address() {
                continue;
            }
            ownerships.push(Self {
                txn_version: token_data.txn_version,
                // set to negative of event index to avoid collison with write set index
                write_set_change_index: -1 * event_index,
                token_data_id: token_data_id.clone(),
                property_version_v1: LEGACY_DEFAULT_PROPERTY_VERSION,
                // previous owner
                owner_address: Some(transfer_event.get_from_address()),
                storage_id: storage_id.clone(),
                // soft delete
                amount: DEFAULT_AMOUNT_VALUE.clone(),
                table_type_v1: None,
                token_properties_mutated_v1: None,
                is_soulbound_v2: Some(is_soulbound),
                token_standard: TokenStandard::V2.to_string(),
                block_timestamp: token_data.block_timestamp,
                non_transferrable_by_owner: Some(is_soulbound),
            });
        }
        Ok(ownerships)
    }

    /// We want to track tokens in any offer/claims and tokenstore
    pub fn get_v1_from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_token_id = match TokenWriteSet::from_table_item_type(
            table_item_data.key_type.as_str(),
            &table_item_data.key,
            txn_version,
        )? {
            Some(TokenWriteSet::TokenId(inner)) => Some(inner),
            _ => None,
        };

        if let Some(token_id_struct) = maybe_token_id {
            let table_handle = standardize_address(&table_item.handle.to_string());
            let token_data_id_struct = token_id_struct.token_data_id;
            let token_data_id = token_data_id_struct.to_id();

            let maybe_table_metadata = table_handle_to_owner.get(&table_handle);
            let (_, owner_address, table_type) = match maybe_table_metadata {
                Some(tm) => {
                    if tm.table_type != "0x3::token::TokenStore" {
                        return Ok(None);
                    }
                    let owner_address = tm.get_owner_address();
                    (
                        Some(CurrentTokenOwnershipV2 {
                            token_data_id: token_data_id.clone(),
                            property_version_v1: token_id_struct.property_version.clone(),
                            owner_address: owner_address.clone(),
                            storage_id: table_handle.clone(),
                            amount: BigDecimal::zero(),
                            table_type_v1: Some(tm.table_type.clone()),
                            token_properties_mutated_v1: None,
                            is_soulbound_v2: None,
                            token_standard: TokenStandard::V1.to_string(),
                            is_fungible_v2: None,
                            last_transaction_version: txn_version,
                            last_transaction_timestamp: txn_timestamp,
                            non_transferrable_by_owner: None,
                        }),
                        Some(owner_address),
                        Some(tm.table_type.clone()),
                    )
                },
                None => (None, None, None),
            };

            Ok(Some(Self {
                txn_version,
                write_set_change_index,
                token_data_id,
                property_version_v1: token_id_struct.property_version.to_u64().unwrap(),
                owner_address,
                storage_id: table_handle,
                amount: DEFAULT_AMOUNT_VALUE.clone(),
                table_type_v1: table_type,
                token_properties_mutated_v1: None,
                is_soulbound_v2: None,
                token_standard: TokenStandard::V1.to_string(),
                block_timestamp: txn_timestamp,
                non_transferrable_by_owner: None,
            }))
        } else {
            Ok(None)
        }
    }

    /// We want to track tokens in any offer/claims and tokenstore
    pub fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
    ) -> anyhow::Result<Option<Self>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_token = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::Token(inner)) => Some(inner),
            _ => None,
        };

        if let Some(token) = maybe_token {
            let table_handle = standardize_address(&table_item.handle.to_string());
            let amount = ensure_not_negative(token.amount);
            let token_id_struct = token.id;
            let token_data_id_struct = token_id_struct.token_data_id;
            let token_data_id = token_data_id_struct.to_id();

            let maybe_table_metadata = table_handle_to_owner.get(&table_handle);
            let (owner_address, table_type) = match maybe_table_metadata {
                Some(tm) => {
                    if tm.table_type != "0x3::token::TokenStore" {
                        return Ok(None);
                    }
                    let owner_address = tm.get_owner_address();
                    (Some(owner_address), Some(tm.table_type.clone()))
                },
                None => (None, None),
            };

            Ok(Some(Self {
                txn_version,
                write_set_change_index,
                token_data_id,
                property_version_v1: token_id_struct.property_version.to_u64().unwrap(),
                owner_address,
                storage_id: table_handle,
                amount: amount.to_string(),
                table_type_v1: table_type,
                token_properties_mutated_v1: Some(
                    canonical_json::to_string(&token.token_properties).unwrap(),
                ),
                is_soulbound_v2: None,
                token_standard: TokenStandard::V1.to_string(),
                block_timestamp: txn_timestamp,
                non_transferrable_by_owner: None,
            }))
        } else {
            Ok(None)
        }
    }
}
