// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    v2_token_datas::TokenDataV2,
    v2_token_utils::{TokenStandard, TokenV2Burned},
};
use crate::{
    db::common::models::{
        default_models::move_resources::MoveResource,
        fungible_asset_models::v2_fungible_asset_utils::V2FungibleAssetResource,
        object_models::v2_object_utils::{ObjectAggregatedDataMapping, ObjectWithMetadata},
        token_models::{token_utils::TokenWriteSet, tokens::TableHandleToOwner},
        token_v2_models::v2_token_utils::DEFAULT_OWNER_ADDRESS,
    },
    schema::{current_token_ownerships_v2, token_ownerships_v2},
    utils::{
        database::DbPoolConnection,
        util::{ensure_not_negative, standardize_address},
    },
};
use ahash::AHashMap;
use anyhow::Context;
use aptos_protos::transaction::v1::{
    DeleteResource, DeleteTableItem, WriteResource, WriteTableItem,
};
use bigdecimal::{BigDecimal, One, Zero};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// PK of current_token_ownerships_v2, i.e. token_data_id, property_version_v1, owner_address, storage_id
pub type CurrentTokenOwnershipV2PK = (String, BigDecimal, String, String);

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = token_ownerships_v2)]
pub struct TokenOwnershipV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub owner_address: Option<String>,
    pub storage_id: String,
    pub amount: BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id, property_version_v1, owner_address, storage_id))]
#[diesel(table_name = current_token_ownerships_v2)]
pub struct CurrentTokenOwnershipV2 {
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub owner_address: String,
    pub storage_id: String,
    pub amount: BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

// Facilitate tracking when a token is burned
#[derive(Clone, Debug)]
pub struct NFTOwnershipV2 {
    pub token_data_id: String,
    pub owner_address: String,
    pub is_soulbound: Option<bool>,
}

/// Need a separate struct for queryable because we don't want to define the inserted_at column (letting DB fill)
#[derive(Clone, Debug, Identifiable, Queryable)]
#[diesel(primary_key(token_data_id, property_version_v1, owner_address, storage_id))]
#[diesel(table_name = current_token_ownerships_v2)]
pub struct CurrentTokenOwnershipV2Query {
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub owner_address: String,
    pub storage_id: String,
    pub amount: BigDecimal,
    pub table_type_v1: Option<String>,
    pub token_properties_mutated_v1: Option<serde_json::Value>,
    pub is_soulbound_v2: Option<bool>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
    pub non_transferrable_by_owner: Option<bool>,
}

impl TokenOwnershipV2 {
    /// For nfts it's the same resources that we parse tokendatas from so we leverage the work done in there to get ownership data
    /// Vecs are returned because there could be multiple transfers and we need to document each one here.
    pub fn get_nft_v2_from_token_data(
        token_data: &TokenDataV2,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<(
        Vec<Self>,
        AHashMap<CurrentTokenOwnershipV2PK, CurrentTokenOwnershipV2>,
    )> {
        // We should be indexing v1 token or v2 fungible token here
        if token_data.is_fungible_v2 != Some(false) {
            return Ok((vec![], AHashMap::new()));
        }
        let mut ownerships = vec![];
        let mut current_ownerships = AHashMap::new();

        let object_data = object_metadatas
            .get(&token_data.token_data_id)
            .context("If token data exists objectcore must exist")?;
        let object_core = object_data.object.object_core.clone();
        let token_data_id = token_data.token_data_id.clone();
        let owner_address = object_core.get_owner_address();
        let storage_id = token_data_id.clone();
        let is_soulbound = !object_core.allow_ungated_transfer;

        ownerships.push(Self {
            transaction_version: token_data.transaction_version,
            write_set_change_index: token_data.write_set_change_index,
            token_data_id: token_data_id.clone(),
            property_version_v1: BigDecimal::zero(),
            owner_address: Some(owner_address.clone()),
            storage_id: storage_id.clone(),
            amount: BigDecimal::one(),
            table_type_v1: None,
            token_properties_mutated_v1: None,
            is_soulbound_v2: Some(is_soulbound),
            token_standard: TokenStandard::V2.to_string(),
            is_fungible_v2: token_data.is_fungible_v2,
            transaction_timestamp: token_data.transaction_timestamp,
            non_transferrable_by_owner: Some(is_soulbound),
        });
        current_ownerships.insert(
            (
                token_data_id.clone(),
                BigDecimal::zero(),
                owner_address.clone(),
                storage_id.clone(),
            ),
            CurrentTokenOwnershipV2 {
                token_data_id: token_data_id.clone(),
                property_version_v1: BigDecimal::zero(),
                owner_address,
                storage_id: storage_id.clone(),
                amount: BigDecimal::one(),
                table_type_v1: None,
                token_properties_mutated_v1: None,
                is_soulbound_v2: Some(is_soulbound),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: token_data.is_fungible_v2,
                last_transaction_version: token_data.transaction_version,
                last_transaction_timestamp: token_data.transaction_timestamp,
                non_transferrable_by_owner: Some(is_soulbound),
            },
        );

        // check if token was transferred
        for (event_index, transfer_event) in &object_data.transfer_events {
            // If it's a self transfer then skip
            if transfer_event.get_to_address() == transfer_event.get_from_address() {
                continue;
            }
            ownerships.push(Self {
                transaction_version: token_data.transaction_version,
                // set to negative of event index to avoid collison with write set index
                write_set_change_index: -1 * event_index,
                token_data_id: token_data_id.clone(),
                property_version_v1: BigDecimal::zero(),
                // previous owner
                owner_address: Some(transfer_event.get_from_address()),
                storage_id: storage_id.clone(),
                // soft delete
                amount: BigDecimal::zero(),
                table_type_v1: None,
                token_properties_mutated_v1: None,
                is_soulbound_v2: Some(is_soulbound),
                token_standard: TokenStandard::V2.to_string(),
                is_fungible_v2: token_data.is_fungible_v2,
                transaction_timestamp: token_data.transaction_timestamp,
                non_transferrable_by_owner: Some(is_soulbound),
            });
            current_ownerships.insert(
                (
                    token_data_id.clone(),
                    BigDecimal::zero(),
                    transfer_event.get_from_address(),
                    storage_id.clone(),
                ),
                CurrentTokenOwnershipV2 {
                    token_data_id: token_data_id.clone(),
                    property_version_v1: BigDecimal::zero(),
                    // previous owner
                    owner_address: transfer_event.get_from_address(),
                    storage_id: storage_id.clone(),
                    // soft delete
                    amount: BigDecimal::zero(),
                    table_type_v1: None,
                    token_properties_mutated_v1: None,
                    is_soulbound_v2: Some(is_soulbound),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2: token_data.is_fungible_v2,
                    last_transaction_version: token_data.transaction_version,
                    last_transaction_timestamp: token_data.transaction_timestamp,
                    non_transferrable_by_owner: Some(is_soulbound),
                },
            );
        }
        Ok((ownerships, current_ownerships))
    }

    /// This handles the case where token is burned but objectCore is still there
    pub async fn get_burned_nft_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        prior_nft_ownership: &AHashMap<String, NFTOwnershipV2>,
        tokens_burned: &TokenV2Burned,
        conn: &mut DbPoolConnection<'_>,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<Option<(Self, CurrentTokenOwnershipV2)>> {
        let token_data_id = standardize_address(&write_resource.address.to_string());
        if tokens_burned
            .get(&standardize_address(&token_data_id))
            .is_some()
        {
            if let Some(object) =
                &ObjectWithMetadata::from_write_resource(write_resource, txn_version)?
            {
                let object_core = &object.object_core;
                let owner_address = object_core.get_owner_address();
                let storage_id = token_data_id.clone();
                let is_soulbound = !object_core.allow_ungated_transfer;

                return Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        token_data_id: token_data_id.clone(),
                        property_version_v1: BigDecimal::zero(),
                        owner_address: Some(owner_address.clone()),
                        storage_id: storage_id.clone(),
                        amount: BigDecimal::zero(),
                        table_type_v1: None,
                        token_properties_mutated_v1: None,
                        is_soulbound_v2: Some(is_soulbound),
                        token_standard: TokenStandard::V2.to_string(),
                        is_fungible_v2: Some(false),
                        transaction_timestamp: txn_timestamp,
                        non_transferrable_by_owner: Some(is_soulbound),
                    },
                    CurrentTokenOwnershipV2 {
                        token_data_id,
                        property_version_v1: BigDecimal::zero(),
                        owner_address,
                        storage_id,
                        amount: BigDecimal::zero(),
                        table_type_v1: None,
                        token_properties_mutated_v1: None,
                        is_soulbound_v2: Some(is_soulbound),
                        token_standard: TokenStandard::V2.to_string(),
                        is_fungible_v2: Some(false),
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        non_transferrable_by_owner: Some(is_soulbound),
                    },
                )));
            } else {
                return Self::get_burned_nft_v2_helper(
                    &token_data_id,
                    txn_version,
                    write_set_change_index,
                    txn_timestamp,
                    prior_nft_ownership,
                    tokens_burned,
                    conn,
                    query_retries,
                    query_retry_delay_ms,
                )
                .await;
            }
        }
        Ok(None)
    }

    /// This handles the case where token is burned and objectCore is deleted
    pub async fn get_burned_nft_v2_from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        prior_nft_ownership: &AHashMap<String, NFTOwnershipV2>,
        tokens_burned: &TokenV2Burned,
        conn: &mut DbPoolConnection<'_>,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<Option<(Self, CurrentTokenOwnershipV2)>> {
        let token_address = standardize_address(&delete_resource.address.to_string());
        Self::get_burned_nft_v2_helper(
            &token_address,
            txn_version,
            write_set_change_index,
            txn_timestamp,
            prior_nft_ownership,
            tokens_burned,
            conn,
            query_retries,
            query_retry_delay_ms,
        )
        .await
    }

    async fn get_burned_nft_v2_helper(
        token_address: &str,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        prior_nft_ownership: &AHashMap<String, NFTOwnershipV2>,
        tokens_burned: &TokenV2Burned,
        conn: &mut DbPoolConnection<'_>,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<Option<(Self, CurrentTokenOwnershipV2)>> {
        let token_address = standardize_address(token_address);
        if let Some(burn_event) = tokens_burned.get(&token_address) {
            // 1. Try to lookup token address in burn event mapping
            let previous_owner = if let Some(burn_event) = burn_event {
                burn_event.get_previous_owner_address()
            } else {
                // 2. If it doesn't exist in burn event mapping, then it must be an old burn event that doesn't contain previous_owner.
                // Do a lookup to get previous owner. This is necessary because previous owner is part of current token ownerships primary key.
                match prior_nft_ownership.get(&token_address) {
                    Some(inner) => inner.owner_address.clone(),
                    None => {
                        match CurrentTokenOwnershipV2Query::get_latest_owned_nft_by_token_data_id(
                            conn,
                            &token_address,
                            query_retries,
                            query_retry_delay_ms,
                        )
                        .await
                        {
                            Ok(nft) => nft.owner_address.clone(),
                            Err(_) => {
                                tracing::error!(
                                    transaction_version = txn_version,
                                    lookup_key = &token_address,
                                    "Failed to find current_token_ownership_v2 for burned token. You probably should backfill db."
                                );
                                DEFAULT_OWNER_ADDRESS.to_string()
                            },
                        }
                    },
                }
            };

            let token_data_id = token_address.clone();
            let storage_id = token_data_id.clone();

            return Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    token_data_id: token_data_id.clone(),
                    property_version_v1: BigDecimal::zero(),
                    owner_address: Some(previous_owner.clone()),
                    storage_id: storage_id.clone(),
                    amount: BigDecimal::zero(),
                    table_type_v1: None,
                    token_properties_mutated_v1: None,
                    is_soulbound_v2: None, // default
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2: None, // default
                    transaction_timestamp: txn_timestamp,
                    non_transferrable_by_owner: None, // default
                },
                CurrentTokenOwnershipV2 {
                    token_data_id,
                    property_version_v1: BigDecimal::zero(),
                    owner_address: previous_owner,
                    storage_id,
                    amount: BigDecimal::zero(),
                    table_type_v1: None,
                    token_properties_mutated_v1: None,
                    is_soulbound_v2: None, // default
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2: None, // default
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                    non_transferrable_by_owner: None, // default
                },
            )));
        }
        Ok(None)
    }

    // Getting this from 0x1::fungible_asset::FungibleStore
    pub async fn get_ft_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
        conn: &mut DbPoolConnection<'_>,
    ) -> anyhow::Result<Option<(Self, CurrentTokenOwnershipV2)>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !V2FungibleAssetResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let V2FungibleAssetResource::FungibleAssetStore(inner) =
            V2FungibleAssetResource::from_resource(
                &type_str,
                resource.data.as_ref().unwrap(),
                txn_version,
            )?
        {
            if let Some(object_data) = object_metadatas.get(&resource.address) {
                let object_core = &object_data.object.object_core;
                let token_data_id = inner.metadata.get_reference_address();
                // Exit early if it's not a token
                if !TokenDataV2::is_address_token(
                    conn,
                    &token_data_id,
                    object_metadatas,
                    txn_version,
                )
                .await
                {
                    return Ok(None);
                }
                let storage_id = resource.address.clone();
                let is_soulbound = inner.frozen;
                let amount = inner.balance;
                let owner_address = object_core.get_owner_address();

                return Ok(Some((
                    Self {
                        transaction_version: txn_version,
                        write_set_change_index,
                        token_data_id: token_data_id.clone(),
                        property_version_v1: BigDecimal::zero(),
                        owner_address: Some(owner_address.clone()),
                        storage_id: storage_id.clone(),
                        amount: amount.clone(),
                        table_type_v1: None,
                        token_properties_mutated_v1: None,
                        is_soulbound_v2: Some(is_soulbound),
                        token_standard: TokenStandard::V2.to_string(),
                        is_fungible_v2: Some(true),
                        transaction_timestamp: txn_timestamp,
                        non_transferrable_by_owner: Some(is_soulbound),
                    },
                    CurrentTokenOwnershipV2 {
                        token_data_id,
                        property_version_v1: BigDecimal::zero(),
                        owner_address,
                        storage_id,
                        amount,
                        table_type_v1: None,
                        token_properties_mutated_v1: None,
                        is_soulbound_v2: Some(is_soulbound),
                        token_standard: TokenStandard::V2.to_string(),
                        is_fungible_v2: Some(true),
                        last_transaction_version: txn_version,
                        last_transaction_timestamp: txn_timestamp,
                        non_transferrable_by_owner: Some(is_soulbound),
                    },
                )));
            }
        }
        Ok(None)
    }

    /// We want to track tokens in any offer/claims and tokenstore
    pub fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
    ) -> anyhow::Result<Option<(Self, Option<CurrentTokenOwnershipV2>)>> {
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
            let (curr_token_ownership, owner_address, table_type) = match maybe_table_metadata {
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
                            amount: amount.clone(),
                            table_type_v1: Some(tm.table_type.clone()),
                            token_properties_mutated_v1: Some(token.token_properties.clone()),
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

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    token_data_id,
                    property_version_v1: token_id_struct.property_version,
                    owner_address,
                    storage_id: table_handle,
                    amount,
                    table_type_v1: table_type,
                    token_properties_mutated_v1: Some(token.token_properties),
                    is_soulbound_v2: None,
                    token_standard: TokenStandard::V1.to_string(),
                    is_fungible_v2: None,
                    transaction_timestamp: txn_timestamp,
                    non_transferrable_by_owner: None,
                },
                curr_token_ownership,
            )))
        } else {
            Ok(None)
        }
    }

    /// We want to track tokens in any offer/claims and tokenstore
    pub fn get_v1_from_delete_table_item(
        table_item: &DeleteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
    ) -> anyhow::Result<Option<(Self, Option<CurrentTokenOwnershipV2>)>> {
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
            let (curr_token_ownership, owner_address, table_type) = match maybe_table_metadata {
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

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    token_data_id,
                    property_version_v1: token_id_struct.property_version,
                    owner_address,
                    storage_id: table_handle,
                    amount: BigDecimal::zero(),
                    table_type_v1: table_type,
                    token_properties_mutated_v1: None,
                    is_soulbound_v2: None,
                    token_standard: TokenStandard::V1.to_string(),
                    is_fungible_v2: None,
                    transaction_timestamp: txn_timestamp,
                    non_transferrable_by_owner: None,
                },
                curr_token_ownership,
            )))
        } else {
            Ok(None)
        }
    }
}

impl CurrentTokenOwnershipV2Query {
    pub async fn get_latest_owned_nft_by_token_data_id(
        conn: &mut DbPoolConnection<'_>,
        token_data_id: &str,
        query_retries: u32,
        query_retry_delay_ms: u64,
    ) -> anyhow::Result<NFTOwnershipV2> {
        let mut tried = 0;
        while tried < query_retries {
            tried += 1;
            match Self::get_latest_owned_nft_by_token_data_id_impl(conn, token_data_id).await {
                Ok(inner) => {
                    return Ok(NFTOwnershipV2 {
                        token_data_id: inner.token_data_id.clone(),
                        owner_address: inner.owner_address.clone(),
                        is_soulbound: inner.is_soulbound_v2,
                    });
                },
                Err(_) => {
                    if tried < query_retries {
                        tokio::time::sleep(std::time::Duration::from_millis(query_retry_delay_ms))
                            .await;
                    }
                },
            }
        }
        Err(anyhow::anyhow!(
            "Failed to get nft by token data id: {}",
            token_data_id
        ))
    }

    async fn get_latest_owned_nft_by_token_data_id_impl(
        conn: &mut DbPoolConnection<'_>,
        token_data_id: &str,
    ) -> diesel::QueryResult<Self> {
        current_token_ownerships_v2::table
            .filter(current_token_ownerships_v2::token_data_id.eq(token_data_id))
            .filter(current_token_ownerships_v2::amount.gt(BigDecimal::zero()))
            .first::<Self>(conn)
            .await
    }
}
