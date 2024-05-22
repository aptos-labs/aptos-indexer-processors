// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_token_utils::TokenStandard;
use crate::{
    models::token_models::token_utils::TokenWriteSet,
    schema::{current_token_royalty, token_royalty},
};
use aptos_protos::transaction::v1::WriteTableItem;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id, transaction_version))]
#[diesel(table_name = token_royalty)]
pub struct TokenRoyalty {
    pub transaction_version: i64,
    pub token_data_id: String,
    pub payee_address: String,
    pub royalty_points_numerator: BigDecimal,
    pub royalty_points_denominator: BigDecimal,
    pub token_standard: String,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(token_data_id, creator_address))]
#[diesel(table_name = current_token_royalty)]
pub struct CurrentTokenRoyalty {
    pub token_data_id: String,
    pub creator_address: String,
    pub payee_address: String,
    pub royalty_points_numerator: BigDecimal,
    pub royalty_points_denominator: BigDecimal,
    pub token_standard: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl TokenRoyalty {
    // Royalty for v2 token is more complicated and not supported yet. For token v2, royalty can be on the collection (default) or on
    // the token (override).
    pub fn get_v1_from_write_table_item(
        write_table_item: &WriteTableItem,
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<(Self, CurrentTokenRoyalty)>> {
        let table_item_data = write_table_item.data.as_ref().unwrap();

        let maybe_token_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            transaction_version,
        )? {
            Some(TokenWriteSet::TokenData(inner)) => Some(inner),
            _ => None,
        };

        if let Some(token_data) = maybe_token_data {
            let maybe_token_data_id = match TokenWriteSet::from_table_item_type(
                table_item_data.key_type.as_str(),
                &table_item_data.key,
                transaction_version,
            )? {
                Some(TokenWriteSet::TokenDataId(inner)) => Some(inner),
                _ => None,
            };
            if let Some(token_data_id_struct) = maybe_token_data_id {
                let token_data_id = token_data_id_struct.to_hash();
                let creator_address = token_data_id_struct.get_creator_address();
                let payee_address = token_data.royalty.get_payee_address();
                let royalty_points_numerator = token_data.royalty.royalty_points_numerator.clone();
                let royalty_points_denominator =
                    token_data.royalty.royalty_points_denominator.clone();

                return Ok(Some((
                    TokenRoyalty {
                        transaction_version,
                        token_data_id: token_data_id.clone(),
                        payee_address: payee_address.clone(),
                        royalty_points_numerator: royalty_points_numerator.clone(),
                        royalty_points_denominator: royalty_points_denominator.clone(),
                        token_standard: TokenStandard::V1.to_string(),
                    },
                    CurrentTokenRoyalty {
                        token_data_id,
                        creator_address,
                        payee_address,
                        royalty_points_numerator,
                        royalty_points_denominator,
                        token_standard: TokenStandard::V1.to_string(),
                        last_transaction_version: transaction_version,
                        last_transaction_timestamp: transaction_timestamp,
                    },
                )));
            } else {
                tracing::warn!(
                    transaction_version,
                    key_type = table_item_data.key_type,
                    key = table_item_data.key,
                    "Expecting token_data_id as key for value = token_data"
                );
            }
        }
        Ok(None)
    }
}
