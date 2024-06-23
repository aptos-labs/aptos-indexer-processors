// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::token_models::token_utils::TokenWriteSet, schema::current_token_royalty_v1,
};
use aptos_protos::transaction::v1::WriteTableItem;
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, PartialEq, Eq,
)]
#[diesel(primary_key(token_data_id))]
#[diesel(table_name = current_token_royalty_v1)]
pub struct CurrentTokenRoyaltyV1 {
    pub token_data_id: String,
    pub payee_address: String,
    pub royalty_points_numerator: BigDecimal,
    pub royalty_points_denominator: BigDecimal,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

impl Ord for CurrentTokenRoyaltyV1 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token_data_id.cmp(&other.token_data_id)
    }
}
impl PartialOrd for CurrentTokenRoyaltyV1 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CurrentTokenRoyaltyV1 {
    pub fn pk(&self) -> String {
        self.token_data_id.clone()
    }

    // Royalty for v2 token is more complicated and not supported yet. For token v2, royalty can be on the collection (default) or on
    // the token (override).
    pub fn get_v1_from_write_table_item(
        write_table_item: &WriteTableItem,
        transaction_version: i64,
        transaction_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
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
                // token data id is the 0x{hash} version of the creator, collection name, and token name
                let token_data_id = token_data_id_struct.to_id();
                let payee_address = token_data.royalty.get_payee_address();
                let royalty_points_numerator = token_data.royalty.royalty_points_numerator.clone();
                let royalty_points_denominator =
                    token_data.royalty.royalty_points_denominator.clone();

                return Ok(Some(Self {
                    token_data_id,
                    payee_address,
                    royalty_points_numerator,
                    royalty_points_denominator,
                    last_transaction_version: transaction_version,
                    last_transaction_timestamp: transaction_timestamp,
                }));
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
