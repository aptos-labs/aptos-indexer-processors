// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::{
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::{
            token_claims::TokenV1Claimed,
            token_utils::{TokenDataIdType, TokenEvent},
        },
        token_v2_models::v2_token_utils::{TokenStandard, V2TokenEvent},
    },
    schema::token_activities_v2,
    utils::util::standardize_address,
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::Event;
use bigdecimal::{BigDecimal, One, ToPrimitive, Zero};
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenActivityV2 {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

/// A simplified TokenActivity (excluded common fields) to reduce code duplication
#[derive(Clone, Debug)]
pub struct TokenActivityHelperV1 {
    pub token_data_id_struct: TokenDataIdType,
    pub property_version: BigDecimal,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
}

/// A simplified TokenActivity (excluded common fields) to reduce code duplication
struct TokenActivityHelperV2 {
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub event_type: String,
}

impl TokenActivityV2 {
    pub async fn get_nft_v2_from_parsed_event(
        event: &Event,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        token_v2_metadata: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.clone();
        if let Some(token_event) =
            &V2TokenEvent::from_event(&event_type, event.data.as_str(), txn_version)?
        {
            let event_account_address =
                standardize_address(&event.key.as_ref().unwrap().account_address);
            // burn and mint events are attached to the collection. The rest should be attached to the token
            let token_data_id = match token_event {
                V2TokenEvent::MintEvent(inner) => inner.get_token_address(),
                V2TokenEvent::Mint(inner) => inner.get_token_address(),
                V2TokenEvent::BurnEvent(inner) => inner.get_token_address(),
                V2TokenEvent::Burn(inner) => inner.get_token_address(),
                V2TokenEvent::TransferEvent(inner) => inner.get_object_address(),
                _ => event_account_address.clone(),
            };

            if let Some(metadata) = token_v2_metadata.get(&token_data_id) {
                let object_core = &metadata.object.object_core;
                let token_activity_helper = match token_event {
                    V2TokenEvent::MintEvent(_) => TokenActivityHelperV2 {
                        from_address: Some(object_core.get_owner_address()),
                        to_address: None,
                        token_amount: BigDecimal::one(),
                        before_value: None,
                        after_value: None,
                        event_type: event_type.clone(),
                    },
                    V2TokenEvent::Mint(_) => TokenActivityHelperV2 {
                        from_address: Some(object_core.get_owner_address()),
                        to_address: None,
                        token_amount: BigDecimal::one(),
                        before_value: None,
                        after_value: None,
                        event_type: "0x4::collection::MintEvent".to_string(),
                    },
                    V2TokenEvent::TokenMutationEvent(inner) => TokenActivityHelperV2 {
                        from_address: Some(object_core.get_owner_address()),
                        to_address: None,
                        token_amount: BigDecimal::zero(),
                        before_value: Some(inner.old_value.clone()),
                        after_value: Some(inner.new_value.clone()),
                        event_type: event_type.clone(),
                    },
                    V2TokenEvent::TokenMutation(inner) => TokenActivityHelperV2 {
                        from_address: Some(inner.token_address.clone()),
                        to_address: None,
                        token_amount: BigDecimal::zero(),
                        before_value: Some(inner.old_value.clone()),
                        after_value: Some(inner.new_value.clone()),
                        event_type: "0x4::collection::MutationEvent".to_string(),
                    },
                    V2TokenEvent::BurnEvent(_) => TokenActivityHelperV2 {
                        from_address: Some(object_core.get_owner_address()),
                        to_address: None,
                        token_amount: BigDecimal::one(),
                        before_value: None,
                        after_value: None,
                        event_type: event_type.clone(),
                    },
                    V2TokenEvent::Burn(_) => TokenActivityHelperV2 {
                        from_address: Some(object_core.get_owner_address()),
                        to_address: None,
                        token_amount: BigDecimal::one(),
                        before_value: None,
                        after_value: None,
                        event_type: "0x4::collection::BurnEvent".to_string(),
                    },
                    V2TokenEvent::TransferEvent(inner) => TokenActivityHelperV2 {
                        from_address: Some(inner.get_from_address()),
                        to_address: Some(inner.get_to_address()),
                        token_amount: BigDecimal::one(),
                        before_value: None,
                        after_value: None,
                        event_type: event_type.clone(),
                    },
                };
                return Ok(Some(Self {
                    transaction_version: txn_version,
                    event_index,
                    event_account_address,
                    token_data_id,
                    property_version_v1: BigDecimal::zero(),
                    type_: token_activity_helper.event_type,
                    from_address: token_activity_helper.from_address,
                    to_address: token_activity_helper.to_address,
                    token_amount: token_activity_helper.token_amount,
                    before_value: token_activity_helper.before_value,
                    after_value: token_activity_helper.after_value,
                    entry_function_id_str: entry_function_id_str.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2: None,
                    transaction_timestamp: txn_timestamp,
                }));
            } else {
                // If the object metadata isn't found in the transaction, then the token was burnt.

                // the new burn event has owner address now!
                let owner_address = if let V2TokenEvent::Burn(inner) = token_event {
                    inner.get_previous_owner_address()
                } else {
                    // To handle a case with the old burn events, when a token is minted and burnt in the same transaction
                    None
                };

                return Ok(Some(Self {
                    transaction_version: txn_version,
                    event_index,
                    event_account_address,
                    token_data_id,
                    property_version_v1: BigDecimal::zero(),
                    type_: event_type,
                    from_address: owner_address.clone(),
                    to_address: None,
                    token_amount: BigDecimal::one(),
                    before_value: None,
                    after_value: None,
                    entry_function_id_str: entry_function_id_str.clone(),
                    token_standard: TokenStandard::V2.to_string(),
                    is_fungible_v2: None,
                    transaction_timestamp: txn_timestamp,
                }));
            }
        }
        Ok(None)
    }

    pub fn get_v1_from_parsed_event(
        event: &Event,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
        event_index: i64,
        entry_function_id_str: &Option<String>,
        tokens_claimed: &mut TokenV1Claimed,
    ) -> anyhow::Result<Option<Self>> {
        let event_type = event.type_str.clone();
        if let Some(token_event) = &TokenEvent::from_event(&event_type, &event.data, txn_version)? {
            let event_account_address =
                standardize_address(&event.key.as_ref().unwrap().account_address);
            let token_activity_helper = match token_event {
                TokenEvent::MintTokenEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.clone(),
                    property_version: BigDecimal::zero(),
                    from_address: Some(event_account_address.clone()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::Mint(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.clone(),
                    property_version: BigDecimal::zero(),
                    from_address: Some(inner.get_account()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::BurnTokenEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: Some(event_account_address.clone()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::Burn(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: Some(inner.get_account()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::MutateTokenPropertyMapEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.new_id.token_data_id.clone(),
                    property_version: inner.new_id.property_version.clone(),
                    from_address: Some(event_account_address.clone()),
                    to_address: None,
                    token_amount: BigDecimal::zero(),
                },
                TokenEvent::MutatePropertyMap(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.new_id.token_data_id.clone(),
                    property_version: inner.new_id.property_version.clone(),
                    from_address: Some(inner.get_account()),
                    to_address: None,
                    token_amount: BigDecimal::zero(),
                },
                TokenEvent::WithdrawTokenEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: Some(event_account_address.clone()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::TokenWithdraw(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: Some(inner.get_account()),
                    to_address: None,
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::DepositTokenEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: None,
                    to_address: Some(standardize_address(&event_account_address)),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::TokenDeposit(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.id.token_data_id.clone(),
                    property_version: inner.id.property_version.clone(),
                    from_address: None,
                    to_address: Some(inner.get_account()),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::OfferTokenEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.token_id.token_data_id.clone(),
                    property_version: inner.token_id.property_version.clone(),
                    from_address: Some(event_account_address.clone()),
                    to_address: Some(inner.get_to_address()),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::CancelTokenOfferEvent(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.token_id.token_data_id.clone(),
                    property_version: inner.token_id.property_version.clone(),
                    from_address: Some(event_account_address.clone()),
                    to_address: Some(inner.get_to_address()),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::ClaimTokenEvent(inner) => {
                    let token_data_id_struct = inner.token_id.token_data_id.clone();
                    let helper = TokenActivityHelperV1 {
                        token_data_id_struct: token_data_id_struct.clone(),
                        property_version: inner.token_id.property_version.clone(),
                        from_address: Some(event_account_address.clone()),
                        to_address: Some(inner.get_to_address()),
                        token_amount: inner.amount.clone(),
                    };
                    tokens_claimed.insert(token_data_id_struct.to_id(), helper.clone());
                    helper
                },
                TokenEvent::Offer(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.token_id.token_data_id.clone(),
                    property_version: inner.token_id.property_version.clone(),
                    from_address: Some(inner.get_from_address()),
                    to_address: Some(inner.get_to_address()),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::CancelOffer(inner) => TokenActivityHelperV1 {
                    token_data_id_struct: inner.token_id.token_data_id.clone(),
                    property_version: inner.token_id.property_version.clone(),
                    from_address: Some(inner.get_from_address()),
                    to_address: Some(inner.get_to_address()),
                    token_amount: inner.amount.clone(),
                },
                TokenEvent::Claim(inner) => {
                    let token_data_id_struct = inner.token_id.token_data_id.clone();
                    let helper = TokenActivityHelperV1 {
                        token_data_id_struct: token_data_id_struct.clone(),
                        property_version: inner.token_id.property_version.clone(),
                        from_address: Some(inner.get_from_address()),
                        to_address: Some(inner.get_to_address()),
                        token_amount: inner.amount.clone(),
                    };
                    tokens_claimed.insert(token_data_id_struct.to_id(), helper.clone());
                    helper
                },
            };
            let token_data_id_struct = token_activity_helper.token_data_id_struct;
            return Ok(Some(Self {
                transaction_version: txn_version,
                event_index,
                event_account_address,
                token_data_id: token_data_id_struct.to_id(),
                property_version_v1: token_activity_helper.property_version,
                type_: event_type,
                from_address: token_activity_helper.from_address,
                to_address: token_activity_helper.to_address,
                token_amount: token_activity_helper.token_amount,
                before_value: None,
                after_value: None,
                entry_function_id_str: entry_function_id_str.clone(),
                token_standard: TokenStandard::V1.to_string(),
                is_fungible_v2: None,
                transaction_timestamp: txn_timestamp,
            }));
        }
        Ok(None)
    }
}

/// This is a parquet version of TokenActivityV2
#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct ParquetTokenActivityV2 {
    pub txn_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: u64, // BigDecimal
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: String, // BigDecimal
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for ParquetTokenActivityV2 {
    const TABLE_NAME: &'static str = "token_activities_v2";
}

impl HasVersion for ParquetTokenActivityV2 {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for ParquetTokenActivityV2 {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl From<TokenActivityV2> for ParquetTokenActivityV2 {
    fn from(raw_item: TokenActivityV2) -> Self {
        Self {
            txn_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            event_account_address: raw_item.event_account_address,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1.to_u64().unwrap(),
            type_: raw_item.type_,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            token_amount: raw_item.token_amount.to_string(),
            before_value: raw_item.before_value,
            after_value: raw_item.after_value,
            entry_function_id_str: raw_item.entry_function_id_str,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            block_timestamp: raw_item.transaction_timestamp,
        }
    }
}

/// This is a postgres version of TokenActivityV2

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = token_activities_v2)]
pub struct PostgresTokenActivityV2 {
    pub transaction_version: i64,
    pub event_index: i64,
    pub event_account_address: String,
    pub token_data_id: String,
    pub property_version_v1: BigDecimal,
    pub type_: String,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_amount: BigDecimal,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
    pub entry_function_id_str: Option<String>,
    pub token_standard: String,
    pub is_fungible_v2: Option<bool>,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl From<TokenActivityV2> for PostgresTokenActivityV2 {
    fn from(raw_item: TokenActivityV2) -> Self {
        Self {
            transaction_version: raw_item.transaction_version,
            event_index: raw_item.event_index,
            event_account_address: raw_item.event_account_address,
            token_data_id: raw_item.token_data_id,
            property_version_v1: raw_item.property_version_v1,
            type_: raw_item.type_,
            from_address: raw_item.from_address,
            to_address: raw_item.to_address,
            token_amount: raw_item.token_amount,
            before_value: raw_item.before_value,
            after_value: raw_item.after_value,
            entry_function_id_str: raw_item.entry_function_id_str,
            token_standard: raw_item.token_standard,
            is_fungible_v2: raw_item.is_fungible_v2,
            transaction_timestamp: raw_item.transaction_timestamp,
        }
    }
}
