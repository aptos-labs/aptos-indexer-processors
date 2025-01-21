// Copyright © Aptos Foundation

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::stake_models::delegator_activities::{
        RawDelegatedStakingActivity, RawDelegatedStakingActivityConvertible,
    },
    schema::delegated_staking_activities,
};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = delegated_staking_activities)]
pub struct DelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: BigDecimal,
}

impl RawDelegatedStakingActivityConvertible for DelegatedStakingActivity {
    fn from_raw(raw: RawDelegatedStakingActivity) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            event_index: raw.event_index,
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            event_type: raw.event_type,
            amount: raw.amount,
        }
    }
}
