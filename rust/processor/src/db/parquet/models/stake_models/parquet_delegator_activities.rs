use crate::{
    bq_analytics::generic_parquet_processor::NamedTable,
    db::common::models::stake_models::delegator_activities::{
        RawDelegatedStakingActivity, RawDelegatedStakingActivityConvertible,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct DelegatedStakingActivity {
    pub transaction_version: i64,
    pub event_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub event_type: String,
    pub amount: String, // BigDecimal
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for DelegatedStakingActivity {
    const TABLE_NAME: &'static str = "delegated_staking_activities";
}

impl RawDelegatedStakingActivityConvertible for DelegatedStakingActivity {
    fn from_raw(raw: RawDelegatedStakingActivity) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            event_index: raw.event_index,
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            event_type: raw.event_type,
            amount: raw.amount.to_string(),
            block_timestamp: raw.block_timestamp,
        }
    }
}
