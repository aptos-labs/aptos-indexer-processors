use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::stake_models::delegator_balances::{
        RawCurrentDelegatorBalance, RawCurrentDelegatorBalanceConvertible, RawDelegatorBalance,
        RawDelegatorBalanceConvertible,
    },
};
use allocative::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct CurrentDelegatorBalance {
    pub delegator_address: String,
    pub pool_address: String,
    pub pool_type: String,
    pub table_handle: String,
    pub last_transaction_version: i64,
    pub shares: String, // BigDecimal
    pub parent_table_handle: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl RawCurrentDelegatorBalanceConvertible for CurrentDelegatorBalance {
    fn from_raw(raw: RawCurrentDelegatorBalance) -> Self {
        Self {
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            pool_type: raw.pool_type,
            table_handle: raw.table_handle,
            last_transaction_version: raw.last_transaction_version,
            shares: raw.shares.to_string(),
            parent_table_handle: raw.parent_table_handle,
            block_timestamp: raw.block_timestamp,
        }
    }
}

impl HasVersion for CurrentDelegatorBalance {
    fn version(&self) -> i64 {
        self.last_transaction_version
    }
}

impl NamedTable for CurrentDelegatorBalance {
    const TABLE_NAME: &'static str = "current_delegator_balances";
}

impl GetTimeStamp for CurrentDelegatorBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct DelegatorBalance {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub delegator_address: String,
    pub pool_address: String,
    pub pool_type: String,
    pub table_handle: String,
    pub shares: String, // BigDecimal
    pub parent_table_handle: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for DelegatorBalance {
    const TABLE_NAME: &'static str = "delegator_balances";
}

impl HasVersion for DelegatorBalance {
    fn version(&self) -> i64 {
        self.transaction_version
    }
}

impl GetTimeStamp for DelegatorBalance {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl RawDelegatorBalanceConvertible for DelegatorBalance {
    fn from_raw(raw: RawDelegatorBalance) -> Self {
        Self {
            transaction_version: raw.transaction_version,
            write_set_change_index: raw.write_set_change_index,
            delegator_address: raw.delegator_address,
            pool_address: raw.pool_address,
            pool_type: raw.pool_type,
            table_handle: raw.table_handle,
            shares: raw.shares.to_string(),
            parent_table_handle: raw.parent_table_handle,
            block_timestamp: raw.block_timestamp,
        }
    }
}
