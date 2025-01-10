// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use bigdecimal::BigDecimal;
use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{signatures, user_transactions};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(
    transaction_version,
    multi_agent_index,
    multi_sig_index,
    is_sender_primary
))]
#[diesel(table_name = signatures)]
pub struct Signature {
    pub transaction_version: i64,
    pub multi_agent_index: i64,
    pub multi_sig_index: i64,
    pub transaction_block_height: i64,
    pub signer: String,
    pub is_sender_primary: bool,
    pub type_: String,
    pub public_key: String,
    pub signature: String,
    pub threshold: i64,
    pub public_key_indices: serde_json::Value,
    pub inserted_at: chrono::NaiveDateTime,
}

#[derive(Clone, Deserialize, Debug, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(version))]
#[diesel(table_name = user_transactions)]
pub struct UserTransaction {
    pub version: i64,
    pub block_height: i64,
    pub parent_signature_type: String,
    pub sender: String,
    pub sequence_number: i64,
    pub max_gas_amount: BigDecimal,
    pub expiration_timestamp_secs: chrono::NaiveDateTime,
    pub gas_unit_price: BigDecimal,
    pub timestamp: chrono::NaiveDateTime,
    pub entry_function_id_str: String,
    pub inserted_at: chrono::NaiveDateTime,
    pub epoch: i64,
    pub entry_function_contract_address: Option<String>,
    pub entry_function_module_name: Option<String>,
    pub entry_function_function_name: Option<String>,
}
