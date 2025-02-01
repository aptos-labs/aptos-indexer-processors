// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::events;
use serde::{Deserialize, Serialize};
/**
* Event model
* this is created b/c there is inserated_at field which isn't defined in the Event struct, we can't just load the events directly without specifying the fields.
*/
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = events)]
pub struct Event {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: serde_json::Value,
    pub inserted_at: chrono::NaiveDateTime,
    pub event_index: i64,
    pub indexed_type: String,
}
