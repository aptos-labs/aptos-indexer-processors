// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{db::common::models::event_models::raw_events::RawEvent, schema::events};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, event_index))]
#[diesel(table_name = events)]
pub struct EventPG {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: serde_json::Value,
    pub event_index: i64,
    pub indexed_type: String,
}

impl From<RawEvent> for EventPG {
    fn from(raw_event: RawEvent) -> Self {
        EventPG {
            sequence_number: raw_event.sequence_number,
            creation_number: raw_event.creation_number,
            account_address: raw_event.account_address,
            transaction_version: raw_event.transaction_version,
            transaction_block_height: raw_event.transaction_block_height,
            type_: raw_event.type_,
            data: serde_json::from_str(&raw_event.data).unwrap(),
            event_index: raw_event.event_index,
            indexed_type: raw_event.indexed_type,
        }
    }
}
