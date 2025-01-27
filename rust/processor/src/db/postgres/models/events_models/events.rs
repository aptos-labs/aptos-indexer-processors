// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::{
        common::models::event_models::raw_events::RawEvent,
        postgres::postgres_convertible::PostgresConvertible,
    },
    schema::events,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
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
    pub event_index: i64,
    pub indexed_type: String,
}

impl PostgresConvertible for RawEvent {
    type PostgresModelType = Event;

    fn to_postgres(&self) -> Self::PostgresModelType {
        Event {
            sequence_number: self.sequence_number,
            creation_number: self.creation_number,
            account_address: self.account_address.clone(),
            transaction_version: self.transaction_version,
            transaction_block_height: self.transaction_block_height,
            type_: self.type_.clone(),
            data: serde_json::from_str(&self.data).unwrap(),
            event_index: self.event_index,
            indexed_type: self.indexed_type.clone(),
        }
    }
}

// Prevent conflicts with other things named `Event`
pub type EventModel = Event;
