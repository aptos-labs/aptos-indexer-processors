// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::events,
    utils::util::{standardize_address, truncate_str},
};
use aptos_protos::transaction::v1::Event as EventPB;
use field_count::FieldCount;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// p99 currently is 303 so using 300 as a safe max length
const EVENT_TYPE_MAX_LENGTH: usize = 300;

#[derive(
    Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Eq, PartialEq,
)]
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

impl Event {
    pub fn from_event(
        event: &EventPB,
        transaction_version: i64,
        transaction_block_height: i64,
        event_index: i64,
    ) -> Self {
        let t: &str = event.type_str.as_ref();
        Event {
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            sequence_number: event.sequence_number as i64,
            transaction_version,
            transaction_block_height,
            type_: t.to_string(),
            data: serde_json::from_str(event.data.as_str()).unwrap(),
            event_index,
            indexed_type: truncate_str(t, EVENT_TYPE_MAX_LENGTH),
        }
    }

    pub fn from_events(
        events: &[EventPB],
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Vec<Self> {
        events
            .iter()
            .enumerate()
            .map(|(index, event)| {
                Self::from_event(
                    event,
                    transaction_version,
                    transaction_block_height,
                    index as i64,
                )
            })
            .collect::<Vec<EventModel>>()
    }
}

// Prevent conflicts with other things named `Event`
pub type EventModel = Event;

#[derive(Clone, Debug, Default, GetSize, Deserialize, Serialize, Eq, PartialEq)]
pub struct EventContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fa_asset_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fa_account_address: Option<String>,
}

impl EventContext {
    pub fn is_empty(&self) -> bool {
        self.coin_type.is_none()
            && self.fa_asset_type.is_none()
            && self.fa_account_address.is_none()
    }
}

#[derive(Clone, Debug, GetSize, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventStreamMessage {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    #[get_size(size_fn = get_serde_json_size_estimate)]
    pub data: serde_json::Value,
    pub event_index: i64,
    pub indexed_type: String,
    #[get_size(size = 12)]
    pub transaction_timestamp: chrono::NaiveDateTime,
    pub context: Option<EventContext>,
}

fn get_serde_json_size_estimate(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 8,
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(arr) => arr.iter().map(get_serde_json_size_estimate).sum(),
        serde_json::Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| k.len() + get_serde_json_size_estimate(v))
            .sum(),
    }
}

impl EventStreamMessage {
    pub fn from_event(
        event: &Event,
        context: Option<EventContext>,
        transaction_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        EventStreamMessage {
            account_address: event.account_address.clone(),
            creation_number: event.creation_number,
            sequence_number: event.sequence_number,
            transaction_version: event.transaction_version,
            transaction_block_height: event.transaction_block_height,
            type_: event.type_.clone(),
            data: event.data.clone(),
            event_index: event.event_index,
            indexed_type: event.indexed_type.clone(),
            transaction_timestamp,
            context: context.clone(),
        }
    }
}

#[derive(Clone, Debug, GetSize, Serialize, Deserialize, Eq, PartialEq)]
pub struct CachedEvents {
    pub transaction_version: i64,
    pub events: Vec<Arc<EventStreamMessage>>,
}

impl CachedEvents {
    pub fn from_event_stream_message(
        transaction_version: i64,
        event_stream_message: Vec<Arc<EventStreamMessage>>,
    ) -> Self {
        CachedEvents {
            transaction_version,
            events: event_stream_message.clone(),
        }
    }

    pub fn empty(transaction_version: i64) -> Self {
        CachedEvents {
            transaction_version,
            events: Vec::with_capacity(0),
        }
    }
}
