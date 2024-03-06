// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::events,
    utils::util::{standardize_address, truncate_str},
};
use aptos_protos::transaction::v1::Event as EventPB;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventStreamMessage {
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub type_: String,
    pub data: serde_json::Value,
    pub event_index: i64,
    pub indexed_type: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl EventStreamMessage {
    pub fn from_event(event: &Event, transaction_timestamp: chrono::NaiveDateTime) -> Self {
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
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CachedEvent {
    pub event_stream_message: EventStreamMessage,
    pub num_events_in_transaction: usize,
    pub weight: usize,
}

impl CachedEvent {
    pub fn from_event_stream_message(
        event_stream_message: &EventStreamMessage,
        num_events_in_transaction: usize,
        weight: usize,
    ) -> Self {
        CachedEvent {
            event_stream_message: event_stream_message.clone(),
            num_events_in_transaction,
            weight,
        }
    }

    pub fn empty(transaction_version: i64) -> Self {
        CachedEvent {
            event_stream_message: EventStreamMessage {
                sequence_number: 0,
                creation_number: 0,
                account_address: "".to_string(),
                transaction_version,
                transaction_block_height: 0,
                type_: "".to_string(),
                data: serde_json::Value::Null,
                event_index: 0,
                indexed_type: "".to_string(),
                transaction_timestamp: chrono::NaiveDateTime::default(),
            },
            num_events_in_transaction: 0,
            weight: 0, // TODO: Fix
        }
    }
}
