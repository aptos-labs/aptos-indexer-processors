// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    db::common::models::event_models::raw_events::{EventConvertible, RawEvent},
    schema::events,
};
use aptos_protos::transaction::v1::Event as EventPB;
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

impl Event {
    pub fn from_event(
        event: &EventPB,
        transaction_version: i64,
        transaction_block_height: i64,
        event_index: i64,
    ) -> Self {
        let raw = RawEvent::from_raw_event(
            event,
            transaction_version,
            transaction_block_height,
            event_index,
            None,
            None,
        );
        Self::from_raw(&raw)
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

impl EventConvertible for Event {
    fn from_raw(raw: &RawEvent) -> Self {
        Event {
            sequence_number: raw.sequence_number,
            creation_number: raw.creation_number,
            account_address: raw.account_address.clone(),
            transaction_version: raw.transaction_version,
            transaction_block_height: raw.transaction_block_height,
            type_: raw.type_.clone(),
            data: serde_json::from_str(&raw.data).unwrap(),
            event_index: raw.event_index,
            indexed_type: raw.indexed_type.clone(),
        }
    }
}

// Prevent conflicts with other things named `Event`
pub type EventModel = Event;
