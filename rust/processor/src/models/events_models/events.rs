// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::events,
    utils::util::{standardize_address, truncate_str},
};
use aptos_protos::transaction::v1::{Event as EventPB, UserTransactionRequest};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// p99 currently is 303 so using 300 as a safe max length
const EVENT_TYPE_MAX_LENGTH: usize = 600;

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
    pub from: String,
    pub entry_function_payload: serde_json::Value,
    pub entry_function_id_str: String,
    pub c_type_1: String,
    pub c_type_2: String,
    pub c_type_3: String,
}

impl Event {
    pub fn from_event(
        event: &EventPB,
        transaction_version: i64,
        transaction_block_height: i64,
        event_index: i64,
        request: &Option<UserTransactionRequest>
    ) -> Self {
        let t: &str = event.type_str.as_ref();
        // GET request OR none
        let request_data = match request {
            Some(r) => r.payload.as_ref(),
            None => None,
        };
        if request_data.is_some(){
            let entry_function_payload_json = match request_data.unwrap().payload.as_ref().unwrap() {
                aptos_protos::transaction::v1::transaction_payload::Payload::EntryFunctionPayload(entry_function_payload) => {
                    serde_json::to_value(entry_function_payload).ok()
                },
                _ => None,
            };
            let entry_function_id_str = match request_data.unwrap().payload.as_ref().unwrap() {
                aptos_protos::transaction::v1::transaction_payload::Payload::EntryFunctionPayload(entry_function_payload) => {
                    entry_function_payload.entry_function_id_str.to_string()
                },
                _ => "".to_string(),
            };
            let from = request.as_ref().unwrap().sender.as_str();

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
                from: from.to_string(),
                entry_function_payload: entry_function_payload_json.unwrap_or_default(),
                entry_function_id_str: entry_function_id_str.to_string(),
                c_type_1: t.split("::").next().unwrap().to_string(),
                c_type_2: t.split("::").nth(1).unwrap().to_string(),
                c_type_3: t.split("::").nth(2).unwrap_or("").to_string(),
            }
        }
        else {
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
                from: "".to_string(),
                entry_function_payload: serde_json::Value::Null,
                entry_function_id_str: "".to_string(),
                c_type_1: t.split("::").next().unwrap().to_string(),
                c_type_2: t.split("::").nth(1).unwrap().to_string(),
                c_type_3: t.split("::").nth(2).unwrap_or("").to_string(),
            }
        }
    }

    pub fn from_events(
        events: &[EventPB],
        transaction_version: i64,
        transaction_block_height: i64,
        request: &Option<UserTransactionRequest>
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
                    request,
                )
            })
            .collect::<Vec<EventModel>>()
    }
}

// Prevent conflicts with other things named `Event`
pub type EventModel = Event;
