// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    utils::util::{standardize_address, truncate_str},
};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::{Event as EventPB, EventSizeInfo};
use itertools::Itertools;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

// p99 currently is 303 so using 300 as a safe max length
const EVENT_TYPE_MAX_LENGTH: usize = 300;

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct Event {
    pub txn_version: i64,
    pub account_address: String,
    pub sequence_number: i64,
    pub creation_number: i64,
    pub block_height: i64,
    pub event_type: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
    pub event_version: i8,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
}

impl NamedTable for Event {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for Event {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for Event {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl Event {
    pub fn from_event(
        event: &EventPB,
        txn_version: i64,
        block_height: i64,
        event_index: i64,
        size_info: &EventSizeInfo,
        block_timestamp: chrono::NaiveDateTime,
    ) -> Self {
        let event_type: &str = event.type_str.as_ref();
        Event {
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            sequence_number: event.sequence_number as i64,
            txn_version,
            block_height,
            event_type: event_type.to_string(),
            data: event.data.clone(),
            event_index,
            indexed_type: truncate_str(event_type, EVENT_TYPE_MAX_LENGTH),
            type_tag_bytes: size_info.type_tag_bytes as i64,
            total_bytes: size_info.total_bytes as i64,
            event_version: 1i8, // this is for future proofing. TODO: change when events v2 comes
            block_timestamp,
        }
    }

    pub fn from_events(
        events: &[EventPB],
        txn_version: i64,
        block_height: i64,
        event_size_info: &[EventSizeInfo],
        block_timestamp: chrono::NaiveDateTime,
    ) -> Vec<Self> {
        // Ensure that lengths match, otherwise log and panic to investigate
        if events.len() != event_size_info.len() {
            tracing::error!(
                events_len = events.len(),
                event_size_info_len = event_size_info.len(),
                txn_version,
                "Length mismatch: events size does not match event_size_info size.",
            );
            panic!("Length mismatch: events len does not match event_size_info len");
        }

        events
            .iter()
            .zip_eq(event_size_info.iter())
            .enumerate()
            .map(|(index, (event, size_info))| {
                Self::from_event(
                    event,
                    txn_version,
                    block_height,
                    index as i64,
                    size_info,
                    block_timestamp,
                )
            })
            .collect::<Vec<ParquetEventModel>>()
    }
}

pub type ParquetEventModel = Event;
