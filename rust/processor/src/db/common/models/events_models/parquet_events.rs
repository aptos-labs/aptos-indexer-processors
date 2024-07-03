// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    bq_analytics::generic_parquet_processor::{HasVersion, NamedTable},
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
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub txn_version: i64,
    pub block_height: i64,
    pub type_: String,
    pub data: String,
    pub event_index: i64,
    pub indexed_type: String,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
}

impl NamedTable for Event {
    const TABLE_NAME: &'static str = "events";
}

impl HasVersion for Event {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl Event {
    pub fn from_event(
        event: &EventPB,
        txn_version: i64,
        block_height: i64,
        event_index: i64,
        size_info: &EventSizeInfo,
    ) -> Self {
        let t: &str = event.type_str.as_ref();
        Event {
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            sequence_number: event.sequence_number as i64,
            txn_version,
            block_height,
            type_: t.to_string(),
            data: serde_json::from_str(event.data.as_str()).unwrap(),
            event_index,
            indexed_type: truncate_str(t, EVENT_TYPE_MAX_LENGTH),
            type_tag_bytes: size_info.type_tag_bytes as i64,
            total_bytes: size_info.total_bytes as i64,
        }
    }

    pub fn from_events(
        events: &[EventPB],
        txn_version: i64,
        block_height: i64,
        event_size_info: &Vec<EventSizeInfo>,
    ) -> Vec<Self> {
        events
            .iter()
            .zip_eq(event_size_info.iter())
            .enumerate()
            .map(|(index, (event, size_info))| {
                Self::from_event(event, txn_version, block_height, index as i64, size_info)
            })
            .collect::<Vec<ParquetEventModel>>()
    }
}

pub type ParquetEventModel = Event;
