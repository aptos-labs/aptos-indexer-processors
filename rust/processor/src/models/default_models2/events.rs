// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

extern crate proc_macro;

use crate::{processors::default_processor2::PGInsertable, utils::util::standardize_address};
use aptos_indexer_protos::transaction::v1::Event as EventPB;
use my_macros::PGInsertable;
use serde::{Deserialize, Serialize};
use crate::{processors::default_processor2::PGInsertable, utils::util::standardize_address};
use aptos_protos::transaction::v1::Event as EventPB;
use my_macros::PGInsertable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct EventsCockroach {
    pub transaction_version: i64,
    pub sequence_number: i64,
    pub creation_number: i64,
    pub account_address: String,
    pub transaction_block_height: i64,
    pub event_type: String,
    pub data: Option<serde_json::Value>,
    pub event_index: i64,
}

impl EventsCockroach {
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
                    event.clone(),
                    transaction_version,
                    transaction_block_height,
                    index as i64,
                )
            })
            .collect::<Vec<EventsCockroach>>()
    }

    fn from_event(
        event: EventPB,
        transaction_version: i64,
        transaction_block_height: i64,
        event_index: i64,
    ) -> Self {
        Self {
            sequence_number: event.sequence_number as i64,
            creation_number: event.key.as_ref().unwrap().creation_number as i64,
            account_address: standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ),
            transaction_version,
            transaction_block_height,
            event_type: event.type_str.clone(),
            data: serde_json::from_str(event.data.as_str()).unwrap(),
            event_index,
        }
    }
}
