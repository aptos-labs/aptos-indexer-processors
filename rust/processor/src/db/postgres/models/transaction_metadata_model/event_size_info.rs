// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::schema::event_size_info;
use aptos_protos::transaction::v1::EventSizeInfo;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, index))]
#[diesel(table_name = event_size_info)]
pub struct EventSize {
    pub transaction_version: i64,
    pub index: i64,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
}

impl EventSize {
    pub fn from_event_size_info(
        info: &EventSizeInfo,
        transaction_version: i64,
        index: i64,
    ) -> Self {
        EventSize {
            transaction_version,
            index,
            type_tag_bytes: info.type_tag_bytes as i64,
            total_bytes: info.total_bytes as i64,
        }
    }
}
