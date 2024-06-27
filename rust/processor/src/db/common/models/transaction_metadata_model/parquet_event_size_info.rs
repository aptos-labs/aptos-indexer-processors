// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::bq_analytics::generic_parquet_processor::{HasVersion, NamedTable};
use allocative_derive::Allocative;
use aptos_protos::transaction::v1::EventSizeInfo;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, ParquetRecordWriter, Serialize,
)]
pub struct EventSize {
    pub txn_version: i64,
    pub event_index: i64,
    pub type_tag_bytes: i64,
    pub total_bytes: i64,
}

impl NamedTable for EventSize {
    const TABLE_NAME: &'static str = "event_size";
}

impl HasVersion for EventSize {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl EventSize {
    pub fn from_event_size_info(info: &EventSizeInfo, txn_version: i64, event_index: i64) -> Self {
        EventSize {
            txn_version,
            event_index,
            type_tag_bytes: info.type_tag_bytes as i64,
            total_bytes: info.total_bytes as i64,
        }
    }
}
