// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::common::models::default_models::raw_block_metadata_transactions::{
        BlockMetadataTransactionConvertible, RawBlockMetadataTransaction,
    },
};
use allocative_derive::Allocative;
use field_count::FieldCount;
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(
    Allocative, Clone, Debug, Default, Deserialize, FieldCount, Serialize, ParquetRecordWriter,
)]
pub struct BlockMetadataTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    pub block_id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: String,
    pub proposer: String,
    pub failed_proposer_indices: String,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub since_unix_epoch: u64,
}

impl NamedTable for BlockMetadataTransaction {
    const TABLE_NAME: &'static str = "block_metadata_transactions";
}

impl HasVersion for BlockMetadataTransaction {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for BlockMetadataTransaction {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.block_timestamp
    }
}

impl BlockMetadataTransactionConvertible for BlockMetadataTransaction {
    fn from_raw(raw_item: &RawBlockMetadataTransaction) -> Self {
        BlockMetadataTransaction {
            txn_version: raw_item.version,
            block_height: raw_item.block_height,
            block_id: raw_item.id.clone(),
            round: raw_item.round,
            epoch: raw_item.epoch,
            previous_block_votes_bitvec: raw_item.previous_block_votes_bitvec.clone(),
            proposer: raw_item.proposer.clone(),
            failed_proposer_indices: raw_item.failed_proposer_indices.clone(),
            block_timestamp: raw_item.timestamp,
            since_unix_epoch: raw_item.ns_since_unix_epoch,
        }
    }
}

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parquet::record::RecordWriter, utils::util::compute_nanos_since_epoch};
    use chrono::NaiveDateTime;
    use parquet::file::writer::SerializedFileWriter;
    use serde_json::json;

    #[test]
    fn test_raw_block_metadata_transaction() {
        let time_stamp = NaiveDateTime::from_timestamp(1, 0);
        let samples = vec![BlockMetadataTransaction {
            txn_version: 1,
            block_height: 1,
            block_id: "id".to_string(),
            round: 1,
            epoch: 1,
            previous_block_votes_bitvec: json!([1, 2, 3]).to_string(),
            proposer: "proposer".to_string(),
            failed_proposer_indices: json!([1, 2, 3]).to_string(),
            block_timestamp: time_stamp,
            since_unix_epoch: compute_nanos_since_epoch(time_stamp),
        }];

        let schema = samples.as_slice().schema().unwrap();

        let mut writer = SerializedFileWriter::new(Vec::new(), schema, Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        samples
            .as_slice()
            .write_to_row_group(&mut row_group)
            .unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_from_raw() {
        let time_stamp = NaiveDateTime::from_timestamp(1, 0);
        let raw = RawBlockMetadataTransaction {
            version: 1,
            block_height: 1,
            id: "id".to_string(),
            round: 1,
            epoch: 1,
            previous_block_votes_bitvec: json!([1, 2, 3]).to_string(),
            proposer: "proposer".to_string(),
            failed_proposer_indices: json!([1, 2, 3]).to_string(),
            timestamp: time_stamp,
            ns_since_unix_epoch: compute_nanos_since_epoch(time_stamp),
        };

        let block_metadata_transaction = BlockMetadataTransaction::from_raw(&raw);

        assert_eq!(block_metadata_transaction.txn_version, 1);
        assert_eq!(block_metadata_transaction.block_height, 1);
        assert_eq!(block_metadata_transaction.block_id, "id");
        assert_eq!(block_metadata_transaction.round, 1);
        assert_eq!(block_metadata_transaction.epoch, 1);
        assert_eq!(
            block_metadata_transaction.previous_block_votes_bitvec,
            "[1,2,3]"
        );
        assert_eq!(block_metadata_transaction.proposer, "proposer");
        assert_eq!(
            block_metadata_transaction.failed_proposer_indices,
            "[1,2,3]"
        );
        assert_eq!(
            block_metadata_transaction.block_timestamp,
            NaiveDateTime::from_timestamp(1, 0)
        );

        let samples = vec![block_metadata_transaction];

        let schema = samples.as_slice().schema().unwrap();

        let mut writer = SerializedFileWriter::new(Vec::new(), schema, Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        samples
            .as_slice()
            .write_to_row_group(&mut row_group)
            .unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }
}
