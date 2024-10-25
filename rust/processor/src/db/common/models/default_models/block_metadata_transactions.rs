// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    schema::block_metadata_transactions,
    utils::util::{parse_timestamp, standardize_address},
};
use aptos_protos::{transaction::v1::BlockMetadataTransaction, util::timestamp::Timestamp};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(version))]
#[diesel(table_name = block_metadata_transactions)]
pub struct BlockMetadataTransactionPG {
    pub version: i64,
    pub block_height: i64,
    pub id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: serde_json::Value,
    pub proposer: String,
    pub failed_proposer_indices: serde_json::Value,
    pub timestamp: chrono::NaiveDateTime,
}

impl BlockMetadataTransactionPG {
    pub fn from_bmt_transaction(
        txn: &BlockMetadataTransaction,
        version: i64,
        block_height: i64,
        epoch: i64,
        timestamp: &Timestamp,
    ) -> Self {
        Self {
            version,
            block_height,
            id: txn.id.to_string(),
            epoch,
            round: txn.round as i64,
            proposer: standardize_address(txn.proposer.as_str()),
            failed_proposer_indices: serde_json::to_value(&txn.failed_proposer_indices).unwrap(),
            previous_block_votes_bitvec: serde_json::to_value(&txn.previous_block_votes_bitvec)
                .unwrap(),
            // time is in microseconds
            timestamp: parse_timestamp(timestamp, version),
        }
    }
}

// Prevent conflicts with other things named `Transaction`
pub type BlockMetadataTransactionModel = BlockMetadataTransactionPG;
