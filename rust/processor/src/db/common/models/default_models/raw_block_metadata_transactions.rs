// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::utils::util::{parse_timestamp, standardize_address};
use aptos_protos::{transaction::v1::BlockMetadataTransaction, util::timestamp::Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawBlockMetadataTransaction {
    pub version: i64,
    pub block_height: i64,
    pub id: String,
    pub round: i64,
    pub epoch: i64,
    pub previous_block_votes_bitvec: String, // serde_json::Value,
    pub proposer: String,
    pub failed_proposer_indices: String, //serde_json::Value,
    pub timestamp: chrono::NaiveDateTime,
}

impl RawBlockMetadataTransaction {
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
            failed_proposer_indices: serde_json::to_value(&txn.failed_proposer_indices)
                .unwrap()
                .to_string(),
            previous_block_votes_bitvec: serde_json::to_value(&txn.previous_block_votes_bitvec)
                .unwrap()
                .to_string(),
            // time is in microseconds
            timestamp: parse_timestamp(timestamp, version),
        }
    }
}

// Prevent conflicts with other things named `Transaction`
pub type RawBlockMetadataTransactionModel = RawBlockMetadataTransaction;

pub trait BlockMetadataTransactionConvertible {
    fn from_raw(raw_item: &RawBlockMetadataTransaction) -> Self;
}
