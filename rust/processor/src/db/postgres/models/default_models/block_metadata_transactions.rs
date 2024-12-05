// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::common::models::default_models::raw_block_metadata_transactions::{
        BlockMetadataTransactionConvertible, RawBlockMetadataTransaction,
    },
    schema::block_metadata_transactions,
};
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

impl BlockMetadataTransactionConvertible for BlockMetadataTransactionPG {
    fn from_raw(raw_item: &RawBlockMetadataTransaction) -> Self {
        BlockMetadataTransactionPG {
            version: raw_item.version,
            block_height: raw_item.block_height,
            id: raw_item.id.clone(),
            round: raw_item.round,
            epoch: raw_item.epoch,
            previous_block_votes_bitvec: serde_json::from_str(
                raw_item.previous_block_votes_bitvec.as_str(),
            )
            .unwrap(),
            failed_proposer_indices: serde_json::from_str(
                raw_item.failed_proposer_indices.as_str(),
            )
            .unwrap(),
            proposer: raw_item.proposer.clone(),
            timestamp: raw_item.timestamp,
        }
    }
}
// Prevent conflicts with other things named `Transaction`
pub type BlockMetadataTransactionModel = BlockMetadataTransactionPG;
