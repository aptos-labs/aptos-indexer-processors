// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    block_metadata_transactions::BlockMetadataTransaction, write_set_changes::WriteSetChangeDetail,
};
use crate::{schema::transactions, utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction as TransactionPB};
use bigdecimal::BigDecimal;
use field_count::FieldCount;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(version))]
#[diesel(table_name = transactions)]
pub struct Transaction {
    pub version: i64,
    pub block_height: i64,
    pub hash: String,
    pub type_: String,
    pub payload: Option<serde_json::Value>,
    pub state_change_hash: String,
    pub event_root_hash: String,
    pub state_checkpoint_hash: Option<String>,
    pub gas_used: BigDecimal,
    pub success: bool,
    pub vm_status: String,
    pub accumulator_root_hash: String,
    pub num_events: i64,
    pub num_write_set_changes: i64,
    pub epoch: i64,
    pub payload_type: Option<String>,
}

impl Transaction {
    pub fn from_transaction(
        transaction: &TransactionPB,
    ) -> (Option<BlockMetadataTransaction>, Vec<WriteSetChangeDetail>) {
        let block_height = transaction.block_height as i64;
        let epoch = transaction.epoch as i64;
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        let txn_data = match transaction.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["Transaction"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return (None, Vec::new());
            },
        };
        let version = transaction.version as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");

        let wsc_detail = WriteSetChangeDetail::from_write_set_changes(
            &transaction_info.changes,
            version,
            block_height,
        );

        match txn_data {
            TxnData::BlockMetadata(block_metadata_txn) => (
                Some(BlockMetadataTransaction::from_transaction(
                    block_metadata_txn,
                    version,
                    block_height,
                    epoch,
                    timestamp,
                )),
                wsc_detail,
            ),
            TxnData::User(_) => (None, wsc_detail),
            TxnData::Genesis(_) => (None, wsc_detail),
            TxnData::StateCheckpoint(_) => (None, vec![]),
            TxnData::Validator(_) => (None, wsc_detail),
            TxnData::BlockEpilogue(_) => (None, vec![]),
        }
    }

    pub fn from_transactions(
        transactions: &[TransactionPB],
    ) -> (Vec<BlockMetadataTransaction>, Vec<WriteSetChangeDetail>) {
        let mut block_metadata_txns = vec![];
        let mut wsc_details = vec![];

        let processed_txns: Vec<_> = transactions
            .par_iter()
            .map(Self::from_transaction)
            .collect();

        for processed_txn in processed_txns {
            let (block_metadata, mut wsc_detail_list) = processed_txn;
            if let Some(a) = block_metadata {
                block_metadata_txns.push(a);
            }
            wsc_details.append(&mut wsc_detail_list);
        }
        (block_metadata_txns, wsc_details)
    }
}

// Prevent conflicts with other things named `Transaction`
pub type TransactionModel = Transaction;
