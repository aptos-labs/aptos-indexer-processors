// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

extern crate proc_macro;

use crate::{models::{default_models::{
        transactions::TransactionModel,
        block_metadata_transactions::BlockMetadataTransactionModel,
    }, user_transactions_models::user_transactions::UserTransactionModel}, processors::default_processor2::PGInsertable};
use aptos_indexer_protos::{transaction::v1::{Transaction as TransactionPB, transaction::TxnData}, util::timestamp::Timestamp};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use my_macros::PGInsertable;

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct TransactionCockroach {
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub hash: String,
    pub transaction_type: String,
    pub payload: Option<serde_json::Value>,
    pub state_change_hash: String,
    pub event_root_hash: String,
    pub state_checkpoint_hash: Option<String>,
    pub gas_used: Decimal,
    pub success: bool,
    pub vm_status: String,
    pub accumulator_root_hash: String,
    pub num_events: i64,
    pub num_write_set_changes: i64,
    pub epoch: i64,
    pub parent_signature_type: String,
    pub sender: String,
    pub sequence_number: i64,
    pub max_gas_amount: Decimal,
    pub expiration_timestamp_secs: chrono::NaiveDateTime,
    pub gas_unit_price: Decimal,
    pub timestamp: chrono::NaiveDateTime,
    pub entry_function_id_str: String,
    pub signature: Option<serde_json::Value>,
    pub id: String,
    pub round: i64,
    pub previous_block_votes_bitvec: serde_json::Value,
    pub proposer: String,
    pub failed_proposer_indices: serde_json::Value,
}

impl TransactionCockroach {
    pub fn from_transactions(transactions: &[TransactionPB]) -> Vec<Self> {
        let mut transactions_to_return = vec![];
        for transaction in transactions {
            let (txn, blockmetadata_txn, _, _) = TransactionModel::from_transaction(transaction);
            let txn_data = transaction
                .txn_data
                .as_ref()
                .expect("Txn Data doesn't exit!");
            let timestamp = &transaction.timestamp.as_ref().unwrap();
            transactions_to_return.push(Self::transaction_to_insert(
                txn,
                txn_data,
                blockmetadata_txn,
                timestamp,
            ));
        }
        transactions_to_return
    }    

    fn transaction_to_insert(
        txn: TransactionModel,
        txn_detail: &TxnData,
        blockmetadata_txn: Option<BlockMetadataTransactionModel>,
        timestamp: &Timestamp,
    ) -> Self {
        let mut transaction = TransactionCockroach::default();
    
        transaction.transaction_version = txn.version;
        transaction.transaction_block_height = txn.block_height;
        transaction.hash = txn.hash;
        transaction.transaction_type = txn.type_;
        transaction.payload = Some(serde_json::to_value(&txn.payload).unwrap());
        transaction.state_change_hash = txn.state_change_hash;
        transaction.event_root_hash = txn.event_root_hash;
        transaction.state_checkpoint_hash = txn.state_checkpoint_hash;
        transaction.gas_used = Decimal::default();
        transaction.success = txn.success;
        transaction.vm_status = txn.vm_status;
        transaction.accumulator_root_hash = txn.accumulator_root_hash;
        transaction.num_events = txn.num_events;
        transaction.num_write_set_changes = txn.num_write_set_changes;
        transaction.epoch = txn.epoch;
    
        if let TxnData::User(inner) = txn_detail {
            let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                inner,
                timestamp,
                txn.block_height,
                txn.epoch,
                txn.version,
            );
    
            transaction.parent_signature_type = user_transaction.parent_signature_type;
            transaction.sender = user_transaction.sender;
            transaction.sequence_number = user_transaction.sequence_number;
            transaction.max_gas_amount = Decimal::default();
            transaction.expiration_timestamp_secs = user_transaction.expiration_timestamp_secs;
            transaction.gas_unit_price = Decimal::default();
            transaction.timestamp = user_transaction.timestamp;
            transaction.entry_function_id_str = user_transaction.entry_function_id_str;
            transaction.signature = Some(serde_json::to_value(sigs).unwrap());
        } else if let Some(blockmetadata_txn) = blockmetadata_txn {
            let previous_block_votes_bitvec =
                serde_json::to_string(&blockmetadata_txn.previous_block_votes_bitvec).unwrap_or_default();
            transaction.id = blockmetadata_txn.id;
            transaction.round = blockmetadata_txn.round;
            transaction.previous_block_votes_bitvec =
                serde_json::Value::String(previous_block_votes_bitvec);
            transaction.proposer = blockmetadata_txn.proposer;
            transaction.failed_proposer_indices = serde_json::Value::Null;
        }
    
        transaction
    }
}
