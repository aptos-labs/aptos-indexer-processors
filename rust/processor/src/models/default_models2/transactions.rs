// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

extern crate proc_macro;

use crate::{
    models::{
        default_models::{
            block_metadata_transactions::BlockMetadataTransactionModel,
            transactions::TransactionModel,
        },
        user_transactions_models::user_transactions::UserTransactionModel,
    },
    processors::default_processor2::PGInsertable,
};
use aptos_indexer_protos::{
    transaction::v1::{
        transaction::TxnData, transaction_payload::Type, write_set::WriteSetType,
        Transaction as TransactionPB,
    },
    util::timestamp::Timestamp,
};
use my_macros::PGInsertable;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Deserialize, Serialize, Clone, PGInsertable, Default)]
pub struct TransactionCockroach {
    pub transaction_version: i64,
    pub transaction_block_height: i64,
    pub hash: String,
    pub transaction_type: String,
    pub payload: Option<serde_json::Value>,
    pub payload_type: Option<String>,
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
        let mut transaction = TransactionCockroach {
            transaction_version: txn.version,
            transaction_block_height: txn.block_height,
            hash: txn.hash,
            transaction_type: txn.type_,
            payload: Some(serde_json::to_value(&txn.payload).unwrap()),
            state_change_hash: txn.state_change_hash,
            event_root_hash: txn.event_root_hash,
            state_checkpoint_hash: txn.state_checkpoint_hash,
            success: txn.success,
            vm_status: txn.vm_status,
            accumulator_root_hash: txn.accumulator_root_hash,
            gas_used: Decimal::from_str(&txn.gas_used.to_string()).unwrap_or(Decimal::ZERO),
            ..Self::default()
        };

        match txn_detail {
            TxnData::User(inner) => {
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
                transaction.expiration_timestamp_secs = user_transaction.expiration_timestamp_secs;
                transaction.timestamp = user_transaction.timestamp;
                transaction.entry_function_id_str = user_transaction.entry_function_id_str;
                transaction.gas_unit_price =
                    Decimal::from_str(&user_transaction.gas_unit_price.to_string())
                        .unwrap_or(Decimal::ZERO);
                transaction.max_gas_amount =
                    Decimal::from_str(&user_transaction.max_gas_amount.to_string())
                        .unwrap_or(Decimal::ZERO);
                transaction.signature = Some(serde_json::to_value(sigs).unwrap());
                let payload = inner
                    .request
                    .as_ref()
                    .expect("Getting user request failed.")
                    .payload
                    .as_ref()
                    .expect("Getting payload failed.");
                transaction.payload_type = Some(
                    Type::try_from(payload.r#type)
                        .expect("Payload type doesn't exist!")
                        .as_str_name()
                        .to_string(),
                );
            },
            TxnData::BlockMetadata(_) if blockmetadata_txn.is_some() => {
                let blockmetadata_txn = blockmetadata_txn.unwrap();
                transaction.id = blockmetadata_txn.id;
                transaction.round = blockmetadata_txn.round;
                let previous_block_votes_bitvec =
                    serde_json::to_string(&blockmetadata_txn.previous_block_votes_bitvec)
                        .unwrap_or_default();
                transaction.previous_block_votes_bitvec =
                    serde_json::Value::String(previous_block_votes_bitvec);
                transaction.proposer = blockmetadata_txn.proposer;
            },
            TxnData::Genesis(inner) => {
                let payload = inner.payload.as_ref().unwrap();
                transaction.payload_type = Some(
                    WriteSetType::try_from(payload.write_set_type)
                        .expect("WriteSet type doesn't exist!")
                        .as_str_name()
                        .to_string(),
                );
            },
            _ => {},
        }

        transaction
    }
}
