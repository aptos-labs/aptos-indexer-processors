// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::{
    block_metadata_transactions::BlockMetadataTransaction,
    parquet_write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
};
use crate::{
    schema::transactions,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{
            get_clean_payload, get_clean_writeset, get_payload_type, standardize_address,
        },
    },
};
use aptos_protos::transaction::v1::{
    transaction::{TransactionType, TxnData},
    Transaction as TransactionPB, TransactionInfo,
};

use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use parquet_derive::{ParquetRecordWriter};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Serialize, ParquetRecordWriter)]
#[diesel(primary_key(version))]
#[diesel(table_name = transactions)]
pub struct Transaction {
    pub version: i64,
    pub block_height: i64,
    pub hash: String,
    pub type_: String,
    pub payload: Option<String>, // parequet derive doesn' support this 
    pub state_change_hash: String,
    pub event_root_hash: String,
    pub state_checkpoint_hash: Option<String>, 
    pub gas_used: u64, // parequet derive doesn' support this 
    pub success: bool, 
    pub vm_status: String,
    pub accumulator_root_hash: String,
    pub num_events: i64,
    pub num_write_set_changes: i64,
    pub epoch: i64,
    pub payload_type: Option<String>,
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            version: 0,
            block_height: 0,
            hash: "".to_string(),
            type_: "".to_string(),
            payload: None,
            state_change_hash: "".to_string(),
            event_root_hash: "".to_string(),
            state_checkpoint_hash: None,
            gas_used: 0,
            success: true,
            vm_status: "".to_string(),
            accumulator_root_hash: "".to_string(),
            num_events: 0,
            num_write_set_changes: 0,
            epoch: 0,
            payload_type: None,
        }
    }
}

impl Transaction {
    fn from_transaction_info(
        info: &TransactionInfo,
        version: i64,
        epoch: i64,
        block_height: i64,
    ) -> Self {
        Self {
            version,
            block_height,
            hash: standardize_address(hex::encode(info.hash.as_slice()).as_str()),
            state_change_hash: standardize_address(
                hex::encode(info.state_change_hash.as_slice()).as_str(),
            ),
            event_root_hash: standardize_address(
                hex::encode(info.event_root_hash.as_slice()).as_str(),
            ),
            state_checkpoint_hash: info
                .state_checkpoint_hash
                .as_ref()
                .map(|hash| standardize_address(hex::encode(hash).as_str())),
            gas_used: info.gas_used,
            success: info.success,
            vm_status: info.vm_status.clone(),
            accumulator_root_hash: standardize_address(
                hex::encode(info.accumulator_root_hash.as_slice()).as_str(),
            ),
            num_write_set_changes: info.changes.len() as i64,
            epoch,
            ..Default::default()
        }
    }

    fn from_transaction_info_with_data(
        info: &TransactionInfo,
        payload: Option<String>,
        payload_type: Option<String>,
        version: i64,
        type_: String,
        num_events: i64,
        block_height: i64,
        epoch: i64,
    ) -> Self {
        Self {
            type_,
            payload,
            version,
            block_height,
            hash: standardize_address(hex::encode(info.hash.as_slice()).as_str()),
            state_change_hash: standardize_address(
                hex::encode(info.state_change_hash.as_slice()).as_str(),
            ),
            event_root_hash: standardize_address(
                hex::encode(info.event_root_hash.as_slice()).as_str(),
            ),
            state_checkpoint_hash: info
                .state_checkpoint_hash
                .as_ref()
                .map(|hash| standardize_address(hex::encode(hash).as_str())),
            gas_used: info.gas_used,
            success: info.success,
            vm_status: info.vm_status.clone(),
            accumulator_root_hash: standardize_address(
                hex::encode(info.accumulator_root_hash.as_slice()).as_str(),
            ),
            num_events,
            num_write_set_changes: info.changes.len() as i64,
            epoch,
            payload_type,
        }
    }

    pub fn from_transaction(
        transaction: &TransactionPB,
    ) -> (
        Self,
        Option<BlockMetadataTransaction>,
        Vec<WriteSetChangeModel>,
        Vec<WriteSetChangeDetail>,
    ) {
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
                let transaction_out = Self::from_transaction_info(
                    transaction_info,
                    transaction.version as i64,
                    epoch,
                    block_height,
                );
                return (transaction_out, None, Vec::new(), Vec::new());
            },
        };
        let version = transaction.version as i64;
        let transaction_type = TransactionType::try_from(transaction.r#type)
            .expect("Transaction type doesn't exist!")
            .as_str_name()
            .to_string();
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let txn_timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0).expect("Txn Timestamp is invalid!");
        match txn_data {
            TxnData::User(user_txn) => {
                let (wsc, wsc_detail) = WriteSetChangeModel::from_write_set_changes(
                    &transaction_info.changes,
                    version,
                    block_height,
                    txn_timestamp,
                );
                let payload = user_txn
                    .request
                    .as_ref()
                    .expect("Getting user request failed.")
                    .payload
                    .as_ref()
                    .expect("Getting payload failed.");
                let payload_cleaned = get_clean_payload(payload, version);
                let payload_type = get_payload_type(payload);

                let serialized_payload = serde_json::to_string(&payload_cleaned).unwrap(); // Handle errors as needed
                (
                    Self::from_transaction_info_with_data(
                        transaction_info,
                        Some(serialized_payload),
                        Some(payload_type),
                        version,
                        transaction_type,
                        user_txn.events.len() as i64,
                        block_height,
                        epoch,
                    ),
                    None,
                    wsc,
                    wsc_detail,
                )
            },
            TxnData::Genesis(genesis_txn) => {
                let (wsc, wsc_detail) = WriteSetChangeModel::from_write_set_changes(
                    &transaction_info.changes,
                    version,
                    block_height,
                    txn_timestamp
                );
                let payload = genesis_txn.payload.as_ref().unwrap();
                let payload_cleaned = get_clean_writeset(payload, version);
                // It's genesis so no big deal
                let serialized_payload = serde_json::to_string(&payload_cleaned).unwrap(); // Handle errors as needed
                let payload_type = None;
                (
                    Self::from_transaction_info_with_data(
                        transaction_info,
                        Some(serialized_payload),
                        payload_type,
                        version,
                        transaction_type,
                        genesis_txn.events.len() as i64,
                        block_height,
                        epoch,
                    ),
                    None,
                    wsc,
                    wsc_detail,
                )
            },
            TxnData::BlockMetadata(block_metadata_txn) => {
                let (wsc, wsc_detail) = WriteSetChangeModel::from_write_set_changes(
                    &transaction_info.changes,
                    version,
                    block_height,
                    txn_timestamp,
                );
                (
                    Self::from_transaction_info_with_data(
                        transaction_info,
                        None,
                        None,
                        version,
                        transaction_type,
                        block_metadata_txn.events.len() as i64,
                        block_height,
                        epoch,
                    ),
                    Some(BlockMetadataTransaction::from_transaction(
                        block_metadata_txn,
                        version,
                        block_height,
                        epoch,
                        timestamp,
                    )),
                    wsc,
                    wsc_detail,
                )
            },
            TxnData::StateCheckpoint(_) => (
                Self::from_transaction_info_with_data(
                    transaction_info,
                    None,
                    None,
                    version,
                    transaction_type,
                    0,
                    block_height,
                    epoch,
                ),
                None,
                vec![],
                vec![],
            ),
            TxnData::Validator(_) => (
                Self::from_transaction_info_with_data(
                    transaction_info,
                    None,
                    None,
                    version,
                    transaction_type,
                    0,
                    block_height,
                    epoch,
                ),
                None,
                vec![],
                vec![],
            ),
        }
    }

    pub fn from_transactions(
        transactions: &[TransactionPB],
    ) -> (
        Vec<Self>,
        Vec<BlockMetadataTransaction>,
        Vec<WriteSetChangeModel>,
        Vec<WriteSetChangeDetail>,
    ) {
        let mut txns = vec![];
        let mut block_metadata_txns = vec![];
        let mut wscs = vec![];
        let mut wsc_details = vec![];

        for txn in transactions {
            let (txn, block_metadata, mut wsc_list, mut wsc_detail_list) =
                Self::from_transaction(txn);
            txns.push(txn);
            if let Some(a) = block_metadata {
                block_metadata_txns.push(a);
            }
            wscs.append(&mut wsc_list);
            wsc_details.append(&mut wsc_detail_list);
        }
        (txns, block_metadata_txns, wscs, wsc_details)
    }
}

// Prevent conflicts with other things named `Transaction`
pub type TransactionModel = Transaction;
