// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::parquet_signatures::Signature;
use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    utils::util::{get_entry_function_from_user_request, parse_timestamp, standardize_address},
};
use allocative::Allocative;
use aptos_protos::{
    transaction::v1::{UserTransaction as UserTransactionPB, UserTransactionRequest},
    util::timestamp::Timestamp,
};
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct UserTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    pub parent_signature_type: String,
    pub sender: String,
    pub sequence_number: i64,
    pub max_gas_amount: u64,
    #[allocative(skip)]
    pub expiration_timestamp_secs: chrono::NaiveDateTime,
    pub gas_unit_price: u64,
    #[allocative(skip)]
    pub timestamp: chrono::NaiveDateTime,
    pub entry_function_id_str: String,
    pub epoch: i64,
}

impl NamedTable for UserTransaction {
    const TABLE_NAME: &'static str = "user_transactions";
}

impl HasVersion for UserTransaction {
    fn version(&self) -> i64 {
        self.txn_version
    }
}

impl GetTimeStamp for UserTransaction {
    fn get_timestamp(&self) -> chrono::NaiveDateTime {
        self.timestamp
    }
}

impl UserTransaction {
    pub fn from_transaction(
        txn: &UserTransactionPB,
        timestamp: &Timestamp,
        block_height: i64,
        epoch: i64,
        version: i64,
    ) -> (Self, Vec<Signature>) {
        let user_request = txn
            .request
            .as_ref()
            .expect("Sends is not present in user txn");
        (
            Self {
                txn_version: version,
                block_height,
                parent_signature_type: txn
                    .request
                    .as_ref()
                    .unwrap()
                    .signature
                    .as_ref()
                    .map(Signature::get_signature_type)
                    .unwrap_or_default(),
                sender: standardize_address(&user_request.sender),
                sequence_number: user_request.sequence_number as i64,
                max_gas_amount: user_request.max_gas_amount,
                expiration_timestamp_secs: parse_timestamp(
                    user_request
                        .expiration_timestamp_secs
                        .as_ref()
                        .expect("Expiration timestamp is not present in user txn"),
                    version,
                ),
                gas_unit_price: user_request.gas_unit_price,
                timestamp: parse_timestamp(timestamp, version),
                entry_function_id_str: get_entry_function_from_user_request(user_request)
                    .unwrap_or_default(),
                epoch,
            },
            Self::get_signatures(user_request, version, block_height, timestamp),
        )
    }

    /// Empty vec if signature is None
    pub fn get_signatures(
        user_request: &UserTransactionRequest,
        version: i64,
        block_height: i64,
        timestamp: &Timestamp,
    ) -> Vec<Signature> {
        let ts = parse_timestamp(timestamp, version);
        user_request
            .signature
            .as_ref()
            .map(|s| {
                Signature::from_user_transaction(s, &user_request.sender, version, block_height, ts)
                    .unwrap()
            })
            .unwrap_or_default()
    }
}
