// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::parquet_signatures::Signature;
use crate::{
    bq_analytics::generic_parquet_processor::{GetTimeStamp, HasVersion, NamedTable},
    db::postgres::models::fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
    utils::util::{get_entry_function_from_user_request, parse_timestamp, standardize_address},
};
use allocative::Allocative;
use anyhow::Result;
use aptos_protos::{
    transaction::v1::{
        TransactionInfo, UserTransaction as UserTransactionPB, UserTransactionRequest,
    },
    util::timestamp::Timestamp,
};
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

#[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
pub struct UserTransaction {
    pub txn_version: i64,
    pub block_height: i64,
    #[allocative(skip)]
    pub block_timestamp: chrono::NaiveDateTime,
    pub epoch: i64,
    pub sender: String,
    pub sequence_number: i64,
    pub entry_function_id_str: String,
    pub expiration_timestamp_secs: u64,
    pub parent_signature_type: String,
    pub gas_fee_payer_address: Option<String>,
    pub gas_used_unit: u64,
    pub gas_unit_price: u64,
    pub max_gas_octa: u64,
    pub storage_refund_octa: u64,
    pub is_transaction_success: bool,
    pub num_signatures: i64,
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
        self.block_timestamp
    }
}

impl UserTransaction {
    pub fn from_transaction(
        txn: &UserTransactionPB,
        txn_info: &TransactionInfo,
        fee_statement: Option<FeeStatement>,
        timestamp: &Timestamp,
        block_height: i64,
        epoch: i64,
        version: i64,
    ) -> Self {
        let user_request = txn
            .request
            .as_ref()
            .expect("Sends is not present in user txn");
        let gas_fee_payer_address = match user_request.signature.as_ref() {
            Some(signature) => Signature::get_fee_payer_address(signature, version),
            None => None,
        };
        let num_signatures = Self::get_signatures(user_request, version, block_height).len() as i64;

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
            max_gas_octa: user_request.max_gas_amount,
            expiration_timestamp_secs: user_request
                .expiration_timestamp_secs
                .as_ref()
                .expect("Expiration timestamp is not present in user txn")
                .seconds as u64,
            gas_unit_price: user_request.gas_unit_price,
            block_timestamp: parse_timestamp(timestamp, version),
            entry_function_id_str: get_entry_function_from_user_request(user_request)
                .unwrap_or_default(),
            epoch,
            gas_used_unit: txn_info.gas_used,
            gas_fee_payer_address,
            is_transaction_success: txn_info.success,
            storage_refund_octa: fee_statement
                .map(|fs| fs.storage_fee_refund_octas)
                .unwrap_or(0),
            num_signatures,
        }
    }

    /// Empty vec if signature is None
    pub fn get_signatures(
        user_request: &UserTransactionRequest,
        version: i64,
        block_height: i64,
    ) -> Vec<Signature> {
        user_request
            .signature
            .as_ref()
            .map(|s| {
                Signature::from_user_transaction(s, &user_request.sender, version, block_height)
                    .unwrap()
            })
            .unwrap_or_default()
    }
}
