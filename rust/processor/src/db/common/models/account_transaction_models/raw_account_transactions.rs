// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    db::{
        common::models::object_models::v2_object_utils::ObjectWithMetadata,
        postgres::models::{
            resources::FromWriteResource,
            user_transactions_models::user_transactions::UserTransaction,
        },
    },
    utils::{counters::PROCESSOR_UNKNOWN_TYPE_COUNT, util::standardize_address},
};
use ahash::AHashSet;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use serde::{Deserialize, Serialize};

pub type AccountTransactionPK = (String, i64);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawAccountTransaction {
    pub transaction_version: i64,
    pub account_address: String,
    pub block_timestamp: chrono::NaiveDateTime,
}

impl RawAccountTransaction {
    /// This table will record every transaction that touch an account which could be
    /// a user account, an object, or a resource account.
    /// We will consider all transactions that modify a resource or event associated with a particular account.
    /// We will do 1 level of redirection for now (e.g. if it's an object, we will record the owner as account address).
    /// We will also consider transactions that the account signed or is part of a multi sig / multi agent.
    /// TODO: recursively find the parent account of an object
    /// TODO: include table items in the detection path
    pub fn get_accounts(transaction: &Transaction) -> AHashSet<String> {
        let txn_version = transaction.version as i64;
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["AccountTransaction"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return AHashSet::new();
            },
        };
        let transaction_info = transaction.info.as_ref().unwrap_or_else(|| {
            panic!("Transaction info doesn't exist for version {}", txn_version)
        });
        let wscs = &transaction_info.changes;
        let (events, signatures) = match txn_data {
            TxnData::User(inner) => (
                &inner.events,
                UserTransaction::get_signatures(
                    inner.request.as_ref().unwrap_or_else(|| {
                        panic!("User request doesn't exist for version {}", txn_version)
                    }),
                    txn_version,
                    transaction.block_height as i64,
                ),
            ),
            TxnData::Genesis(inner) => (&inner.events, vec![]),
            TxnData::BlockMetadata(inner) => (&inner.events, vec![]),
            TxnData::Validator(inner) => (&inner.events, vec![]),
            _ => {
                return AHashSet::new();
            },
        };
        let mut accounts = AHashSet::new();
        for sig in signatures {
            accounts.insert(sig.signer);
        }
        for event in events {
            // Record event account address. We don't really have to worry about objects here
            // because it'll be taken care of in the resource section.
            accounts.insert(standardize_address(
                event.key.as_ref().unwrap().account_address.as_str(),
            ));
        }
        for wsc in wscs {
            match wsc.change.as_ref().unwrap() {
                Change::DeleteResource(res) => {
                    // Record resource account.
                    // TODO: If the resource is an object, then we need to look for the latest
                    // owner. This isn't really possible right now given we have parallel threads
                    // so it'll be very difficult to ensure that we have the correct latest owner.
                    accounts.insert(standardize_address(res.address.as_str()));
                },
                Change::WriteResource(res) => {
                    // Record resource account. If the resource is an object, then we record the
                    // owner as well.
                    // This handles partial deletes as well.
                    accounts.insert(standardize_address(res.address.as_str()));
                    if let Some(inner) = &ObjectWithMetadata::from_write_resource(res).unwrap() {
                        accounts.insert(inner.object_core.get_owner_address());
                    }
                },
                _ => {},
            }
        }
        accounts
    }
}
