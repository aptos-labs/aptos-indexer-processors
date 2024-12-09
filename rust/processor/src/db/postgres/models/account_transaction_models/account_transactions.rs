// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]
use crate::schema::account_transactions;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

pub type AccountTransactionPK = (String, i64);

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(account_address, transaction_version))]
#[diesel(table_name = account_transactions)]
pub struct AccountTransaction {
    pub transaction_version: i64,
    pub account_address: String,
}
