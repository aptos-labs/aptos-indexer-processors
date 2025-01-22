// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::schema::auth_key_account_addresses::{self};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[allow(clippy::too_long_first_doc_paragraph)]
/// Represents a row in the `auth_key_account_addresses` table, which associates
/// an authentication key with the corresponding account address.
/// This mapping helps to track account address rotations by enabling discovery of the address
/// that is currently tied to a given authentication key.
///
/// # Columns
///
/// * `auth_key` - The authentication key.
/// * `address` - The Aptos account address that was authenticated by `auth_key`.
/// * `verified` - Indicates whether this auth key was actually used at least once to authenticate a
///   transaction (i.e., whether its usage has been confirmed).
/// * `last_transaction_version` - The last transaction version where the mapping was detected.
#[derive(Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(address))]
#[diesel(table_name = auth_key_account_addresses)]
pub struct AuthKeyAccountAddress {
    pub auth_key: String,
    pub address: String,
    pub verified: bool,
    pub last_transaction_version: i64,
}
