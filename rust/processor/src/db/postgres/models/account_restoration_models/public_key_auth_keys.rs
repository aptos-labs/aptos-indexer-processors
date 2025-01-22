// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::schema::public_key_auth_keys::{self};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[allow(clippy::too_long_first_doc_paragraph)]
/// Represents a row in the `public_key_auth_keys` table, mapping a single public key to the
/// multi-key `auth_key` it is part of. This relationship can be many-to-many: a single `public_key`
/// may map to multiple `auth_key` values (if used in different multi-key contexts),
/// and an `auth_key` correspond to multiple public keys.
///
/// # Columns
///
/// * `public_key` - A single public key (with no prefix) that belongs to a multi-key composition.
/// * `public_key_type` - Denotes the type of the `public_key` (e.g., "ed25519", "secp256k1_ecdsa", etc.).
/// * `auth_key` - The multi-key authentication key that `public_key` participates in.
/// * `verified` - Indicates if this public key was confirmed to have been used historically for
///   transaction signing within the multi-key scheme.
/// * `last_transaction_version` - The last transaction version where the mapping was detected.
#[derive(Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(public_key, public_key_type, auth_key))]
#[diesel(table_name = public_key_auth_keys)]
pub struct PublicKeyAuthKey {
    pub public_key: String,
    pub public_key_type: String,
    pub auth_key: String,
    pub verified: bool,
    pub last_transaction_version: i64,
}
