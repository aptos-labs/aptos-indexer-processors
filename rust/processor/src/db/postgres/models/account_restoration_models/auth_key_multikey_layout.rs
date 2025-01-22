// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::schema::auth_key_multikey_layout::{self};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[allow(clippy::too_long_first_doc_paragraph)]
/// Represents a row in the `auth_key_multikey_layout` table, describing the composition of a multi-key
/// authentication key (e.g., a set of public keys, their types and prefixes, and the signature threshold).
/// This information is essential for reconstructing multi-key transaction authenticators.
///
/// # Columns
///
/// * `auth_key` - The unique authentication key derived from the multi-key setup.
/// * `signatures_required` - The minimum number of valid signatures required (threshold) to
///   authenticate transactions using this multi-key scheme.
/// * `multikey_layout_with_prefixes` - A JSON-encoded array of public keys and their 1-byte type prefixes
///   (e.g., `0x00` for Ed25519 Generalized).
///   For MultiEd25519 keys, the prefixes are omitted (legacy Ed25519 format).
/// * `multikey_type` - The high-level classification of the multi-key (`multi_ed25519` or `multi_key`).
/// * `last_transaction_version` - The last transaction version where the mapping was detected.
#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(auth_key))]
#[diesel(table_name = auth_key_multikey_layout)]
pub struct AuthKeyMultikeyLayout {
    pub auth_key: String,
    pub signatures_required: i64,
    pub multikey_layout_with_prefixes: serde_json::Value,
    pub multikey_type: String,
    pub last_transaction_version: i64,
}
