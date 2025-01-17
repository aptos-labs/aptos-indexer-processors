// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use diesel::{Identifiable, Insertable, Queryable};
use field_count::FieldCount;
use processor::schema::{
    auth_key_account_addresses, auth_key_multikey_layout, public_key_auth_keys,
};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
#[diesel(primary_key(address))]
#[diesel(table_name = auth_key_account_addresses)]
pub struct AuthKeyAccountAddress {
    pub auth_key: String,
    pub address: String,
    pub verified: bool,
    pub last_transaction_version: i64,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable)]
#[diesel(primary_key(auth_key))]
#[diesel(table_name = auth_key_multikey_layout)]
pub struct AuthKeyMultikeyLayout {
    pub auth_key: String,
    pub signatures_required: i64,
    pub multikey_layout_with_prefixes: serde_json::Value,
    pub multikey_type: String,
    pub last_transaction_version: i64,
}

#[derive(
    Clone, Debug, Default, Deserialize, FieldCount, Identifiable, Insertable, Serialize, Queryable,
)]
#[diesel(primary_key(public_key, public_key_type, auth_key))]
#[diesel(table_name = public_key_auth_keys)]
pub struct PublicKeyAuthKey {
    pub public_key: String,
    pub public_key_type: String,
    pub auth_key: String,
    pub verified: bool,
    pub last_transaction_version: i64,
}
