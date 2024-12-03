// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::auth_key_account_addresses::{self},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(auth_key, address))]
#[diesel(table_name = auth_key_account_addresses)]
pub struct AuthKeyAccountAddress {
    pub auth_key: String,
    pub address: String,
    pub verified: bool,
}