// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{
    schema::public_key_auth_keys::{self},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(public_key_bcs, public_key_type, auth_key))]
#[diesel(table_name = public_key_auth_keys)]
pub struct PublicKeyAuthKey {
    pub public_key_bcs: String,
    pub public_key_type: String,
    pub auth_key: String,
    pub verified: bool,
}