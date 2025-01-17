use crate::models::account_restoration_models::{
    AuthKeyAccountAddress, AuthKeyMultikeyLayout, PublicKeyAuthKey,
};
use anyhow::Result;
use diesel::{PgConnection, RunQueryDsl};
use processor::schema::{
    auth_key_account_addresses::dsl as aa_dsl, auth_key_multikey_layout::dsl as am_dsl,
    public_key_auth_keys::dsl as pa_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    _txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let aa_result = aa_dsl::auth_key_account_addresses.load::<AuthKeyAccountAddress>(conn)?;
    result_map.insert(
        "auth_key_account_addresses".to_string(),
        serde_json::to_value(&aa_result)?,
    );

    let am_result = am_dsl::auth_key_multikey_layout.load::<AuthKeyMultikeyLayout>(conn)?;
    result_map.insert(
        "auth_key_multikey_layout".to_string(),
        serde_json::to_value(&am_result)?,
    );

    let pa_result = pa_dsl::public_key_auth_keys.load::<PublicKeyAuthKey>(conn)?;
    result_map.insert(
        "public_key_auth_keys".to_string(),
        serde_json::to_value(&pa_result)?,
    );

    Ok(result_map)
}
