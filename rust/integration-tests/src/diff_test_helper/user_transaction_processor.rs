use crate::models::user_transactions_models::{Signature, UserTransaction};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{signatures::dsl as sig_dsl, user_transactions::dsl as ut_dsl};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let ut_result = ut_dsl::user_transactions
        .filter(ut_dsl::version.eq_any(&txn_versions))
        .then_order_by(ut_dsl::version.asc())
        .load::<UserTransaction>(conn)?;
    result_map.insert(
        "user_transactions".to_string(),
        serde_json::to_value(&ut_result)?,
    );

    let sig_result = sig_dsl::signatures
        .filter(sig_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(sig_dsl::transaction_version.asc())
        .then_order_by(sig_dsl::multi_sig_index)
        .load::<Signature>(conn)?;
    result_map.insert("signatures".to_string(), serde_json::to_value(&sig_result)?);

    Ok(result_map)
}
