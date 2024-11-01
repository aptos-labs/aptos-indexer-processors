use crate::models::account_transaction_models::AccountTransaction;
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::account_transactions::dsl::*;
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let acc_txn_result = account_transactions
        .filter(transaction_version.eq_any(&txn_versions))
        .then_order_by(transaction_version.asc())
        .then_order_by(inserted_at.asc())
        .then_order_by(account_address.asc())
        .load::<AccountTransaction>(conn)?;

    result_map.insert(
        "account_transactions".to_string(),
        serde_json::to_value(&acc_txn_result)?,
    );

    Ok(result_map)
}
