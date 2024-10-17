use crate::models::objects_models::{CurrentObject, Object};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{current_objects::dsl as co_dsl, objects::dsl as o_dsl};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let objects_result = o_dsl::objects
        .filter(o_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(o_dsl::transaction_version.asc())
        .load::<Object>(conn)?;
    result_map.insert(
        "objects".to_string(),
        serde_json::to_value(&objects_result)?,
    );

    let current_objects_result = co_dsl::current_objects
        .filter(co_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(co_dsl::last_transaction_version.asc())
        .load::<CurrentObject>(conn)?;
    result_map.insert(
        "current_objects".to_string(),
        serde_json::to_value(&current_objects_result)?,
    );

    // Return the result map
    Ok(result_map)
}
