use crate::models::default_models::{BlockMetadataTransaction, CurrentTableItem, TableItem};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{
    block_metadata_transactions::dsl as bmt_dsl, current_table_items::dsl as cti_dsl,
    table_items::dsl as ti_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let bmt_result = bmt_dsl::block_metadata_transactions
        .filter(bmt_dsl::version.eq_any(&txn_versions))
        .then_order_by(bmt_dsl::version.asc())
        .then_order_by(bmt_dsl::block_height.asc())
        .load::<BlockMetadataTransaction>(conn)?;
    result_map.insert(
        "block_metadata_transactions".to_string(),
        serde_json::to_value(&bmt_result)?,
    );

    let ti_result = ti_dsl::table_items
        .filter(ti_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(ti_dsl::transaction_version.asc())
        .load::<TableItem>(conn)?;
    result_map.insert("table_items".to_string(), serde_json::to_value(&ti_result)?);

    let cti_result = cti_dsl::current_table_items
        .filter(cti_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(cti_dsl::last_transaction_version.asc())
        .load::<CurrentTableItem>(conn)?;
    result_map.insert(
        "current_table_items".to_string(),
        serde_json::to_value(&cti_result)?,
    );

    Ok(result_map)
}
