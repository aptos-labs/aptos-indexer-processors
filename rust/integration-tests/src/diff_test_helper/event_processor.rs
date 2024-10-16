use crate::queryable_models::events_models::Event;
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::events::dsl::*;
use serde_json::Value;
use std::collections::HashMap;

pub fn load_data(conn: &mut PgConnection, txn_version: i64) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let events_result = events
        .filter(transaction_version.eq(txn_version))
        .then_order_by(event_index.asc())
        .load::<Event>(conn);

    let all_events = events_result?;
    let events_json_data = serde_json::to_string_pretty(&all_events)?;

    result_map.insert(
        "events".to_string(),
        serde_json::from_str(&events_json_data)?,
    );

    Ok(result_map)
}
