use crate::models::token_v2_models::TokenActivityV2;
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::token_activities_v2::dsl::*;
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let token_activities_v2_result = token_activities_v2
        .filter(transaction_version.eq_any(&txn_versions))
        .then_order_by(transaction_version.asc())
        .then_order_by(event_index.asc())
        .load::<TokenActivityV2>(conn);

    let all_token_activities_v2 = token_activities_v2_result?;
    let token_activities_v2_json_data = serde_json::to_string_pretty(&all_token_activities_v2)?;

    result_map.insert(
        "token_activities_v2".to_string(),
        serde_json::from_str(&token_activities_v2_json_data)?,
    );

    Ok(result_map)
}
