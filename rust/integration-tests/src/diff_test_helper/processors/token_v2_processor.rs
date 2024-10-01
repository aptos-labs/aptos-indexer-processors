use crate::{diff_test_helper::ProcessorTestHelper, models::queryable_models::TokenActivityV2};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::token_activities_v2::dsl::*;
use serde_json::Value;

pub struct TokenV2ProcessorTestHelper;

impl ProcessorTestHelper for TokenV2ProcessorTestHelper {
    fn load_data(&self, conn: &mut PgConnection, txn_version: &str) -> Result<Value> {
        let token_activities_v2_result = token_activities_v2
            .filter(transaction_version.eq(txn_version.parse::<i64>().unwrap()))
            .then_order_by(event_index.asc())
            .load::<TokenActivityV2>(conn);

        let all_collections_v2 = token_activities_v2_result?;
        let json_data = serde_json::to_string_pretty(&all_collections_v2)?;
        Ok(serde_json::from_str(&json_data)?)
    }
}
