use crate::{diff_test_helper::ProcessorTestHelper, models::queryable_models::Event};
use anyhow::Result;
use diesel::{pg::PgConnection, query_dsl::methods::FilterDsl, ExpressionMethods, RunQueryDsl};
use serde_json::Value;
use processor::schema::events::dsl::*;

// Example implementation for the EventsProcessor
pub struct EventsProcessorTestHelper;

impl ProcessorTestHelper for EventsProcessorTestHelper {
    fn load_data(&self, conn: &mut PgConnection, txn_version: &str) -> Result<Value> {
        let events_result = events
            .filter(transaction_version.eq(txn_version.parse::<i64>().unwrap()))
            .load::<Event>(conn);

        let all_events = events_result?;
        let json_data = serde_json::to_string_pretty(&all_events)?;
        Ok(serde_json::from_str(&json_data)?)
    }
}
