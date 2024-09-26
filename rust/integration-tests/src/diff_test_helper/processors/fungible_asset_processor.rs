use crate::{
    diff_test_helper::ProcessorTestHelper, models::queryable_models::FungibleAssetActivity,
};
use anyhow::Result;
use diesel::{pg::PgConnection, query_dsl::methods::FilterDsl, ExpressionMethods, RunQueryDsl};
use serde_json::Value;
use processor::schema::fungible_asset_activities::dsl::*;

pub struct FungibleAssetProcessorTestHelper;

impl ProcessorTestHelper for FungibleAssetProcessorTestHelper {
    fn load_data(&self, conn: &mut PgConnection, txn_version: &str) -> Result<Value> {


        let fungible_asset_activities_result = fungible_asset_activities
            .filter(transaction_version.eq(txn_version.parse::<i64>().unwrap()))
            .load::<FungibleAssetActivity>(conn);

        let all_fungible_asset_activities = fungible_asset_activities_result?;
        let json_data = serde_json::to_string_pretty(&all_fungible_asset_activities)?;
        Ok(serde_json::from_str(&json_data)?)
    }
}
