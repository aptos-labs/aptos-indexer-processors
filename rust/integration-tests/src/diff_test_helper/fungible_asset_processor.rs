use crate::queryable_models::fungible_asset_models::{
    CoinSupply, CurrentFungibleAssetBalance, FungibleAssetActivity, FungibleAssetBalance,
    FungibleAssetMetadataModel,
};
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{
    coin_supply::dsl as cs_dsl, current_fungible_asset_balances_legacy::dsl as cfab_dsl,
    fungible_asset_activities::dsl as faa_dsl, fungible_asset_balances::dsl as fab_dsl,
    fungible_asset_metadata::dsl as fam_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(conn: &mut PgConnection, txn_version: &str) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    // Query from fungible_asset_activities
    let fungible_asset_activities_result = faa_dsl::fungible_asset_activities
        .filter(faa_dsl::transaction_version.eq(txn_version.parse::<i64>().unwrap()))
        .then_order_by(faa_dsl::event_index.asc())
        .load::<FungibleAssetActivity>(conn);
    let all_fungible_asset_activities = fungible_asset_activities_result?;
    let fungible_asset_activities_json =
        serde_json::to_string_pretty(&all_fungible_asset_activities)?;
    result_map.insert(
        "fungible_asset_activities".to_string(),
        serde_json::from_str(&fungible_asset_activities_json)?,
    );

    // Query from fungible_asset_metadata
    let fungible_asset_metadata_result = fam_dsl::fungible_asset_metadata
        .filter(fam_dsl::last_transaction_version.eq(txn_version.parse::<i64>().unwrap()))
        .load::<FungibleAssetMetadataModel>(conn);
    let all_fungible_asset_metadata = fungible_asset_metadata_result?;
    let fungible_asset_metadata_json = serde_json::to_string_pretty(&all_fungible_asset_metadata)?;
    result_map.insert(
        "fungible_asset_metadata".to_string(),
        serde_json::from_str(&fungible_asset_metadata_json)?,
    );

    // Query from fungible_asset_balances
    let fungible_asset_balances_result = fab_dsl::fungible_asset_balances
        .filter(fab_dsl::transaction_version.eq(txn_version.parse::<i64>().unwrap()))
        .load::<FungibleAssetBalance>(conn);
    let all_fungible_asset_balances = fungible_asset_balances_result?;
    let fungible_asset_balances_json = serde_json::to_string_pretty(&all_fungible_asset_balances)?;
    result_map.insert(
        "fungible_asset_balances".to_string(),
        serde_json::from_str(&fungible_asset_balances_json)?,
    );

    // Query from current_fungible_asset_balances
    let current_fungible_asset_balances_result = cfab_dsl::current_fungible_asset_balances_legacy
        .filter(cfab_dsl::last_transaction_version.eq(txn_version.parse::<i64>().unwrap()))
        .load::<CurrentFungibleAssetBalance>(conn);
    let all_current_fungible_asset_balances = current_fungible_asset_balances_result?;
    let current_fungible_asset_balances_json =
        serde_json::to_string_pretty(&all_current_fungible_asset_balances)?;
    result_map.insert(
        "current_fungible_asset_balances".to_string(),
        serde_json::from_str(&current_fungible_asset_balances_json)?,
    );

    // Query from current_unified_fungible_asset_balances
    // TODO: handle v1 and v2
    // let current_unified_fungible_asset_balances_result = cu_fab_dsl::current_fungible_asset_balances
    //     .filter(cu_fab_dsl::transaction_version.eq(txn_version.parse::<i64>().unwrap()))
    //     .load::<CurrentUnifiedFungibleAssetBalance>(conn);
    // let all_current_unified_fungible_asset_balances = current_unified_fungible_asset_balances_result?;
    // let current_unified_fungible_asset_balances_json = serde_json::to_string_pretty(&all_current_unified_fungible_asset_balances)?;
    // result_map.insert(
    //     "current_unified_fungible_asset_balances".to_string(),
    //     serde_json::from_str(&current_unified_fungible_asset_balances_json)?,
    // );

    // Query from coin_supply
    let coin_supply_result = cs_dsl::coin_supply
        .filter(cs_dsl::transaction_version.eq(txn_version.parse::<i64>().unwrap()))
        .load::<CoinSupply>(conn);
    let all_coin_supply = coin_supply_result?;
    let coin_supply_json = serde_json::to_string_pretty(&all_coin_supply)?;
    result_map.insert(
        "coin_supply".to_string(),
        serde_json::from_str(&coin_supply_json)?,
    );

    // Return the complete map containing all table data
    Ok(result_map)
}
