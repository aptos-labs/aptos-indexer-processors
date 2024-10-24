use crate::models::stake_models::{
    CurrentDelegatedVoter, CurrentDelegatorBalance, CurrentDelegatorPoolBalance,
    CurrentStakingPoolVoter, DelegatedStakingActivity, DelegatorBalance, DelegatorPool,
    DelegatorPoolBalance, ProposalVote,
};
use anyhow::Result;
use diesel::{pg::PgConnection, query_dsl::methods::FilterDsl, ExpressionMethods, RunQueryDsl};
use processor::schema::{
    current_delegated_staking_pool_balances::dsl as cdsp_dsl,
    current_delegated_voter::dsl as cdv_dsl, current_delegator_balances::dsl as cdb_dsl,
    current_staking_pool_voter::dsl as cspv_dsl, delegated_staking_activities::dsl as dsa_dsl,
    delegated_staking_pool_balances::dsl as dspb_dsl, delegated_staking_pools::dsl as dsp_dsl,
    delegator_balances::dsl as dp_dsl, proposal_votes::dsl as pv_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let cspv_result = cspv_dsl::current_staking_pool_voter
        .filter(cspv_dsl::last_transaction_version.eq_any(&txn_versions))
        .load::<CurrentStakingPoolVoter>(conn)?;
    result_map.insert(
        "current_staking_pool_voter".to_string(),
        serde_json::to_value(&cspv_result)?,
    );

    let pv_result = pv_dsl::proposal_votes
        .filter(pv_dsl::transaction_version.eq_any(&txn_versions))
        .load::<ProposalVote>(conn)?;
    result_map.insert(
        "proposal_votes".to_string(),
        serde_json::to_value(&pv_result)?,
    );

    let dsa_result = dsa_dsl::delegated_staking_activities
        .filter(dsa_dsl::transaction_version.eq_any(&txn_versions))
        .load::<DelegatedStakingActivity>(conn)?;
    result_map.insert(
        "delegated_staking_activities".to_string(),
        serde_json::to_value(&dsa_result)?,
    );

    let dp_result = dp_dsl::delegator_balances
        .filter(dp_dsl::transaction_version.eq_any(&txn_versions))
        .load::<DelegatorBalance>(conn)?;
    result_map.insert(
        "delegator_balances".to_string(),
        serde_json::to_value(&dp_result)?,
    );

    let cdb_result = cdb_dsl::current_delegator_balances
        .filter(cdb_dsl::last_transaction_version.eq_any(&txn_versions))
        .load::<CurrentDelegatorBalance>(conn)?;
    result_map.insert(
        "current_delegator_balances".to_string(),
        serde_json::to_value(&cdb_result)?,
    );

    let dsp_result = dsp_dsl::delegated_staking_pools
        .filter(dsp_dsl::first_transaction_version.eq_any(&txn_versions))
        .load::<DelegatorPool>(conn)?;
    result_map.insert(
        "delegated_staking_pools".to_string(),
        serde_json::to_value(&dsp_result)?,
    );

    let dspb_result = dspb_dsl::delegated_staking_pool_balances
        .filter(dspb_dsl::transaction_version.eq_any(&txn_versions))
        .load::<DelegatorPoolBalance>(conn)?;
    result_map.insert(
        "delegated_staking_pool_balances".to_string(),
        serde_json::to_value(&dspb_result)?,
    );

    let cdsp_result = cdsp_dsl::current_delegated_staking_pool_balances
        .filter(cdsp_dsl::last_transaction_version.eq_any(&txn_versions))
        .load::<CurrentDelegatorPoolBalance>(conn)?;
    result_map.insert(
        "current_delegated_staking_pool_balances".to_string(),
        serde_json::to_value(&cdsp_result)?,
    );

    let cdv_result = cdv_dsl::current_delegated_voter
        .filter(cdv_dsl::last_transaction_version.eq_any(&txn_versions))
        .load::<CurrentDelegatedVoter>(conn)?;
    result_map.insert(
        "current_delegated_voter".to_string(),
        serde_json::to_value(&cdv_result)?,
    );

    Ok(result_map)
}
