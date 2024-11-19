use crate::models::token_v2_models::*;
use anyhow::Result;
use diesel::{
    pg::PgConnection,
    query_dsl::methods::{FilterDsl, ThenOrderDsl},
    ExpressionMethods, RunQueryDsl,
};
use processor::schema::{
    collections_v2::dsl as cv2_dsl, current_collections_v2::dsl as ccv2_dsl,
    current_token_datas_v2::dsl as ctdv2_dsl, current_token_ownerships_v2::dsl as ctov2_dsl,
    current_token_pending_claims::dsl as ctpc_dsl, current_token_v2_metadata::dsl as ctmv2_dsl,
    token_activities_v2::dsl as tav2_dsl, token_datas_v2::dsl as tdv2_dsl,
    token_ownerships_v2::dsl as tov2_dsl,
};
use serde_json::Value;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn load_data(
    conn: &mut PgConnection,
    txn_versions: Vec<i64>,
) -> Result<HashMap<String, Value>> {
    let mut result_map: HashMap<String, Value> = HashMap::new();

    let token_activities_v2_result = tav2_dsl::token_activities_v2
        .filter(tav2_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(tav2_dsl::transaction_version.asc())
        .then_order_by(tav2_dsl::event_index.asc())
        .load::<TokenActivityV2>(conn);

    let all_token_activities_v2 = token_activities_v2_result?;
    let token_activities_v2_json_data = serde_json::to_string_pretty(&all_token_activities_v2)?;

    result_map.insert(
        "token_activities_v2".to_string(),
        serde_json::from_str(&token_activities_v2_json_data)?,
    );

    let token_datas_v2_result = tdv2_dsl::token_datas_v2
        .filter(tdv2_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(tdv2_dsl::transaction_version.asc())
        .then_order_by(tdv2_dsl::write_set_change_index.asc())
        .load::<TokenDataV2>(conn);

    let all_token_datas_v2 = token_datas_v2_result?;
    let token_datas_v2_json_data = serde_json::to_string_pretty(&all_token_datas_v2)?;

    result_map.insert(
        "token_datas_v2".to_string(),
        serde_json::from_str(&token_datas_v2_json_data)?,
    );

    let token_ownerships_v2_result = tov2_dsl::token_ownerships_v2
        .filter(tov2_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(tov2_dsl::transaction_version.asc())
        .then_order_by(tov2_dsl::write_set_change_index.asc())
        .load::<TokenOwnershipV2>(conn);

    let all_token_ownerships_v2 = token_ownerships_v2_result?;
    let token_ownerships_v2_json_data = serde_json::to_string_pretty(&all_token_ownerships_v2)?;

    result_map.insert(
        "token_ownerships_v2".to_string(),
        serde_json::from_str(&token_ownerships_v2_json_data)?,
    );

    let current_token_v2_metadata_result = ctmv2_dsl::current_token_v2_metadata
        .filter(ctmv2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(ctmv2_dsl::object_address.asc())
        .then_order_by(ctmv2_dsl::resource_type.asc())
        .load::<CurrentTokenV2Metadata>(conn);

    let all_current_token_v2_metadata = current_token_v2_metadata_result?;
    let current_token_v2_metadata_json_data =
        serde_json::to_string_pretty(&all_current_token_v2_metadata)?;

    result_map.insert(
        "current_token_v2_metadata".to_string(),
        serde_json::from_str(&current_token_v2_metadata_json_data)?,
    );

    let collections_v2_result = cv2_dsl::collections_v2
        .filter(cv2_dsl::transaction_version.eq_any(&txn_versions))
        .then_order_by(cv2_dsl::transaction_version.asc())
        .then_order_by(cv2_dsl::write_set_change_index.asc())
        .load::<CollectionV2>(conn);

    let all_collections_v2 = collections_v2_result?;
    let collections_v2_json_data = serde_json::to_string_pretty(&all_collections_v2)?;

    result_map.insert(
        "collections_v2".to_string(),
        serde_json::from_str(&collections_v2_json_data)?,
    );

    let current_collections_v2_result = ccv2_dsl::current_collections_v2
        .filter(ccv2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(ccv2_dsl::collection_id.asc())
        .load::<CurrentCollectionV2>(conn);

    let all_current_collections_v2 = current_collections_v2_result?;
    let current_collections_v2_json_data =
        serde_json::to_string_pretty(&all_current_collections_v2)?;

    result_map.insert(
        "current_collections_v2".to_string(),
        serde_json::from_str(&current_collections_v2_json_data)?,
    );

    let current_token_datas_v2_result = ctdv2_dsl::current_token_datas_v2
        .filter(ctdv2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(ctdv2_dsl::token_data_id.asc())
        .load::<CurrentTokenDataV2>(conn);

    let all_current_token_datas_v2 = current_token_datas_v2_result?;
    let current_token_datas_v2_json_data =
        serde_json::to_string_pretty(&all_current_token_datas_v2)?;

    result_map.insert(
        "current_token_datas_v2".to_string(),
        serde_json::from_str(&current_token_datas_v2_json_data)?,
    );

    let current_token_ownerships_v2_result = ctov2_dsl::current_token_ownerships_v2
        .filter(ctov2_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(ctov2_dsl::token_data_id.asc())
        .then_order_by(ctov2_dsl::property_version_v1.asc())
        .then_order_by(ctov2_dsl::owner_address.asc())
        .then_order_by(ctov2_dsl::storage_id.asc())
        .load::<CurrentTokenOwnershipV2>(conn);

    let all_current_token_ownerships_v2 = current_token_ownerships_v2_result?;
    let current_token_ownerships_v2_json_data =
        serde_json::to_string_pretty(&all_current_token_ownerships_v2)?;

    result_map.insert(
        "current_token_ownerships_v2".to_string(),
        serde_json::from_str(&current_token_ownerships_v2_json_data)?,
    );

    let current_token_pending_claims_result = ctpc_dsl::current_token_pending_claims
        .filter(ctpc_dsl::last_transaction_version.eq_any(&txn_versions))
        .then_order_by(ctpc_dsl::token_data_id_hash.asc())
        .then_order_by(ctpc_dsl::property_version.asc())
        .then_order_by(ctpc_dsl::from_address.asc())
        .then_order_by(ctpc_dsl::to_address.asc())
        .load::<CurrentTokenPendingClaim>(conn);

    let all_current_token_pending_claims = current_token_pending_claims_result?;
    let current_token_pending_claims_json_data =
        serde_json::to_string_pretty(&all_current_token_pending_claims)?;

    result_map.insert(
        "current_token_pending_claims".to_string(),
        serde_json::from_str(&current_token_pending_claims_json_data)?,
    );

    Ok(result_map)
}
