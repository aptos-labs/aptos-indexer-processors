use super::database::ArcDbPool;
use crate::{
    db::common::models::ledger_info::LedgerInfo, schema::ledger_infos,
    utils::database::execute_with_better_error_conn,
};
use anyhow::{Context, Result};
use tracing::info;

/// Verify the chain id from GRPC against the database.
pub async fn check_or_update_chain_id(grpc_chain_id: i64, db_pool: ArcDbPool) -> Result<u64> {
    info!("Checking if chain id is correct");
    let mut conn = db_pool.get().await?;

    let maybe_existing_chain_id = LedgerInfo::get(&mut conn).await?.map(|li| li.chain_id);

    match maybe_existing_chain_id {
        Some(chain_id) => {
            anyhow::ensure!(chain_id == grpc_chain_id, "Wrong chain detected! Trying to index chain {} now but existing data is for chain {}", grpc_chain_id, chain_id);
            info!(
                chain_id = chain_id,
                "Chain id matches! Continue to index...",
            );
            Ok(chain_id as u64)
        },
        None => {
            info!(
                chain_id = grpc_chain_id,
                "Adding chain id to db, continue to index..."
            );
            execute_with_better_error_conn(
                &mut conn,
                diesel::insert_into(ledger_infos::table)
                    .values(LedgerInfo {
                        chain_id: grpc_chain_id,
                    })
                    .on_conflict_do_nothing(),
                None,
            )
            .await
            .context("Error updating chain_id!")
            .map(|_| grpc_chain_id as u64)
        },
    }
}
