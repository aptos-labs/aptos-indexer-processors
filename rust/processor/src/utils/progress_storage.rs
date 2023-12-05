// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::database::{execute_with_better_error, PgDbPool};
use crate::{
    models::{
        ledger_info::LedgerInfo,
        processor_status::{update_last_processed_version, ProcessorStatusQuery},
    },
    schema::ledger_infos,
};
use anyhow::{Context, Result};
use aptos_processor_sdk::progress_storage::ProgressStorageTrait;
use async_trait::async_trait;

pub struct DieselProgressStorage {
    db_pool: PgDbPool,
}

impl DieselProgressStorage {
    pub fn new(db_pool: PgDbPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl ProgressStorageTrait for DieselProgressStorage {
    async fn read_chain_id(&self) -> Result<Option<u8>> {
        let mut conn = self.db_pool.get().await?;
        Ok(LedgerInfo::get(&mut conn)
            .await?
            .map(|li| li.chain_id as u8))
    }

    async fn write_chain_id(&self, chain_id: u8) -> Result<()> {
        let mut conn = self.db_pool.get().await?;
        execute_with_better_error(
            &mut conn,
            diesel::insert_into(ledger_infos::table)
                .values(LedgerInfo {
                    chain_id: chain_id as i64,
                })
                .on_conflict_do_nothing(),
            None,
        )
        .await
        .context("[Parser] Error updating chain_id!")?;
        Ok(())
    }

    async fn read_last_processed_version(&self, processor_name: &str) -> Result<Option<u64>> {
        let mut conn = self.db_pool.get().await?;
        match ProcessorStatusQuery::get_by_processor(processor_name, &mut conn).await? {
            Some(status) => Ok(Some(status.last_success_version as u64)),
            None => Ok(None),
        }
    }

    async fn write_last_processed_version(&self, processor_name: &str, version: u64) -> Result<()> {
        let conn = self.db_pool.get().await?;
        update_last_processed_version(conn, processor_name.to_string(), version)
            .await
            .context("Failed to update latest processed version")?;
        Ok(())
    }
}
