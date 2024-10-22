use super::database::ArcDbPool;
use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    db::common::models::{
        backfill_processor_status::{BackfillProcessorStatusQuery, BackfillStatus},
        processor_status::ProcessorStatusQuery,
    },
};
use anyhow::{Context, Result};

/// Get the appropriate starting version for the processor.
///
/// If it is a regular processor, this will return the higher of the checkpointed version,
/// or `staring_version` from the config, or 0 if not set.
///
/// If this is a backfill processor and threre is an in-progress backfill, this will return
/// the checkpointed version + 1.
///
/// If this is a backfill processor and there is not an in-progress backfill (i.e., no checkpoint or
/// backfill status is COMPLETE), this will return `starting_version` from the config, or 0 if not set.
pub async fn get_starting_version(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<u64> {
    // Check if there's a checkpoint in the approrpiate processor status table.
    let latest_processed_version =
        get_latest_processed_version_from_db(indexer_processor_config, conn_pool)
            .await
            .context("Failed to get latest processed version from DB")?;

    // If nothing checkpointed, return the `starting_version` from the config, or 0 if not set.
    Ok(latest_processed_version.unwrap_or(
        indexer_processor_config
            .transaction_stream_config
            .starting_version
            .unwrap_or(0),
    ))
}

async fn get_latest_processed_version_from_db(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<Option<u64>> {
    let mut conn = conn_pool.get().await?;

    if let Some(backfill_config) = &indexer_processor_config.backfill_config {
        let backfill_status = BackfillProcessorStatusQuery::get_by_processor(
            &backfill_config.backfill_alias,
            &mut conn,
        )
        .await
        .context("Failed to query backfill_processor_status table.")?;

        // Return None if there is no checkpoint or if the backfill is old (complete).
        // Otherwise, return the checkpointed version + 1.
        return Ok(
            backfill_status.and_then(|status| match status.backfill_status {
                BackfillStatus::InProgress => Some(status.last_success_version as u64 + 1),
                // If status is Complete, this is the start of a new backfill job.
                BackfillStatus::Complete => None,
            }),
        );
    }

    let status = ProcessorStatusQuery::get_by_processor(
        indexer_processor_config.processor_config.name(),
        &mut conn,
    )
    .await
    .context("Failed to query processor_status table.")?;

    // Return None if there is no checkpoint. Otherwise,
    // return the higher of the checkpointed version + 1 and `starting_version`.
    Ok(status.map(|status| {
        std::cmp::max(
            status.last_success_version as u64 + 1,
            indexer_processor_config
                .transaction_stream_config
                .starting_version
                .unwrap_or(0),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::{BackfillConfig, IndexerProcessorConfig},
            processor_config::{DefaultProcessorConfig, ProcessorConfig},
        },
        db::common::models::{
            backfill_processor_status::{BackfillProcessorStatus, BackfillStatus},
            processor_status::ProcessorStatus,
        },
        utils::database::{new_db_pool, run_migrations},
    };
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
    use aptos_indexer_testing_framework::database::{PostgresTestDatabase, TestDatabase};
    use diesel_async::RunQueryDsl;
    use processor::schema::processor_status;
    use std::collections::HashSet;
    use url::Url;

    fn create_indexer_config(
        db_url: String,
        backfill_config: Option<BackfillConfig>,
        starting_version: Option<u64>,
    ) -> IndexerProcessorConfig {
        let default_processor_config = DefaultProcessorConfig {
            per_table_chunk_sizes: AHashMap::new(),
            channel_size: 100,
            deprecated_tables: HashSet::new(),
        };
        let processor_config = ProcessorConfig::EventsProcessor(default_processor_config);
        let postgres_config = PostgresConfig {
            connection_string: db_url.to_string(),
            db_pool_size: 100,
        };
        let db_config = DbConfig::PostgresConfig(postgres_config);
        IndexerProcessorConfig {
            db_config,
            transaction_stream_config: TransactionStreamConfig {
                indexer_grpc_data_service_address: Url::parse("https://test.com").unwrap(),
                starting_version,
                request_ending_version: None,
                auth_token: "test".to_string(),
                request_name_header: "test".to_string(),
                indexer_grpc_http2_ping_interval_secs: 1,
                indexer_grpc_http2_ping_timeout_secs: 1,
                indexer_grpc_reconnection_timeout_secs: 1,
                indexer_grpc_response_item_timeout_secs: 1,
            },
            processor_config,
            backfill_config,
        }
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_starting_version_no_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, None);
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 0)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_starting_version_no_checkpoint_with_start_ver() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, Some(5));
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 5)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_starting_version_with_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, None);
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor_status::table)
            .values(ProcessorStatus {
                processor: indexer_processor_config.processor_config.name().to_string(),
                last_success_version: 10,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 11)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_get_starting_version_with_completed_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        let backfill_alias = "backfill_processor".to_string();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_alias: backfill_alias.clone(),
            }),
            None,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: backfill_alias.clone(),
                backfill_status: BackfillStatus::Complete,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: 10,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 0)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_get_starting_version_with_inprogress_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        let backfill_alias = "backfill_processor".to_string();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_alias: backfill_alias.clone(),
            }),
            None,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: backfill_alias.clone(),
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: 10,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 11)
    }
}
