use super::database::ArcDbPool;
use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    db::common::models::{
        backfill_processor_status::{
            BackfillProcessorStatus, BackfillProcessorStatusQuery, BackfillStatus,
        },
        processor_status::ProcessorStatusQuery,
    },
    utils::database::execute_with_better_error,
};
use anyhow::{Context, Result};
use diesel::{result::Error as DieselError, upsert::excluded, ExpressionMethods};
use processor::schema::backfill_processor_status;

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
    // Check if there's a checkpoint in the appropriate processor status table.
    let latest_processed_version =
        get_starting_version_from_db(indexer_processor_config, conn_pool)
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

/// Get the appropriate minimum last success version for the parquet processors.
///
/// This will return the minimum of the last success version of the processors in the list.
/// If no processor has a checkpoint, this will return the `starting_version` from the config, or 0 if not set.
pub async fn get_min_last_success_version_parquet(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
    table_names: Vec<String>,
) -> Result<u64> {
    let min_processed_version = if indexer_processor_config.backfill_config.is_some() {
        get_starting_version_from_db(indexer_processor_config, conn_pool.clone())
            .await
            .context("Failed to get latest processed version from DB")?
    } else {
        get_min_processed_version_from_db(conn_pool.clone(), table_names)
            .await
            .context("Failed to get minimum last success version from DB")?
    };

    let config_starting_version = indexer_processor_config
        .transaction_stream_config
        .starting_version
        .unwrap_or(0);

    if let Some(min_processed_version) = min_processed_version {
        Ok(std::cmp::max(
            min_processed_version,
            config_starting_version,
        ))
    } else {
        Ok(config_starting_version)
    }
}

/// Get the minimum last success version from the database for the given processors.
///
/// This should return the minimum of the last success version of the processors in the list.
/// If any of the tables handled by the parquet processor has no entry, it should use 0 as a default value.
/// To avoid skipping any versions, the minimum of the last success version should be used as the starting version.
async fn get_min_processed_version_from_db(
    conn_pool: ArcDbPool,
    table_names: Vec<String>,
) -> Result<Option<u64>> {
    let mut queries = Vec::new();

    // Spawn all queries concurrently with separate connections
    for processor_name in table_names {
        let conn_pool = conn_pool.clone();
        let processor_name = processor_name.clone();

        let query = async move {
            let mut conn = conn_pool.get().await.map_err(|err| {
                // the type returned by conn_pool.get().await? does not have an appropriate error type that can be converted into diesel::result::Error.
                // In this case, the error type is bb8::api::RunError<PoolError>, but the ? operator is trying to convert it to diesel::result::Error, which fails because there's no conversion implemented between those types.
                // so we convert the error type from connection pool into a type that Diesel API expects.
                DieselError::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(err.to_string()),
                )
            })?;
            ProcessorStatusQuery::get_by_processor(&processor_name, &mut conn).await
        };

        queries.push(query);
    }

    let results = futures::future::join_all(queries).await;

    // Collect results and find the minimum processed version
    let min_processed_version = results
        .into_iter()
        .filter_map(|res| {
            match res {
                // If the result is `Ok`, proceed to check the status
                Ok(Some(status)) => {
                    // Return the version if the status contains a version
                    Some(status.last_success_version as u64)
                },
                // Handle specific cases where `Ok` contains `None` (no status found)
                Ok(None) => None,
                // TODO: If the result is an `Err`, what should we do?
                Err(e) => {
                    eprintln!("Error fetching processor status: {:?}", e);
                    None
                },
            }
        })
        .min();

    Ok(min_processed_version)
}

async fn get_starting_version_from_db(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<Option<u64>> {
    let mut conn = conn_pool.get().await?;

    if let Some(backfill_config) = &indexer_processor_config.backfill_config {
        let backfill_status_option = BackfillProcessorStatusQuery::get_by_processor(
            &backfill_config.backfill_alias,
            &mut conn,
        )
        .await
        .context("Failed to query backfill_processor_status table.")?;

        // Return None if there is no checkpoint or if the backfill is old (complete).
        // Otherwise, return the checkpointed version + 1.
        if let Some(status) = backfill_status_option {
            match status.backfill_status {
                BackfillStatus::InProgress => {
                    return Ok(Some(status.last_success_version as u64 + 1));
                },
                // If status is Complete, this is the start of a new backfill job.
                BackfillStatus::Complete => {
                    let backfill_alias = status.backfill_alias.clone();
                    let backfill_end_version_mapped = status.backfill_end_version;
                    let status = BackfillProcessorStatus {
                        backfill_alias,
                        backfill_status: BackfillStatus::InProgress,
                        last_success_version: 0,
                        last_transaction_timestamp: None,
                        backfill_start_version: indexer_processor_config
                            .transaction_stream_config
                            .starting_version
                            .unwrap_or(0) as i64,
                        backfill_end_version: backfill_end_version_mapped,
                    };
                    execute_with_better_error(
                        conn_pool.clone(),
                        diesel::insert_into(backfill_processor_status::table)
                            .values(&status)
                            .on_conflict(backfill_processor_status::backfill_alias)
                            .do_update()
                            .set((
                                backfill_processor_status::backfill_status
                                    .eq(excluded(backfill_processor_status::backfill_status)),
                                backfill_processor_status::last_success_version
                                    .eq(excluded(backfill_processor_status::last_success_version)),
                                backfill_processor_status::last_updated
                                    .eq(excluded(backfill_processor_status::last_updated)),
                                backfill_processor_status::last_transaction_timestamp.eq(excluded(
                                    backfill_processor_status::last_transaction_timestamp,
                                )),
                                backfill_processor_status::backfill_start_version.eq(excluded(
                                    backfill_processor_status::backfill_start_version,
                                )),
                                backfill_processor_status::backfill_end_version
                                    .eq(excluded(backfill_processor_status::backfill_end_version)),
                            )),
                        None,
                    )
                    .await?;
                    return Ok(None);
                },
            }
        } else {
            return Ok(None);
        }
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
            status.last_success_version as u64,
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
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::{
        utils::AdditionalHeaders, TransactionStreamConfig,
    };
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
                additional_headers: AdditionalHeaders::default(),
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
                backfill_end_version: Some(10),
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
                backfill_end_version: Some(10),
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
    async fn test_get_min_last_success_version_parquet_no_checkpoints() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, Some(0));
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;

        let processor_names = vec!["processor_1".to_string(), "processor_2".to_string()];

        let min_version = get_min_last_success_version_parquet(
            &indexer_processor_config,
            conn_pool,
            processor_names,
        )
        .await
        .unwrap();

        assert_eq!(min_version, 0);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_min_last_success_version_parquet_with_checkpoints() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, Some(0));
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;

        // Insert processor statuses with different last_success_version values
        diesel::insert_into(processor::schema::processor_status::table)
            .values(vec![
                ProcessorStatus {
                    processor: "processor_1".to_string(),
                    last_success_version: 10,
                    last_transaction_timestamp: None,
                },
                ProcessorStatus {
                    processor: "processor_2".to_string(),
                    last_success_version: 5,
                    last_transaction_timestamp: None,
                },
            ])
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let processor_names = vec!["processor_1".to_string(), "processor_2".to_string()];

        let min_version = get_min_last_success_version_parquet(
            &indexer_processor_config,
            conn_pool,
            processor_names,
        )
        .await
        .unwrap();

        assert_eq!(min_version, 5);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_min_last_success_version_parquet_with_partial_checkpoints() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(db.get_db_url(), None, Some(0));
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;

        // Insert processor statuses with different last_success_version values
        diesel::insert_into(processor::schema::processor_status::table)
            .values(vec![ProcessorStatus {
                processor: "processor_1".to_string(),
                last_success_version: 15,
                last_transaction_timestamp: None,
            }])
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let processor_names = vec![
            "processor_1".to_string(),
            "processor_2".to_string(), // No checkpoint for processor_2
        ];

        let min_version = get_min_last_success_version_parquet(
            &indexer_processor_config,
            conn_pool,
            processor_names,
        )
        .await
        .unwrap();

        // Since processor_2 has no checkpoint, the minimum version should be the starting version of processor_1
        assert_eq!(min_version, 0);
    }
}
