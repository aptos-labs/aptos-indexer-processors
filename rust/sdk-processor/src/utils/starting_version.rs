use super::database::ArcDbPool;
use crate::{
    config::indexer_processor_config::{IndexerProcessorConfig, ProcessorMode},
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
/// If it is a regular (`mode == default`) processor, this will return the higher of the (checkpointed version,
/// `initial_starting_version` || 0) from the bootstrap config.
///
/// If this is a backfill processor and there is an in-progress backfill, this will return
/// the checkpointed version + 1.
///
/// If this is a backfill processor and there is not an in-progress backfill (i.e., no checkpoint or
/// backfill status is COMPLETE), this will return `intial_starting_version` from the backfill config.
///
/// If the backfill status is IN_PROGRESS, and `backfill_config.overwrite_checkpoint` is `true`, this will return
/// `initial_starting_version` from the backfill config, or 0 if not set. This allows users to restart a
/// backfill job if they want to.
pub async fn get_starting_version(
    indexer_processor_config: &IndexerProcessorConfig,
    conn_pool: ArcDbPool,
) -> Result<u64> {
    let mut conn = conn_pool.get().await?;

    // Determine processor type.
    match indexer_processor_config.mode {
        ProcessorMode::Backfill => {
            let backfill_config = indexer_processor_config.backfill_config.clone().unwrap();
            let backfill_status_option = BackfillProcessorStatusQuery::get_by_processor(
                indexer_processor_config.processor_config.name(),
                &backfill_config.backfill_id,
                &mut conn,
            )
            .await
            .context("Failed to query backfill_processor_status table.")?;
            // Return None if there is no checkpoint, if the backfill is old (complete), or if overwrite_checkpoint is true.
            // Otherwise, return the checkpointed version + 1.
            if let Some(status) = backfill_status_option {
                // If the backfill is complete and overwrite_checkpoint is false, return the ending_version to end the backfill.
                if status.backfill_status == BackfillStatus::Complete
                    && !backfill_config.overwrite_checkpoint
                {
                    return Ok(backfill_config.ending_version);
                }
                // If status is Complete or overwrite_checkpoint is true, this is the start of a new backfill job.
                if backfill_config.overwrite_checkpoint {
                    let backfill_alias = status.backfill_alias.clone();
                    let status = BackfillProcessorStatus {
                        backfill_alias,
                        backfill_status: BackfillStatus::InProgress,
                        last_success_version: 0,
                        last_transaction_timestamp: None,
                        backfill_start_version: backfill_config.initial_starting_version as i64,
                        backfill_end_version: backfill_config.ending_version as i64,
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
                    return Ok(backfill_config.initial_starting_version);
                }

                // `backfill_config.initial_starting_version` is NOT respected.
                // Return the last success version + 1.
                let starting_version = status.last_success_version as u64 + 1;
                log_ascii_warning(starting_version);
                Ok(starting_version)
            } else {
                Ok(backfill_config.initial_starting_version)
            }
        },
        ProcessorMode::Default => {
            // Return initial_starting_version if there is no checkpoint. Otherwise,
            // return the higher of the checkpointed version and `initial_starting_version`.
            let status = ProcessorStatusQuery::get_by_processor(
                indexer_processor_config.processor_config.name(),
                &mut conn,
            )
            .await
            .context("Failed to query processor_status table.")?;

            let default_starting_version = indexer_processor_config
                .bootstrap_config
                .clone()
                .map_or(0, |config| config.initial_starting_version);

            Ok(status.map_or(default_starting_version, |status| {
                std::cmp::max(status.last_success_version as u64, default_starting_version)
            }))
        },
        ProcessorMode::Testing => {
            // Always start from the override_starting_version.
            let testing_config = indexer_processor_config.testing_config.clone().unwrap();
            Ok(testing_config.override_starting_version)
        },
    }
}

fn log_ascii_warning(version: u64) {
    println!(
        r#"
 ██╗    ██╗ █████╗ ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗ ██╗
 ██║    ██║██╔══██╗██╔══██╗████╗  ██║██║████╗  ██║██╔════╝ ██║
 ██║ █╗ ██║███████║██████╔╝██╔██╗ ██║██║██╔██╗ ██║██║  ███╗██║
 ██║███╗██║██╔══██║██╔══██╗██║╚██╗██║██║██║╚██╗██║██║   ██║╚═╝
 ╚███╔███╔╝██║  ██║██║  ██║██║ ╚████║██║██║ ╚████║╚██████╔╝██╗
  ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝
                                                               
=================================================================
   This backfill job is resuming progress at version {}
=================================================================
"#,
        version
    );
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
    let min_processed_version = match indexer_processor_config.mode {
        ProcessorMode::Testing => {
            let testing_config = indexer_processor_config.testing_config.clone().unwrap();
            Some(testing_config.override_starting_version)
        },
        ProcessorMode::Backfill => Some(
            get_starting_version(indexer_processor_config, conn_pool.clone())
                .await
                .context("Failed to get latest processed version from DB")?,
        ),
        ProcessorMode::Default => get_min_processed_version_from_db(conn_pool.clone(), table_names)
            .await
            .context("Failed to get minimum last success version from DB")?,
    };

    let config_starting_version = indexer_processor_config
        .bootstrap_config
        .clone()
        .map_or(0, |config| config.initial_starting_version);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::{
                BackfillConfig, BootStrapConfig, IndexerProcessorConfig, ProcessorMode,
                TestingConfig,
            },
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
        initial_starting_version: Option<u64>,
        override_starting_version: Option<u64>,
        mode: ProcessorMode,
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
        let bootstrap_config = Some(BootStrapConfig {
            initial_starting_version: initial_starting_version.unwrap_or(0),
        });
        let testing_config = Some(TestingConfig {
            override_starting_version: override_starting_version.unwrap_or(0),
            ending_version: 0,
        });
        IndexerProcessorConfig {
            db_config,
            transaction_stream_config: TransactionStreamConfig {
                indexer_grpc_data_service_address: Url::parse("https://test.com").unwrap(),
                starting_version: None,
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
            bootstrap_config,
            testing_config,
            mode,
        }
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_starting_version_no_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, None, None, ProcessorMode::Default);
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
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, Some(5), None, ProcessorMode::Default);
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
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, None, None, ProcessorMode::Default);
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor_status::table)
            .values(ProcessorStatus {
                processor: indexer_processor_config.processor_config.name().to_string(),
                last_success_version: 12,
                last_transaction_timestamp: None,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 12)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_get_starting_version_with_completed_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let backfill_id = "1".to_string();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_id: backfill_id.clone(),
                initial_starting_version: 0,
                ending_version: 10,
                overwrite_checkpoint: false,
            }),
            None,
            None,
            ProcessorMode::Backfill,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: backfill_id.clone(),
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
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_id: "1".to_string(),
                initial_starting_version: 0,
                ending_version: 10,
                overwrite_checkpoint: false,
            }),
            None,
            None,
            ProcessorMode::Backfill,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: "events_processor_1".to_string(),
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: 100,
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
    async fn test_backfill_get_starting_version_with_inprogress_checkpoint_overwrite_checkpoint() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_id: "1".to_string(),
                initial_starting_version: 3,
                ending_version: 10,
                overwrite_checkpoint: true,
            }),
            None,
            None,
            ProcessorMode::Backfill,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: "events_processor_1".to_string(),
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: 100,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 3)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_backfill_get_starting_version_with_checkpoint_test_mode() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config = create_indexer_config(
            db.get_db_url(),
            Some(BackfillConfig {
                backfill_id: "1".to_string(),
                initial_starting_version: 3,
                ending_version: 10,
                overwrite_checkpoint: false,
            }),
            None,
            Some(3),
            ProcessorMode::Testing,
        );
        let conn_pool = new_db_pool(db.get_db_url().as_str(), Some(10))
            .await
            .expect("Failed to create connection pool");
        run_migrations(db.get_db_url(), conn_pool.clone()).await;
        diesel::insert_into(processor::schema::backfill_processor_status::table)
            .values(BackfillProcessorStatus {
                backfill_alias: "events_processor_1".to_string(),
                backfill_status: BackfillStatus::InProgress,
                last_success_version: 10,
                last_transaction_timestamp: None,
                backfill_start_version: 0,
                backfill_end_version: 100,
            })
            .execute(&mut conn_pool.clone().get().await.unwrap())
            .await
            .expect("Failed to insert processor status");

        let starting_version = get_starting_version(&indexer_processor_config, conn_pool)
            .await
            .unwrap();

        assert_eq!(starting_version, 3)
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_get_min_last_success_version_parquet_no_checkpoints() {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, Some(0), None, ProcessorMode::Default);
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
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, Some(0), None, ProcessorMode::Default);
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
        let indexer_processor_config =
            create_indexer_config(db.get_db_url(), None, Some(5), None, ProcessorMode::Default);
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
        assert_eq!(min_version, 15);
    }
}
