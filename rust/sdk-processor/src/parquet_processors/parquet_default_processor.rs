use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::{get_min_last_success_version_parquet, get_starting_version},
    },
};
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStream, traits::processor_trait::ProcessorTrait,
};

pub struct ParquetDefaultProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool, // for processor status
}

impl ParquetDefaultProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> anyhow::Result<Self> {
        match config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                let conn_pool = new_db_pool(
                    &postgres_config.connection_string,
                    Some(postgres_config.db_pool_size),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create connection pool for PostgresConfig: {:?}",
                        e
                    )
                })?;

                Ok(Self {
                    config,
                    db_pool: conn_pool,
                })
            },
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for ParquetDefaultProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> anyhow::Result<()> {
        // Run Migrations
        match self.config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                run_migrations(&postgres_config.connection_string, self.db_pool.clone()).await;
            },
        }

        // Determine the processing mode (backfill or regular)
        let is_backfill = self.config.backfill_config.is_some();

        // Query the starting version
        let _starting_version = if is_backfill {
            get_starting_version(&self.config, self.db_pool.clone()).await?;
        } else {
            // Regular mode logic: Fetch the minimum last successful version across all relevant tables
            let table_names = self
                .config
                .processor_config
                .get_table_names()
                .context(format!(
                    "Failed to get table names for the processor {}",
                    self.config.processor_config.name()
                ))?;
            get_min_last_success_version_parquet(&self.config, self.db_pool.clone(), table_names)
                .await?;
        };

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let _parquet_processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::ParquetDefaultProcessor(parquet_processor_config) => {
                parquet_processor_config
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor configuration for ParquetDefaultProcessor {:?}",
                    self.config.processor_config
                ));
            },
        };

        // Define processor steps
        Ok(())
    }
}
