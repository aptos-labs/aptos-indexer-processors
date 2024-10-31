use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
    },
};
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
                run_migrations(
                    postgres_config.connection_string.clone(),
                    self.db_pool.clone(),
                )
                .await;
            },
        }

        // TODO: Starting version from config. 0 if not set.

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
