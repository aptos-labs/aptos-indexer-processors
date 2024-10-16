use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    steps::{
        common::latest_processed_version_tracker::{
            LatestVersionProcessedTracker, UPDATE_PROCESSOR_STATUS_SECS,
        },
        fungible_asset_processor::{
            fungible_asset_extractor::FungibleAssetExtractor,
            fungible_asset_storer::FungibleAssetStorer,
        },
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{OrderByVersionStep, TransactionStreamStep},
    traits::IntoRunnableStep,
};
use processor::worker::TableFlags;
use std::time::Duration;
use tracing::{debug, info};

pub struct FungibleAssetProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl FungibleAssetProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
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

    pub async fn run_processor(self) -> Result<()> {
        let processor_name = self.config.processor_config.name();

        //  Run migrations
        match self.config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                run_migrations(
                    postgres_config.connection_string.clone(),
                    self.db_pool.clone(),
                )
                .await;
            },
        }

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let processor_config = match self.config.processor_config {
            ProcessorConfig::FungibleAssetProcessor(processor_config) => processor_config,
            _ => return Err(anyhow::anyhow!("Processor config is wrong type")),
        };
        let channel_size = processor_config.channel_size;
        let deprecated_table_flags = TableFlags::from_set(&processor_config.deprecated_tables);

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config
        })
        .await?;
        let fa_extractor = FungibleAssetExtractor {};
        let fa_storer = FungibleAssetStorer::new(
            self.db_pool.clone(),
            processor_config,
            deprecated_table_flags,
        );
        let version_tracker =
            LatestVersionProcessedTracker::new(self.db_pool.clone(), processor_name.to_string());

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(fa_extractor.into_runnable_step(), channel_size)
        .connect_to(fa_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                },
                Err(e) => {
                    info!("No more transactions in channel: {:?}", e);
                    break Ok(());
                },
            }
        }
    }
}
