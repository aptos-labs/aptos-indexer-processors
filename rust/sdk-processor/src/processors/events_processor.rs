use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    steps::{
        common::get_processor_status_saver,
        events_processor::{EventsExtractor, EventsStorer},
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
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use tracing::{debug, info};

pub struct EventsProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl EventsProcessor {
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
            _ => Err(anyhow::anyhow!(
                "Invalid db config for EventsProcessor {:?}",
                config.db_config
            )),
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
            )
            .await;
        }

        //  Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::EventsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for EventsProcessor: {:?}",
                    self.config.processor_config
                ))
            },
        };
        let channel_size = processor_config.channel_size;

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config.clone()
        })
        .await?;
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(self.db_pool.clone(), processor_config);
        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(events_extractor.into_runnable_step(), channel_size)
        .connect_to(events_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing events from versions [{:?}, {:?}]",
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
