use crate::steps::events_processor::{EventsExtractor, EventsStorer};
use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    steps::common::latest_processed_version_tracker::LatestVersionProcessedTracker,
    utils::{
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    instrumented_channel::instrumented_bounded_channel,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
};
use std::time::Duration;

pub struct EventsProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl EventsProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        let conn_pool = new_db_pool(
            &config.db_config.postgres_connection_string,
            Some(config.db_config.db_pool_size),
        )
        .await
        .expect("Failed to create connection pool");

        Ok(Self {
            config,
            db_pool: conn_pool,
        })
    }

    pub async fn run_processor(self) -> Result<()> {
        // (Optional) Run migrations
        run_migrations(
            self.config.db_config.postgres_connection_string.clone(),
            self.db_pool.clone(),
        )
        .await;

        // (Optional) Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Define processor steps
        let (_input_sender, input_receiver) = instrumented_bounded_channel("input", 1);

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config
        })
        .await?;
        let transaction_stream_with_input = RunnableStepWithInputReceiver::new(
            input_receiver,
            transaction_stream.into_runnable_step(),
        );
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(self.db_pool.clone(), self.config.db_config.clone());
        let timed_buffer = TimedBuffer::new(Duration::from_secs(1));
        let version_tracker = LatestVersionProcessedTracker::new(
            self.config.db_config,
            starting_version,
            self.config.processor_config.name().to_string(),
        )
        .await?;

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_runnable_input_receiver_first_step(
            transaction_stream_with_input,
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(timed_buffer.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
        .connect_to(version_tracker.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    if txn_context.data.is_empty() {
                        tracing::debug!("Received no transactions");
                        continue;
                    }
                    tracing::debug!(
                        "Received events versions: {:?} to {:?}",
                        txn_context.start_version,
                        txn_context.end_version
                    );
                },
                Err(e) => {
                    println!("Error receiving transactions: {:?}", e);
                },
            }
        }
    }
}
