mod config;
mod processor;
mod storage;

use crate::{
    config::{Args, Config},
    processor::ExampleProcessor,
    storage::MemoryStorage,
};
use anyhow::{Context as AnyhowContext, Result};
use aptos_processor_framework::{
    Dispatcher, GrpcStreamSubscriber, ProcessorTrait, StorageTrait, StreamSubscriberTrait,
};
use clap::Parser;
use std::sync::Arc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::try_from(args)?;

    let subscriber = FmtSubscriber::builder()
        // All spans of this level or more severe will be written to stdout.
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .context("Setting default tracing subscriber failed")?;

    let processor = Arc::new(ExampleProcessor {});

    let storage = Arc::new(MemoryStorage::new());

    let starting_version_from_db = storage
        .read_last_processed_version(processor.name())
        .await?;

    let starting_version = config
        .common_storage_config
        .determine_starting_version(starting_version_from_db);

    let stream_subscriber = GrpcStreamSubscriber {
        config: config.stream_subscriber_config.clone(),
        processor_name: processor.name().to_string(),
        starting_version,
    };

    let channel_handle = stream_subscriber.start().await?;

    let mut dispatcher = Dispatcher {
        config: config.dispatcher_config.clone(),
        storage: storage.clone(),
        processor: processor.clone(),
        receiver: channel_handle.receiver,
        starting_version,
    };

    // We expect this to never end.
    dispatcher.dispatch().await;

    Err(anyhow::anyhow!("Dispatcher exited unexpectedly"))
}
