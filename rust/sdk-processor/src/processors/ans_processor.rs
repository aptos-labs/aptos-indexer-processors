use crate::{
    config::{
        db_config::DbConfig,
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    steps::{
        ans_processor::{AnsExtractor, AnsStorer},
        common::get_processor_status_saver,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use processor::worker::TableFlags;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::{debug, info};
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnsProcessorConfig {
    // #[serde(flatten)]
    // pub default: DefaultProcessorConfig,
    pub ans_v1_primary_names_table_handle: String,
    pub ans_v1_name_records_table_handle: String,
    pub ans_v2_contract_address: String,

    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    // Size of channel between steps
    #[serde(default = "DefaultProcessorConfig::default_channel_size")]
    pub channel_size: usize,
    // String vector for deprecated tables to skip db writes
    #[serde(default)]
    pub deprecated_tables: HashSet<String>,
}
pub struct AnsProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl AnsProcessor {
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
}

#[async_trait::async_trait]
impl ProcessorTrait for AnsProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        match self.config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                run_migrations(
                    postgres_config.connection_string.clone(),
                    self.db_pool.clone(),
                )
                .await;
            },
        }

        //  Merge the starting version from config and the latest processed version from the DB.
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain.
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::AnsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for Account Transactions Processor: {:?}",
                    self.config.processor_config
                ))
            },
        };
        let channel_size = processor_config.channel_size;
        let deprecated_table_flags = TableFlags::from_set(&processor_config.deprecated_tables);

        // Define processor steps.
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config.clone()
        })
        .await?;
        let acc_txns_extractor =
            AnsExtractor::new(deprecated_table_flags, self.config.processor_config.clone());
        let acc_txns_storer = AnsStorer::new(self.db_pool.clone(), processor_config);
        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together.
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(acc_txns_extractor?.into_runnable_step(), channel_size)
        .connect_to(acc_txns_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing transactions from versions [{:?}, {:?}]",
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
