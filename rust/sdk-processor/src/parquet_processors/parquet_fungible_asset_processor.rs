use crate::{
    config::{
        db_config::DbConfig,
        indexer_processor_config::{IndexerProcessorConfig, ProcessorMode},
        processor_config::ProcessorConfig,
    },
    parquet_processors::{
        initialize_database_pool, initialize_gcs_client, initialize_parquet_buffer_step,
        set_backfill_table_flag, ParquetTypeEnum,
    },
    steps::{
        common::{
            parquet_version_tracker_step::ParquetVersionTrackerStep,
            processor_status_saver::get_processor_status_saver,
        },
        parquet_fungible_asset_processor::parquet_fa_extractor::ParquetFungibleAssetExtractor,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{run_migrations, ArcDbPool},
        starting_version::get_min_last_success_version_parquet,
    },
};
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{TransactionStreamStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS},
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use parquet::schema::types::Type;
use processor::{
    bq_analytics::generic_parquet_processor::HasParquetSchema,
    db::parquet::models::fungible_asset_models::{
        parquet_v2_fungible_asset_activities::FungibleAssetActivity,
        parquet_v2_fungible_asset_balances::{
            CurrentFungibleAssetBalance, CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
        },
        parquet_v2_fungible_metadata::FungibleAssetMetadataModel,
    },
};
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info};

pub struct ParquetFungibleAssetProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl ParquetFungibleAssetProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> anyhow::Result<Self> {
        let db_pool = initialize_database_pool(&config.db_config).await?;
        Ok(Self { config, db_pool })
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for ParquetFungibleAssetProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> anyhow::Result<()> {
        // Run Migrations
        let parquet_db_config = match self.config.db_config {
            DbConfig::ParquetConfig(ref parquet_config) => {
                run_migrations(
                    parquet_config.connection_string.clone(),
                    self.db_pool.clone(),
                )
                .await;
                parquet_config
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid db config for ParquetFungibleAssetProcessor {:?}",
                    self.config.db_config
                ));
            },
        };

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let parquet_processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::ParquetFungibleAssetProcessor(parquet_processor_config) => {
                parquet_processor_config
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor configuration for ParquetFungibleAssetProcessor {:?}",
                    self.config.processor_config
                ));
            },
        };

        let processor_status_table_names = self
            .config
            .processor_config
            .get_processor_status_table_names()
            .context("Failed to get table names for the processor status table")?;

        let starting_version = get_min_last_success_version_parquet(
            &self.config,
            self.db_pool.clone(),
            processor_status_table_names,
        )
        .await?;

        // Define processor transaction stream config
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            request_ending_version: match self.config.mode {
                ProcessorMode::Default => None,
                ProcessorMode::Backfill => self
                    .config
                    .backfill_config
                    .as_ref()
                    .map(|c| c.ending_version),
                ProcessorMode::Testing => self
                    .config
                    .testing_config
                    .as_ref()
                    .map(|c| c.ending_version),
            },
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let backfill_table = set_backfill_table_flag(parquet_processor_config.backfill_table);
        let mut parquet_fa_extractor = ParquetFungibleAssetExtractor::new(backfill_table);
        parquet_fa_extractor
            .bootstrap_fa_to_coin_mapping(self.db_pool.clone())
            .await?;

        let gcs_client =
            initialize_gcs_client(parquet_db_config.google_application_credentials.clone()).await;

        let parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>> = [
            (
                ParquetTypeEnum::FungibleAssetActivities,
                FungibleAssetActivity::schema(),
            ),
            (
                ParquetTypeEnum::FungibleAssetMetadata,
                FungibleAssetMetadataModel::schema(),
            ),
            (
                ParquetTypeEnum::FungibleAssetBalances,
                FungibleAssetBalance::schema(),
            ),
            (
                ParquetTypeEnum::CurrentFungibleAssetBalancesLegacy,
                CurrentFungibleAssetBalance::schema(),
            ),
            (
                ParquetTypeEnum::CurrentFungibleAssetBalances,
                CurrentUnifiedFungibleAssetBalance::schema(),
            ),
        ]
        .into_iter()
        .collect();

        let default_size_buffer_step = initialize_parquet_buffer_step(
            gcs_client.clone(),
            parquet_type_to_schemas,
            parquet_processor_config.upload_interval,
            parquet_processor_config.max_buffer_size,
            parquet_db_config.bucket_name.clone(),
            parquet_db_config.bucket_root.clone(),
            self.name().to_string(),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to initialize parquet buffer step: {:?}", e);
        });

        let parquet_version_tracker_step = ParquetVersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        let channel_size = parquet_processor_config.channel_size;

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(parquet_fa_extractor.into_runnable_step(), channel_size)
        .connect_to(default_size_buffer_step.into_runnable_step(), channel_size)
        .connect_to(
            parquet_version_tracker_step.into_runnable_step(),
            channel_size,
        )
        .end_and_return_output_receiver(channel_size);

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
