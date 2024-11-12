use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    parquet_processors::ParquetTypeEnum,
    steps::parquet_default_processor::{
        gcs_handler::{create_new_writer, GCSUploader},
        parquet_default_extractor::ParquetDefaultExtractor,
        size_buffer::SizeBufferStep,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::{get_min_last_success_version_parquet, get_starting_version},
    },
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::TransactionStreamStep,
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
use parquet::schema::types::Type;
use processor::{
    bq_analytics::generic_parquet_processor::HasParquetSchema,
    db::common::models::default_models::{
        parquet_move_modules::MoveModule, parquet_move_resources::MoveResource,
        parquet_move_tables::TableItem, parquet_transactions::Transaction as ParquetTransaction,
        parquet_write_set_changes::WriteSetChangeModel,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use aptos_indexer_processor_sdk::common_steps::{DEFAULT_UPDATE_PROCESSOR_STATUS_SECS, VersionTrackerStep};
use tracing::{debug, info};
use crate::steps::common::processor_status_saver::get_parquet_processor_status_saver;
use crate::steps::parquet_default_processor::parquet_version_tracker_step::ParquetVersionTrackerStep;


const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";

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

        // Determine the processing mode (backfill or regular)
        let is_backfill = self.config.backfill_config.is_some();

        // TODO: Revisit when parquet verstion tracker is available.
        // Query the starting version
        let starting_version = if is_backfill {
            get_starting_version(&self.config, self.db_pool.clone()).await?
        } else {
            // Regular mode logic: Fetch the minimum last successful version across all relevant tables
            let table_names = self
                .config
                .processor_config
                .get_table_names()?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to get table names for the processor {}",
                        self.config.processor_config.name()
                    )
                })?;

            get_min_last_success_version_parquet(&self.config, self.db_pool.clone(), table_names)
                .await?
        };

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let parquet_processor_config = match self.config.processor_config.clone() {
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

        println!(
            "===============Starting version: {}===============",
            starting_version
        );

        // Define processor transaction stream config
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config.clone()
        })
        .await?;
        //
        // // TODO: look at the config to dynamically set the opt_in_tables, tables
        let parquet_default_extractor = ParquetDefaultExtractor {
            opt_in_tables: None, // : parquet_processor_config.tables.iter().map(|s| s.to_string()).collect(),
        };

        let credentials = parquet_processor_config
            .google_application_credentials
            .clone();

        if let Some(credentials) = credentials {
            std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS, credentials);
        }

        let gcs_config = GcsClientConfig::default()
            .with_auth()
            .await
            .expect("Failed to create GCS client config");

        let gcs_client = Arc::new(GCSClient::new(gcs_config));

        let parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>> = [
            (ParquetTypeEnum::MoveResource, MoveResource::schema()),
            (
                ParquetTypeEnum::WriteSetChange,
                WriteSetChangeModel::schema(),
            ),
            (ParquetTypeEnum::Transaction, ParquetTransaction::schema()),
            (ParquetTypeEnum::TableItem, TableItem::schema()),
            (ParquetTypeEnum::MoveModule, MoveModule::schema()),
        ]
        .into_iter()
        .collect();

        let parquet_type_to_writer = parquet_type_to_schemas
            .iter()
            .map(|(key, schema)| {
                let writer = create_new_writer(schema.clone()).expect("Failed to create writer");
                (*key, writer)
            })
            .collect();

        let buffer_uploader = GCSUploader::new(
            gcs_client.clone(),
            parquet_type_to_schemas,
            parquet_type_to_writer,
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            self.name().to_string(),
        )?;

        let channel_size = parquet_processor_config.channel_size;

        let default_size_buffer_step = SizeBufferStep::new(
            Duration::from_secs(parquet_processor_config.parquet_upload_interval),
            buffer_uploader,
        );

        let parquet_version_tracker_step = ParquetVersionTrackerStep::new(
            get_parquet_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(parquet_default_extractor.into_runnable_step(), channel_size)
        .connect_to(default_size_buffer_step.into_runnable_step(), channel_size)
        .connect_to(parquet_version_tracker_step.into_runnable_step(), channel_size)
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
