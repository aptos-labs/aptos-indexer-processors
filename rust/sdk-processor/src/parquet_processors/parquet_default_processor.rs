use crate::{
    config::{
        db_config::DbConfig, indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    steps::parquet_default_processor::{
        generic_parquet_buffer_handler::GenericParquetBufferHandler,
        parquet_default_extractor::ParquetDefaultExtractor,
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
    common_steps::{
        timed_size_buffer_step::{TableConfig, TimedSizeBufferStep},
        TransactionStreamStep,
    },
    test::steps::pass_through_step::PassThroughStep,
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep, RunnableAsyncStep},
};
use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
use parquet::record::RecordWriter;
use processor::db::common::models::default_models::{
    parquet_move_modules::MoveModule, parquet_move_resources::MoveResource,
    parquet_move_tables::TableItem, parquet_transactions::Transaction as ParquetTransaction,
    parquet_write_set_changes::WriteSetChangeModel,
};
use std::{sync::Arc, time::Duration};
use tracing::{debug, info};

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

type Input = (
    Vec<ParquetTransaction>,
    Vec<MoveResource>,
    Vec<WriteSetChangeModel>,
    Vec<TableItem>,
    Vec<MoveModule>,
);

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

        let generic_parquet_buffer_handler = GenericParquetBufferHandler::new(
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            [ParquetTransaction::default()].as_slice().schema().unwrap(),
            self.name().to_string(),
        )
        .unwrap();

        let transaction_step = TimedSizeBufferStep::<
            Input,
            ParquetTransaction,
            GenericParquetBufferHandler<ParquetTransaction>,
        >::new(
            Duration::from_secs(parquet_processor_config.parquet_upload_interval),
            TableConfig {
                table_name: "transactions".to_string(),
                bucket_name: parquet_processor_config.bucket_name.clone(),
                bucket_root: parquet_processor_config.bucket_root.clone(),
                max_size: parquet_processor_config.max_buffer_size,
            },
            gcs_client.clone(),
            self.name(),
            generic_parquet_buffer_handler,
        );

        let move_resource_generic_parquet_buffer_handler = GenericParquetBufferHandler::new(
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            [MoveResource::default()].as_slice().schema().unwrap(),
            self.name().to_string(),
        )
        .unwrap();

        let move_resource_step = TimedSizeBufferStep::<
            Input,
            MoveResource,
            GenericParquetBufferHandler<MoveResource>,
        >::new(
            Duration::from_secs(parquet_processor_config.parquet_upload_interval),
            TableConfig {
                table_name: "move_resources".to_string(),
                bucket_name: parquet_processor_config.bucket_name.clone(),
                bucket_root: parquet_processor_config.bucket_root.clone(),
                max_size: parquet_processor_config.max_buffer_size,
            },
            gcs_client.clone(),
            self.name(),
            move_resource_generic_parquet_buffer_handler,
        );

        let wsc_generic_parquet_buffer_handler = GenericParquetBufferHandler::new(
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            [WriteSetChangeModel::default()]
                .as_slice()
                .schema()
                .unwrap(),
            self.name().to_string(),
        )
        .unwrap();

        let write_set_change_step = TimedSizeBufferStep::<
            Input,
            WriteSetChangeModel,
            GenericParquetBufferHandler<WriteSetChangeModel>,
        >::new(
            Duration::from_secs(parquet_processor_config.parquet_upload_interval),
            TableConfig {
                table_name: "write_set_changes".to_string(),
                bucket_name: parquet_processor_config.bucket_name.clone(),
                bucket_root: parquet_processor_config.bucket_root.clone(),
                max_size: parquet_processor_config.max_buffer_size,
            },
            gcs_client.clone(),
            self.name(),
            wsc_generic_parquet_buffer_handler,
        );

        let table_items_generic_parquet_buffer_handler = GenericParquetBufferHandler::new(
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            [TableItem::default()].as_slice().schema().unwrap(),
            self.name().to_string(),
        )
        .unwrap();

        let table_item_step =
            TimedSizeBufferStep::<Input, TableItem, GenericParquetBufferHandler<TableItem>>::new(
                Duration::from_secs(parquet_processor_config.parquet_upload_interval),
                TableConfig {
                    table_name: "table_items".to_string(),
                    bucket_name: parquet_processor_config.bucket_name.clone(),
                    bucket_root: parquet_processor_config.bucket_root.clone(),
                    max_size: parquet_processor_config.max_buffer_size,
                },
                gcs_client.clone(),
                self.name(),
                table_items_generic_parquet_buffer_handler,
            );

        let move_module_generic_parquet_buffer_handler = GenericParquetBufferHandler::new(
            parquet_processor_config.bucket_name.clone(),
            parquet_processor_config.bucket_root.clone(),
            [MoveModule::default()].as_slice().schema().unwrap(),
            self.name().to_string(),
        )
        .unwrap();

        let move_module_step =
            TimedSizeBufferStep::<Input, MoveModule, GenericParquetBufferHandler<MoveModule>>::new(
                Duration::from_secs(parquet_processor_config.parquet_upload_interval),
                TableConfig {
                    table_name: "move_modules".to_string(),
                    bucket_name: parquet_processor_config.bucket_name.clone(),
                    bucket_root: parquet_processor_config.bucket_root.clone(),
                    max_size: parquet_processor_config.max_buffer_size,
                },
                gcs_client.clone(),
                self.name(),
                move_module_generic_parquet_buffer_handler,
            );

        let channel_size = parquet_processor_config.channel_size;

        let builder = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        );

        let builder =
            builder.connect_to(parquet_default_extractor.into_runnable_step(), channel_size);
        let mut fanout_builder = builder.fanout_broadcast(5);

        let (first_builder, first_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(transaction_step.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        let (second_builder, second_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(move_resource_step.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        let (third_builder, third_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(write_set_change_step.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        let (fourth_builder, fourth_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(table_item_step.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        let (fifth_builder, fifth_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(move_module_step.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        let (_, buffer_receiver) = ProcessorBuilder::new_with_fanin_step_with_receivers(
            vec![
                (first_output_receiver, first_builder.graph),
                (second_output_receiver, second_builder.graph),
                (third_output_receiver, third_builder.graph),
                (fourth_output_receiver, fourth_builder.graph),
                (fifth_output_receiver, fifth_builder.graph),
            ],
            // TODO: Replace with parquet version tracker
            RunnableAsyncStep::new(PassThroughStep::new_named("FaninStep".to_string())),
            channel_size,
        )
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

pub trait ExtractResources<ParquetType> {
    fn extract(self) -> Vec<ParquetType>;
}

impl ExtractResources<ParquetTransaction> for Input {
    fn extract(self) -> Vec<ParquetTransaction> {
        self.0
    }
}

impl ExtractResources<MoveResource> for Input {
    fn extract(self) -> Vec<MoveResource> {
        self.1
    }
}

impl ExtractResources<WriteSetChangeModel> for Input {
    fn extract(self) -> Vec<WriteSetChangeModel> {
        self.2
    }
}

impl ExtractResources<TableItem> for Input {
    fn extract(self) -> Vec<TableItem> {
        self.3
    }
}

impl ExtractResources<MoveModule> for Input {
    fn extract(self) -> Vec<MoveModule> {
        self.4
    }
}
