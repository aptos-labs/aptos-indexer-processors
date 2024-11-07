use crate::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::{
            IndexerProcessorConfig, QUERY_DEFAULT_RETRIES, QUERY_DEFAULT_RETRY_DELAY_MS,
        },
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    steps::{
        common::get_processor_status_saver,
        nft_metadata_processor::asset_uri_publisher::AssetUriPublisher,
        token_v2_processor::token_v2_extractor::TokenV2Extractor,
    },
    utils::{
        chain_id::check_or_update_chain_id,
        database::{new_db_pool, run_migrations, ArcDbPool},
        starting_version::get_starting_version,
    },
};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig},
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
};
use google_cloud_pubsub::{
    client::{Client as PubSubClient, ClientConfig},
    publisher::Publisher,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NftMetadataProcessorConfig {
    #[serde(flatten)]
    pub default_config: DefaultProcessorConfig,
    pub pubsub_topic_name: String,
    pub google_application_credentials: Option<String>,
    #[serde(default = "NftMetadataProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "NftMetadataProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}

impl NftMetadataProcessorConfig {
    pub const fn default_query_retries() -> u32 {
        QUERY_DEFAULT_RETRIES
    }

    pub const fn default_query_retry_delay_ms() -> u64 {
        QUERY_DEFAULT_RETRY_DELAY_MS
    }
}

pub struct NftMetadataProcessor {
    db_pool: ArcDbPool,
    config: IndexerProcessorConfig,
    publisher: Arc<Publisher>,
}

impl NftMetadataProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> anyhow::Result<Self> {
        let (nft_metadata_processor_config, db_config) = extract_configs(&config)?;

        let db_pool =
            new_db_pool(&db_config.connection_string, Some(db_config.db_pool_size)).await?;

        // The PubSub crate utilizes service account file in GOOGLE_APPLICATION_CREDENTIALS env var for auth.
        // If not set, the crate will attempt to retrieve credentials from the metadata server.
        if let Some(credentials) = &nft_metadata_processor_config.google_application_credentials {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }

        // Initialize PubSub client
        let pubsub_config = ClientConfig::default().with_auth().await?;
        let client = PubSubClient::new(pubsub_config).await?;
        let topic = client.topic(&nft_metadata_processor_config.pubsub_topic_name.clone());
        let publisher = Arc::new(topic.new_publisher(None));

        Ok(Self {
            config,
            publisher,
            db_pool,
        })
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for NftMetadataProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> anyhow::Result<()> {
        let (nft_metadata_processor_config, db_config) = extract_configs(&self.config)?;
        run_migrations(&db_config.connection_string, self.db_pool.clone()).await;

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(&self.config, self.db_pool.clone()).await?;

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        let grpc_chain_id = TransactionStream::new(self.config.transaction_stream_config.clone())
            .await?
            .get_chain_id()
            .await?;
        check_or_update_chain_id(grpc_chain_id as i64, self.db_pool.clone()).await?;

        let channel_size = nft_metadata_processor_config.default_config.channel_size;

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        // Reuse the token_v2_extractor to keep the indexing logic in the same place
        let token_v2_extractor = TokenV2Extractor::new(
            nft_metadata_processor_config.query_retries,
            nft_metadata_processor_config.query_retry_delay_ms,
            self.db_pool.clone(),
        );

        let asset_uri_publisher = AssetUriPublisher::new(self.publisher.clone(), grpc_chain_id);

        let version_tracker = VersionTrackerStep::new(
            get_processor_status_saver(self.db_pool.clone(), self.config.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(token_v2_extractor.into_runnable_step(), channel_size)
        .connect_to(asset_uri_publisher.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
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
                    // Error means the channel has been closed
                    error!(error = ?e, "Error receiving transactions");
                    return Err(e.into());
                },
            }
        }
    }
}

fn extract_configs(
    config: &IndexerProcessorConfig,
) -> anyhow::Result<(&NftMetadataProcessorConfig, &PostgresConfig)> {
    let (
        ProcessorConfig::NftMetadataProcessor(nft_metadata_processor_config),
        DbConfig::PostgresConfig(db_config),
    ) = (&config.processor_config, &config.db_config)
    else {
        anyhow::bail!("Invalid config")
    };

    Ok((nft_metadata_processor_config, db_config))
}
