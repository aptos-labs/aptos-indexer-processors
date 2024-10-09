use crate::{sdk::mock_grpc::MockGrpcServer, TestType};
use anyhow::Context;
use aptos_protos::{indexer::v1::TransactionsResponse, transaction::v1::Transaction};
use diesel::{pg::PgConnection, Connection};
use processor::utils::database::new_db_pool;
use sdk_processor::{config::processor_config, processors::events_processor::ProcessorTrait};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
use sdk_processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
    },
    schema::events::dsl::*,
};use url::Url;

pub struct SdkTestContext {
    pub transaction_batches: Vec<Transaction>,
    postgres_container: ContainerAsync<GenericImage>,
}

impl SdkTestContext {
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        // creating new
        println!("sdk test context new");
        let transaction_batches = txn_bytes
            .iter()
            .map(|txn| {
                let txn: Transaction = serde_json::from_slice(txn).unwrap();
                txn
            })
            .collect::<Vec<Transaction>>();

        // Set up Postgres container
        let postgres_container = GenericImage::new("postgres", "14")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Postgres started");

        Ok(SdkTestContext {
            transaction_batches,
            postgres_container,
        })
    }

    pub async fn get_db_url(&self) -> String {
        let host = self.postgres_container.get_host().await.unwrap();
        let port = self
            .postgres_container
            .get_host_port_ipv4(5432)
            .await
            .unwrap();
        format!("postgres://postgres:postgres@{host}:{port}/postgres")
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(&self, transactions: Vec<TransactionsResponse>, chain_id: u64) {
        println!("received transactions size: {:?}", transactions.len());
        let mock_grpc_server = MockGrpcServer {
            transactions,
            chain_id,
        };

        // Start the Mock GRPC server
        tokio::spawn(async move {
            println!("Starting Mock GRPC server");
            mock_grpc_server.run().await;
        });
    }

    /// Helper function to create TransactionStreamConfig and DbConfig, now part of SdkTestContext.
    pub async fn create_transaction_and_db_config(
        &self,
        starting_version: Option<u64>,
        ending_version: Option<u64>,
    ) -> (TransactionStreamConfig, DbConfig) {
        let db_url = self.get_db_url().await;

        let transaction_stream_config = TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse("http://localhost:51254")
                .expect("Could not parse database url"),
            starting_version, // dynamically pass the starting version
            request_ending_version: ending_version, // dynamically pass the ending version
            auth_token: "".to_string(),
            request_name_header: "sdk testing".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        };

        let postgres_config = PostgresConfig {
            connection_string: db_url.to_string(),
            db_pool_size: 100,
        };

        let db_config = DbConfig::PostgresConfig(postgres_config);

        (transaction_stream_config, db_config)
    }

    pub async fn run<F>(
        &self,
        processor: &impl ProcessorTrait, // Single instance that implements both traits
        test_type: TestType,
        verification_f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&mut PgConnection, &str) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        // setup grpc server
        let transactions = self.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        self.setup_mock_grpc(transactions_response, 1).await;

        let db_url = self.get_db_url().await;
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url))?;
        let _db_pool = new_db_pool(&db_url, None).await.unwrap();
        println!("Starting processor...");
        processor
            .run_processor()
            .await
            .expect("Failed to run processor");
        println!("Processor finished.");

        if matches!(test_type, TestType::Diff(_)) {
            test_type.run_verification(&mut conn, "1", &verification_f)?;
        }
        // // // For ScenarioTest, use the last transaction version if needed
        // if matches!(test_type, TestType::Scenario(_)) {
        //     if let Some(last_version) = last_version {
        //         test_type.run_verification(
        //             &mut conn,
        //             &last_version.to_string(),
        //             &verification_f,
        //         )?;
        //     } else {
        //         return Err(anyhow::anyhow!(
        //             "No transactions found to get the last version"
        //         ));
        //     }
        // }
        //
        Ok(())
    }
}

pub struct TestProcessorConfig {
    pub config: processor_config::ProcessorConfig,
}
