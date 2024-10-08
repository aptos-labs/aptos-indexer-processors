#[cfg(test)]
mod tests {
    use aptos_protos::{
        indexer::v1::{
            raw_data_server::{RawData, RawDataServer},
            GetTransactionsRequest, TransactionsResponse,
        },
        transaction::v1::Transaction,
    };
    use futures::Stream;
    use std::pin::Pin;
    use tonic::{Request, Response, Status};
    use diesel::{pg::PgConnection, Connection, RunQueryDsl};
    use sdk_processor::processors::events_processor::EventsProcessorConfig;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };
    use sdk_processor::config::processor_config::ProcessorConfig;
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
    use aptos_indexer_processor_sdk::common_steps::TransactionStreamStep;
    use sdk_processor::config::db_config::DbConfig;
    use sdk_processor::config::db_config::PostgresConfig;
    use sdk_processor::config::indexer_processor_config::IndexerProcessorConfig;
    use sdk_processor::processors::events_processor::EventsProcessor;
    use ahash::AHashMap;
    use anyhow::Context;
    use url::Url;
    use crate::models::queryable_models::Event;
    use crate::utils::mock_grpc::MockGrpcServer;
    use sdk_processor::schema::events::dsl::*;
    use diesel::{

        query_dsl::methods::{FilterDsl, ThenOrderDsl},
        ExpressionMethods
    };

    type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

    /// Helper function to set up and start Postgres container.
    async fn setup_postgres() -> (String, ContainerAsync<GenericImage>) {
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

        let host = postgres_container.get_host().await.unwrap();
        let port = postgres_container
            .get_host_port_ipv4(5432)
            .await
            .unwrap();
        let db_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        (db_url, postgres_container)
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(transactions: Vec<TransactionsResponse>, chain_id: u64) {
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

    /// Helper function to initialize the Events Processor.
    async fn init_events_processor(db_url: String) -> EventsProcessor {
        let events_processor_config = EventsProcessorConfig {
            channel_size: 10,
            per_table_chunk_sizes: AHashMap::new(),
        };

        let processor_config = ProcessorConfig::EventsProcessor(events_processor_config);

        let transaction_stream_config = TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse("http://localhost:51254").expect("Could not parse database url"),
            starting_version: Some(1), // starting version for the stream
            request_ending_version: Some(1),
            auth_token: "".to_string(),
            request_name_header: "".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        };

        let postgres_config = PostgresConfig {
            connection_string: db_url.clone(),
            db_pool_size: 100,
        };

        let db_config = DbConfig::PostgresConfig(postgres_config);

        let indexer_processor_config = IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        };

        EventsProcessor::new(indexer_processor_config).await.expect("Failed to create EventsProcessor")
    }

    #[tokio::test]
    async fn test_run() {
        // Setup Postgres
        let (db_url, _postgres_container) = setup_postgres().await;

        // Setup Mock GRPC server
        let transaction = Transaction {
            version: 1,
            ..Transaction::default()
        };
        let transactions = vec![TransactionsResponse {
            transactions: vec![transaction],
            ..TransactionsResponse::default()
        }];
        setup_mock_grpc(transactions, 1).await;

        // Initialize and run the EventsProcessor
        let events_processor = init_events_processor(db_url.clone()).await;
        events_processor.run_processor().await.expect("Failed to run processor");

        // Connect to Postgres and verify the events
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url)).expect("Failed to connect to database");

        let events_result = events
            .load::<Event>(&mut conn);

        let all_events = events_result.expect("Failed to load events");
        let json_data = serde_json::to_string_pretty(&all_events).expect("Failed to serialize events");
        print!("{}", json_data);
    }

}
