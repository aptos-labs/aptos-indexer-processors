#[cfg(test)]
mod tests {
    use crate::{utils::mock_grpc::MockGrpcServer};
    use ahash::AHashMap;
    use anyhow::Context;
    use aptos_indexer_processor_sdk::{
        aptos_indexer_transaction_stream::TransactionStreamConfig,
        common_steps::TransactionStreamStep,
    };
    use crate::models::queryable_models::Event;
    use aptos_protos::{
        indexer::v1::{
            raw_data_server::{RawData, RawDataServer},
            GetTransactionsRequest, TransactionsResponse,
        },
        transaction::v1::Transaction,
    };
    use diesel::{pg::PgConnection, query_dsl::methods::{FilterDsl, LimitDsl, ThenOrderDsl}, Connection, ExpressionMethods, RunQueryDsl, sql_query};
    use futures::Stream;
    use sdk_processor::{
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::ProcessorConfig,

        },
        db::common::models::events_models::events::EventModel,
        processors::events_processor::{EventsProcessor, EventsProcessorConfig},
        schema::events::dsl::*,
    };
    use std::{pin::Pin, time::Duration};
    use aptos_indexer_test_transactions::IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };
    use tonic::{Request, Response, Status};
    use url::Url;
    use diesel::associations::HasTable;
    use testing_transactions::ALL_IMPORTED_TESTNET_TXNS;

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
        let port = postgres_container.get_host_port_ipv4(5432).await.unwrap();
        let db_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        (db_url, postgres_container)
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(transactions: Vec<TransactionsResponse>, chain_id: u64) {
        println!("received transactions: {:?}", transactions.len());
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
            indexer_grpc_data_service_address: Url::parse("http://localhost:51254")
                .expect("Could not parse database url"),
            starting_version: Some(1255836496), // starting version for the stream
            request_ending_version: Some(1255836496),
            auth_token: "".to_string(),
            request_name_header: "sdk testing".to_string(),
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

        EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor")
    }

    #[tokio::test]
    async fn test_run() {
        // Setup Postgres
        let (db_url, _postgres_container) = setup_postgres().await;

        // Setup Mock GRPC server
        let txns = ALL_IMPORTED_TESTNET_TXNS.iter().map(|txn| {
            let txn: Transaction = serde_json::from_slice(txn).unwrap();
            txn
        }).collect::<Vec<Transaction>>();

        let txn = serde_json::from_slice(IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_).unwrap();

        let transactions = vec![TransactionsResponse {
            transactions: vec![txn],
            ..TransactionsResponse::default()
        }];

        setup_mock_grpc(transactions, 1).await;


        // Connect to Postgres and verify the events
        let mut conn = PgConnection::establish(&db_url)
            .with_context(|| format!("Error connecting to {}", db_url))
            .expect("Failed to connect to database");

        conn.begin_test_transaction().expect("Failed to begin test transaction");

        // Initialize and run the EventsProcessor
        let events_processor = init_events_processor(db_url.clone()).await;
        events_processor
            .run_processor()
            .await
            .expect("Failed to run processor");

        tokio::time::sleep(Duration::from_millis(1000)).await;


        // println!("Queried {} event(s)", results.len());
        let results: Vec<Event> = events
            .limit(1)
            .load::<Event>(&mut conn)
            .expect("Failed to load events");
        println!("Queried {} event(s)", results.len());

        for event in results {
            println!("Event: {:?}", event);
        }


        let events_res = diesel::sql_query("SELECT * FROM events").execute(&mut conn).expect("Failed to execute query");

        println!("Queried {} event(s)", events_res);


        let events_result = events.load::<Event>(&mut conn);

        let all_events = events_result.expect("Failed to load events");
        let json_data =
            serde_json::to_string_pretty(&all_events).expect("Failed to serialize events");
        assert_eq!(
            all_events.len(),
            5,
            "Expected 1 event, got {}",
            all_events.len()
        );
        print!("{}", json_data);
    }
}
