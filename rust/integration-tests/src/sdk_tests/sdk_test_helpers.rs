#[cfg(test)]
mod tests {
    use crate::{
        models::queryable_models::Event, sdk::sdk_test_context,
        DiffTest, TestType,
    };
    use ahash::AHashMap;
    use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
    use aptos_indexer_test_transactions::IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_;
    use diesel::{pg::PgConnection, RunQueryDsl};
    use sdk_processor::{
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::ProcessorConfig,
        },
        processors::events_processor::{EventsProcessor, EventsProcessorConfig},
        schema::events::dsl::*,
    };
    use url::Url;

    /// Helper function to initialize the Events Processor.
    async fn init_events_processor(db_url: String) -> EventsProcessor {
        let events_processor_config = EventsProcessorConfig {
            channel_size: 10,
            per_table_chunk_sizes: AHashMap::new(),
        };

        let processor_config = ProcessorConfig::EventsProcessor(events_processor_config);

        // TODO: we need to provide this
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

        // TODO: we need to provide this
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
        let imported_txns = [IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_];
        // this doesn't work, b/c sdk receives transaction batches with a gap, it returns an error
        // let test_context = sdk_test_context::SdkTestContext::new(&ALL_IMPORTED_TESTNET_TXNS)

        let test_context = sdk_test_context::SdkTestContext::new(&imported_txns)
            .await
            .unwrap();
        let db_url = test_context.get_db_url().await;
        println!("starting db_url: {}", db_url);
        // Initialize and run the EventsProcessor

        let events_processor = init_events_processor(db_url.clone()).await;
        let test_type = TestType::Diff(DiffTest);

        test_context
            .run(
                &events_processor,
                test_type,
                move |conn: &mut PgConnection, _txn_version: &str| {
                    // validation logic here
                    let events_result = events.load::<Event>(conn);

                    let all_events = events_result.expect("Failed to load events");
                    let json_data = serde_json::to_string_pretty(&all_events)
                        .expect("Failed to serialize events");
                    assert_eq!(
                        all_events.len(),
                        5,
                        "Expected 1 event, got {}",
                        all_events.len()
                    );
                    println!("json data: {}", json_data);
                    Ok(())
                },
            )
            .await
            .unwrap();
    }
    
}
