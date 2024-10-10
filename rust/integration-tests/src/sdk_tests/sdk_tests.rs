#[cfg(test)]
mod tests {
    use std::fs;
    use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
    use aptos_indexer_test_transactions::IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_;
    use assert_json_diff::assert_json_eq;
    use diesel::{pg::PgConnection, RunQueryDsl};
    use sdk_processor::{
        config::{
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::ProcessorConfig,
        },
        processors::events::events_processor::{EventsProcessor},
        schema::events::dsl::*,
    };
    use testing_framework::new_test_context::{PostgresTestDatabase, SdkTestContext, TestDatabase};
    use sdk_processor::config::indexer_processor_config::DbConfig;
    use diesel::Connection;
    use crate::diff_test_helper::processors::event_processor::EventsProcessorTestHelper;
    use crate::diff_test_helper::ProcessorTestHelper;
    use crate::diff_tests::{remove_inserted_at, remove_transaction_timestamp};

    #[tokio::test]
    async fn test_run() {
        // Step 1: set up an input transaction that will be used
        let imported_txns = [IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_];

        let txn_version = 1255836496; // TOOD: more elegant way to get this

        // custom db setup
        let db = PostgresTestDatabase::new();

        // test context setup
        let test_context = SdkTestContext::new(&imported_txns, db)
            .await
            .unwrap();

        // Step 2: build a custom processor
        let processor_config = ProcessorConfig::EventsProcessor;
        let transaction_stream_config = test_context.create_transaction_stream_config(1255836496, 1255836496);

        let db_config = DbConfig {
            postgres_connection_string: test_context.database.get_db_url(),
            db_pool_size: 100,
        };

        let indexer_processor_config = IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        };

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");


        let custom_output_path = Some("custom_output_path/test_results.json".to_string());
        let processor_name = events_processor.name();

        // Step 3: run the processor with custom validation logic
        let mut db_value = test_context
            .run(
                &events_processor,
                txn_version,
                true,   // TODO: enable a command line flag
                custom_output_path.clone(),
                move|db_url| {
                    // Custom validation logic
                    let mut conn = PgConnection::establish(&db_url).expect("Failed to establish DB connection");
                    let test_helper = Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>;
                    let json_data = match test_helper.load_data(&mut conn, &txn_version.to_string()) {
                        Ok(events_json) => {
                            events_json
                        }
                        Err(e) => {
                            println!(
                                "[ERROR] Failed to load data for processor {} and transaction version {}: {}",
                                processor_name, "txn_version", e
                            );
                            return Err(e);
                        }
                    };

                    println!(
                        "[INFO] Test passed for processor {} and transaction version: {}",
                        processor_name, txn_version
                    );
                    Ok(json_data)
                },
            )
            .await
            .unwrap();

        // Additional custom validation logic can be added here
        let mut expected_json = match read_and_parse_json(&custom_output_path.unwrap()) {
            Ok(json) => json,
            Err(e) => {
                println!(
                    "[ERROR] Error handling JSON for processor {} and transaction version {}: {}",
                    processor_name, txn_version, e
                );
                panic!("Failed to read and parse JSON");
            }
        };

        // TODO: we need to enhance json diff, as we might have more complex diffs.
        remove_inserted_at(&mut db_value);
        remove_transaction_timestamp(&mut db_value);
        remove_transaction_timestamp(&mut expected_json);
        remove_inserted_at(&mut expected_json);
        println!("Json data: {:?}", db_value);
        println!("Expected json: {:?}", expected_json);
        assert_json_eq!(&db_value, &expected_json);

    }

    fn read_and_parse_json(path: &str) -> anyhow::Result<serde_json::Value> {
        match fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(json) => Ok(json),
                Err(e) => {
                    eprintln!("[ERROR] Failed to parse JSON at {}: {}", path, e);
                    Err(anyhow::anyhow!("Failed to parse JSON: {}", e))
                },
            },
            Err(e) => {
                eprintln!("[ERROR] Failed to read file at {}: {}", path, e);
                Err(anyhow::anyhow!("Failed to read file: {}", e))
            },
        }
    }
}

