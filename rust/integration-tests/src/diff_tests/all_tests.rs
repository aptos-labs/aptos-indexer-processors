#[allow(clippy::needless_return)]
#[cfg(test)]
mod test {

    use crate::{
        diff_test_helper::{
            processors::{
                event_processor::EventsProcessorTestHelper,
                fungible_asset_processor::FungibleAssetProcessorTestHelper,
            },
            ProcessorTestHelper,
        },
        diff_tests::remove_inserted_at,
        TestContext, TestProcessorConfig,
    };
    use assert_json_diff::assert_json_eq;
    use diesel::pg::PgConnection;
    use std::{collections::HashMap, fs, sync::Arc};
    use testing_transactions::{
        ALL_GENERATED_TXNS, ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS,
    };
    use crate::diff_tests::{get_expected_generated_txns, get_expected_imported_mainnet_txns, get_expected_imported_testnet_txns};

    #[tokio::test]
    async fn test_all_testnet_txns_schema_output_for_all_processors() {
        let mut processor_map: HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> = HashMap::new();

        processor_map.insert(
            "events_processor".to_string(),
            Arc::new(Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );
        processor_map.insert(
            "fungible_asset_processor".to_string(),
            Arc::new(Box::new(FungibleAssetProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );

        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };
        let processor_config2 = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
        };

        let processor_configs = vec![processor_config, processor_config2];

        let test_context = TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            // println!("Running tests for processor: {}", processor_name);
            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);
                println!("Running tests for processor: {}", processor_name);
                // Run the processor logic
                test_context
                    .run(
                        processor_config,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            // Process the transaction using the processor logic
                            // Load and validate data using the test helper
                            let mut json_data = test_helper.load_data(conn, txn_version)?;
                            // Read the trusted JSON file
                            let trusted_json_path = get_expected_imported_testnet_txns(processor_name, txn_version);
                            let mut trusted_json = match fs::read_to_string(&trusted_json_path) {
                                Ok(content) => serde_json::from_str(&content).unwrap(),
                                Err(e) => {
                                    return Err(anyhow::anyhow!(
                                        "Failed to read trusted JSON file: {} {}",
                                        trusted_json_path,
                                        e
                                    ))
                                },
                            };

                            // Optionally remove fields that should be ignored during comparison (e.g., `inserted_at`)
                            remove_inserted_at(&mut json_data);
                            remove_inserted_at(&mut trusted_json);

                            // Perform the validation
                            assert_json_eq!(&json_data, &trusted_json);
                            println!("Test passed for transaction version: {}", txn_version);
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_all_mainnet_txns_schema_output_for_all_processors() {
        let mut processor_map: HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> = HashMap::new();

        processor_map.insert(
            "events_processor".to_string(),
            Arc::new(Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );

        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };

        let processor_configs = vec![processor_config];

        let test_context = TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            // println!("Running tests for processor: {}", processor_name);
            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);
                println!("Running tests for processor: {}", processor_name);
                // Run the processor logic
                test_context
                    .run(
                        processor_config,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            // Process the transaction using the processor logic
                            // Load and validate data using the test helper
                            let mut json_data = test_helper.load_data(conn, txn_version)?;
                            // Read the trusted JSON file
                            let trusted_json_path = get_expected_imported_mainnet_txns(processor_name, txn_version);
                            let mut trusted_json = match fs::read_to_string(&trusted_json_path) {
                                Ok(content) => serde_json::from_str(&content).unwrap(),
                                Err(e) => {
                                    return Err(anyhow::anyhow!(
                                        "Failed to read trusted JSON file: {} {}",
                                        trusted_json_path,
                                        e
                                    ))
                                },
                            };
                            // Optionally remove fields that should be ignored during comparison (e.g., `inserted_at`)
                            remove_inserted_at(&mut json_data);
                            remove_inserted_at(&mut trusted_json);

                            // Perform the validation
                            assert_json_eq!(&json_data, &trusted_json);
                            println!("Test passed for transaction version: {}", txn_version);
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_all_generated_txns_schema_output_for_all_processors() {
        let mut processor_map: HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> = HashMap::new();

        processor_map.insert(
            "events_processor".to_string(),
            Arc::new(Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );

        let processor_config = TestProcessorConfig {
            config: processor::processors::ProcessorConfig::EventsProcessor,
        };

        let processor_configs = vec![processor_config];

        let test_context = TestContext::new(ALL_GENERATED_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            // println!("Running tests for processor: {}", processor_name);
            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);
                println!("Running tests for processor: {}", processor_name);
                // Run the processor logic
                test_context
                    .run(
                        processor_config,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            // Process the transaction using the processor logic
                            // Load and validate data using the test helper
                            let mut json_data = test_helper.load_data(conn, txn_version)?;
                            // Read the trusted JSON file
                            let trusted_json_path = get_expected_generated_txns(processor_name, txn_version);
                            let mut trusted_json = match fs::read_to_string(&trusted_json_path) {
                                Ok(content) => serde_json::from_str(&content).unwrap(),
                                Err(e) => {
                                    return Err(anyhow::anyhow!(
                                        "Failed to read trusted JSON file: {} {}",
                                        trusted_json_path,
                                        e
                                    ))
                                },
                            };

                            // Optionally remove fields that should be ignored during comparison (e.g., `inserted_at`)
                            remove_inserted_at(&mut json_data);
                            remove_inserted_at(&mut trusted_json);

                            // Perform the validation
                            assert_json_eq!(&json_data, &trusted_json);
                            println!("Test passed for transaction version: {}", txn_version);
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }
}
