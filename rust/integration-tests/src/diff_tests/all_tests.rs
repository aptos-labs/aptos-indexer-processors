#[allow(clippy::needless_return)]
#[cfg(test)]
mod test {

    use crate::{
        diff_test_helper::{
            processors::{
                event_processor::EventsProcessorTestHelper,
                fungible_asset_processor::FungibleAssetProcessorTestHelper,
                token_v2_processor::TokenV2ProcessorTestHelper,
            },
            ProcessorTestHelper,
        },
        diff_tests::{
            get_expected_imported_mainnet_txns, get_expected_imported_testnet_txns,
            get_expected_scripted_txns, remove_inserted_at, remove_transaction_timestamp,
        },
        DiffTest, TestContext, TestProcessorConfig, TestType,
    };
    use aptos_indexer_test_transactions::{
        ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS, ALL_SCRIPTED_TRANSACTIONS,
    };
    use assert_json_diff::assert_json_eq;
    use diesel::pg::PgConnection;
    use processor::processors::token_v2_processor::TokenV2ProcessorConfig;
    use std::{collections::HashMap, fs, sync::Arc};

    #[tokio::test]
    async fn test_all_testnet_txns_schema_output_for_all_processors() {
        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await.unwrap();

        run_processor_tests(
            processor_configs,
            &test_context,
            get_expected_imported_testnet_txns,
        )
        .await;
    }

    #[tokio::test]
    async fn test_all_mainnet_txns_schema_output_for_all_processors() {
        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await.unwrap();

        run_processor_tests(
            processor_configs,
            &test_context,
            get_expected_imported_mainnet_txns,
        )
        .await;
    }

    #[tokio::test]
    async fn test_all_scripted_txns_schema_output_for_all_processors() {
        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_SCRIPTED_TRANSACTIONS).await.unwrap();

        run_processor_tests(processor_configs, &test_context, get_expected_scripted_txns).await;
    }

    // Helper function to reduce duplicate code for running tests on all processors
    async fn run_processor_tests(
        processor_configs: Vec<TestProcessorConfig>,
        test_context: &TestContext,
        get_expected_json_path_fn: fn(&str, &str) -> String,
    ) {
        let processor_map = get_processor_map();
        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);

            if let Some(test_helper) = processor_map.get(processor_name).cloned() {
                test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            let mut json_data = match test_helper.load_data(conn, txn_version) {
                                Ok(data) => data,
                                Err(e) => {
                                    eprintln!(
                                        "[ERROR] Failed to load data for processor {} and transaction version {}: {}",
                                        processor_name, txn_version, e
                                    );
                                    return Err(e);
                                }
                            };

                            let expected_json_path = get_expected_json_path_fn(processor_name, txn_version);
                            let mut expected_json = match read_and_parse_json(&expected_json_path) {
                                Ok(json) => json,
                                Err(e) => {
                                    eprintln!(
                                        "[ERROR] Error handling JSON for processor {} and transaction version {}: {}",
                                        processor_name, txn_version, e
                                    );
                                    return Err(e);
                                }
                            };

                            // TODO: we need to enhance json diff, as we might have more complex diffs.
                            remove_inserted_at(&mut json_data);
                            remove_transaction_timestamp(&mut json_data);
                            remove_transaction_timestamp(&mut expected_json);
                            assert_json_eq!(&json_data, &expected_json);

                            println!(
                                "[INFO] Test passed for processor {} and transaction version: {}",
                                processor_name, txn_version
                            );
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }

    // Helper function to read and parse JSON files
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

    fn get_processor_map() -> HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> {
        let mut processor_map: HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> = HashMap::new();
        processor_map.insert(
            "events_processor".to_string(),
            Arc::new(Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );
        processor_map.insert(
            "fungible_asset_processor".to_string(),
            Arc::new(Box::new(FungibleAssetProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );
        processor_map.insert(
            "token_v2_processor".to_string(),
            Arc::new(Box::new(TokenV2ProcessorTestHelper) as Box<dyn ProcessorTestHelper>),
        );

        processor_map
    }

    fn get_processor_configs() -> Vec<TestProcessorConfig> {
        vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::TokenV2Processor(
                    TokenV2ProcessorConfig {
                        query_retries: 3,
                        query_retry_delay_ms: 1000,
                    },
                ),
            },
        ]
    }
}
