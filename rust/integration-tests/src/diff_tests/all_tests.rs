use std::collections::HashMap;
use std::sync::Arc;
use crate::diff_test_helper::processors::event_processor::EventsProcessorTestHelper;
use crate::diff_test_helper::processors::fungible_asset_processor::FungibleAssetProcessorTestHelper;
use crate::diff_test_helper::processors::token_v2_processor::TokenV2ProcessorTestHelper;
use crate::diff_test_helper::ProcessorTestHelper;

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
        diff_tests::{
            get_expected_generated_txns, get_expected_imported_mainnet_txns,
            get_expected_imported_testnet_txns, remove_inserted_at,
        },
        DiffTest, TestContext, TestProcessorConfig, TestType,
    };
    use assert_json_diff::assert_json_eq;
    use diesel::pg::PgConnection;
    use std::{collections::HashMap, fs, sync::Arc};
    use processor::processors::token_v2_processor::TokenV2ProcessorConfig;
    use testing_transactions::{
        ALL_GENERATED_TXNS, ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS,
    };
    use crate::diff_test_helper::processors::token_v2_processor::TokenV2ProcessorTestHelper;
    use crate::diff_tests::get_processor_map;
    #[tokio::test]
    async fn test_all_testnet_txns_schema_output_for_all_processors() {
        let processor_map = get_processor_map();
        let processor_configs = vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::TokenV2Processor(TokenV2ProcessorConfig {
                    query_retries: 3,
                    query_retry_delay_ms: 1000,
                }),
            }
        ];

        let test_context = TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);

            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);

                test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {
                            let mut json_data = test_helper.load_data(conn, txn_version)?;

                            let trusted_json_path =
                                get_expected_imported_testnet_txns(processor_name, txn_version);

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

                            // Optionally remove fields that should be ignored during comparison (e.g.,`inserted_at`)
                            // TODO: we will need to handle other fields well such as state_key_hash,
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
        let processor_map = get_processor_map();

        let processor_configs = vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
        ];
        let test_context = TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);

            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);

                test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {

                            let mut json_data = test_helper.load_data(conn, txn_version)?;

                            let trusted_json_path =
                                get_expected_imported_mainnet_txns(processor_name, txn_version);

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
                            // remove_inserted_at(&mut trusted_json);

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
        let processor_map = get_processor_map();
        let processor_configs = vec![
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::EventsProcessor,
            },
            TestProcessorConfig {
                config: processor::processors::ProcessorConfig::FungibleAssetProcessor,
            },
        ];
        let test_context = TestContext::new(ALL_GENERATED_TXNS).await.unwrap();

        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);

            if let Some(test_helper) = processor_map.get(processor_name) {
                let test_helper = Arc::clone(test_helper);

                test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {

                            let mut json_data = test_helper.load_data(conn, txn_version)?;
                            // Read the trusted JSON file

                            let trusted_json_path =
                                get_expected_generated_txns(processor_name, txn_version);
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

pub fn get_processor_map() -> HashMap<String, Arc<Box<dyn ProcessorTestHelper>>> {
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
