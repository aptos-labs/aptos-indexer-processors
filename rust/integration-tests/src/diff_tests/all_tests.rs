#[allow(clippy::needless_return)]
#[cfg(test)]
mod test {

    use crate::{
        diff_test_helper::{
            event_processor::load_data as load_event_data,
            fungible_asset_processor::load_data as load_fungible_asset_data,
            token_v2_processor::load_data as load_token_v2_data,
        },
        diff_tests::diff_tests_helper::{
            get_expected_imported_mainnet_txns, get_expected_imported_testnet_txns,
            get_expected_scripted_txns, remove_inserted_at, remove_transaction_timestamp,
        },
        DiffTest, TestContext, TestProcessorConfig, TestType,
    };
    use aptos_indexer_test_transactions::{
        ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS, ALL_SCRIPTED_TRANSACTIONS,
    };
    use aptos_indexer_testing_framework::{
        sdk_test_context::{generate_output_file},
    };
    use assert_json_diff::assert_json_eq;
    use diesel::pg::PgConnection;
    use processor::processors::token_v2_processor::TokenV2ProcessorConfig;
    use std::fs;
    use aptos_indexer_testing_framework::cli_parser::get_test_config;

    const DEFAULT_OUTPUT_FOLDER: &str = "expected_db_output_files";

    #[tokio::test]
    async fn test_all_testnet_txns_schema_output_for_all_processors() {
        let (generate_output_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/imported_testnet_txns");

        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_IMPORTED_TESTNET_TXNS).await.unwrap();

        run_processor_tests(
            processor_configs,
            &test_context,
            generate_output_flag,
            output_path,
            false,
            get_expected_imported_testnet_txns,
        )
        .await;
    }

    #[tokio::test]
    async fn test_all_mainnet_txns_schema_output_for_all_processors() {
        let (generate_output_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/imported_mainnet_txns");

        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_IMPORTED_MAINNET_TXNS).await.unwrap();
        run_processor_tests(
            processor_configs,
            &test_context,
            generate_output_flag,
            output_path,
            false,
            get_expected_imported_mainnet_txns,
        )
        .await;
    }

    #[tokio::test]
    async fn test_all_scripted_txns_schema_output_for_all_processors() {
        let (generate_output_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/scripted_txns");

        let processor_configs = get_processor_configs();
        let test_context = TestContext::new(ALL_SCRIPTED_TRANSACTIONS).await.unwrap();

        run_processor_tests(
            processor_configs,
            &test_context,
            generate_output_flag,
            output_path,
            true,
            get_expected_scripted_txns,
        )
        .await;
    }

    // Helper function to reduce duplicate code for running tests on all processors
    async fn run_processor_tests(
        processor_configs: Vec<TestProcessorConfig>,
        test_context: &TestContext,
        generate_output_flag: bool,
        output_path: String,
        _scripted: bool,
        get_expected_json_path_fn: fn(&str, &str, &str) -> String,
    ) {
        for processor_config in processor_configs {
            let processor_name = processor_config.config.name();
            let test_type = TestType::Diff(DiffTest);
            let output_path = output_path.clone();

            let db_values_fn = match processor_name {
                "events_processor" => load_event_data,
                "fungible_asset_processor" => load_fungible_asset_data,
                "token_v2_processor" => load_token_v2_data,
                _ => panic!("Unknown processor: {}", processor_name),
            };

            test_context
                    .run(
                        processor_config,
                        test_type,
                        move |conn: &mut PgConnection, txn_version: &str| {

                            let mut db_values = match db_values_fn(conn, txn_version) {
                                Ok(db_data) => db_data,
                                Err(e) => {
                                    eprintln!(
                                        "[ERROR] Failed to load data for processor {} and transaction version {}: {}",
                                        processor_name, txn_version, e
                                    );
                                    return Err(e);
                                },
                            };

                            // Iterate over each table in the map and validate its data
                            for (table_name, db_value) in db_values.iter_mut() {
                                if generate_output_flag {
                                    println!("[TEST] Generating output files for all tables.");
                                    remove_inserted_at(db_value);
                                    // Iterate over each table's data in the HashMap and generate an output file
                                    generate_output_file(
                                        processor_name,
                                        table_name,
                                        txn_version,
                                        db_value,
                                        output_path.clone(),
                                    )?;
                                }

                                // Generate the expected JSON file path for each table
                                let expected_file_path = &get_expected_json_path_fn(processor_name, txn_version, table_name);

                                // Read and parse the expected JSON file for the current table
                                let mut expected_json = match read_and_parse_json(expected_file_path) {
                                    Ok(json) => json,
                                    Err(e) => {
                                        eprintln!(
                                            "[ERROR] Error handling JSON for processor {} table {} and transaction version {}: {}",
                                            processor_name, table_name, txn_version, e
                                        );
                                        panic!("Failed to read and parse JSON for table: {}", table_name);
                                    },
                                };

                                // TODO: Clean up non-deterministic fields (e.g., timestamps, `inserted_at`)
                                remove_inserted_at(db_value);
                                remove_transaction_timestamp(db_value);
                                remove_inserted_at(&mut expected_json);
                                remove_transaction_timestamp(&mut expected_json);

                                // Validate the actual vs expected JSON for the current table
                                assert_json_eq!(db_value, expected_json);
                            }

                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
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
