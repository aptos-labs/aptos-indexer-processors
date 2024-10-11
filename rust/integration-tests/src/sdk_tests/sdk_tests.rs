#[cfg(test)]
mod tests {
    use crate::{
        diff_test_helper::{
            processors::event_processor::EventsProcessorTestHelper, ProcessorTestHelper,
        },
        diff_tests::{remove_inserted_at, remove_transaction_timestamp},
        sdk_tests::test_cli_flag_util::parse_test_args,
    };
    use aptos_indexer_test_transactions::{
        ALL_IMPORTED_TESTNET_TXNS, IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN
    };
    use aptos_protos::transaction::v1::Transaction;
    use assert_json_diff::assert_json_eq;
    use diesel::{pg::PgConnection, Connection, RunQueryDsl};
    use sdk_processor::{
        config::{
            indexer_processor_config::{DbConfig, IndexerProcessorConfig},
            processor_config::ProcessorConfig,
        },
        processors::events::events_processor::EventsProcessor,
        schema::events::dsl::*,
    };
    use std::{fs, path::Path};
    use testing_framework::{
        database::{PostgresTestDatabase, TestDatabase},
        new_test_context::SdkTestContext,
    };
    use once_cell::sync::Lazy;
    use std::sync::Mutex;
    use crate::sdk_tests::test_cli_flag_util::TestArgs;

    // Define a global static to store the parsed arguments
    static TEST_CONFIG: Lazy<Mutex<TestArgs>> = Lazy::new(|| {
        let args = parse_test_args();
        Mutex::new(args)
    });

    // Example function to fetch global test args
    fn get_test_config() -> TestArgs {
        TEST_CONFIG.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn test_run() {
        let test_config = get_test_config();
        let diff_flag = test_config.diff;
        let custom_output_path = test_config
            .output_path;

        // Step 1: set up an input transaction that will be used
        let transaction_batches = ALL_IMPORTED_TESTNET_TXNS
            .iter()
            .map(|txn| serde_json::from_slice(txn).unwrap())
            .collect::<Vec<Transaction>>();

        let (db, mut test_context) = setup_test_environment(ALL_IMPORTED_TESTNET_TXNS).await;

        // Step 2: Loop over each transaction and run the test for each
        for txn in transaction_batches.iter() {
            let txn_version = txn.version;

            // Step 3: Run the processor
            let db_url = db.get_db_url();
            let diff_flag = diff_flag; // Can use CLI flag or hardcoded value

            match run_processor_test(
                &mut test_context,
                &db_url,
                txn_version,
                diff_flag,
                custom_output_path.clone(),
            )
                .await
            {
                Ok(mut db_value) => {
                    validate_json(
                        &mut db_value,
                        txn_version,
                        "events_processor",
                        custom_output_path.clone(),
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[ERROR] Failed to run processor for txn version {}: {}",
                        txn_version, e
                    );
                    panic!("Test failed due to processor error");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_run2() {
        // Set up environment with a single transaction
        let imported_txns = [IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN];
        let txn_version = 5523474016; // TODO: More elegant way to get this value
        let (db, mut test_context) = setup_test_environment(&imported_txns).await;

        // Run the processor
        let db_url = db.get_db_url();
        let diff_flag = false;
        let custom_output_path = Some("custom_output_path".to_string());

        match run_processor_test(
            &mut test_context,
            &db_url,
            txn_version,
            diff_flag,
            custom_output_path.clone(),
        )
            .await
        {
            Ok(mut db_value) => {
                validate_json(
                    &mut db_value,
                    txn_version,
                    "events_processor",
                    custom_output_path,
                );
            }
            Err(e) => {
                eprintln!(
                    "[ERROR] Failed to run processor for txn version {}: {}",
                    txn_version, e
                );
                panic!("Test failed due to processor error");
            }
        }
    }

    // Helper function to configure and run the processor
    async fn run_processor_test(
        test_context: &mut SdkTestContext,
        db_url: &str,
        txn_version: u64,
        diff_flag: bool,
        output_path: Option<String>,
    ) -> anyhow::Result<serde_json::Value> {
        // Log test details
        println!("[INFO] Running test for transaction version: {}", txn_version);

        let transaction_stream_config = test_context.create_transaction_stream_config(txn_version);
        let db_config = DbConfig {
            postgres_connection_string: db_url.to_string(),
            db_pool_size: 100,
        };
        let processor_config = ProcessorConfig::EventsProcessor;

        let indexer_processor_config = IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        };

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");

        let db_value = test_context
            .run(
                &events_processor,
                db_url,
                txn_version,
                diff_flag,
                output_path.clone(),
                move |db_url| {
                    let mut conn =
                        PgConnection::establish(db_url).expect("Failed to establish DB connection");
                    let test_helper =
                        Box::new(EventsProcessorTestHelper) as Box<dyn ProcessorTestHelper>;

                    let json_data = match test_helper.load_data(&mut conn, &txn_version.to_string()) {
                        Ok(db_data) => db_data,
                        Err(e) => {
                            eprintln!(
                                "[ERROR] Failed to load data for transaction version {}: {}",
                                txn_version, e
                            );
                            return Err(e);
                        }
                    };

                    if json_data.is_null() {
                        eprintln!("[WARNING] No data found for txn version: {}", txn_version);
                    }

                    Ok(json_data)
                },
            )
            .await?;

        Ok(db_value)
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

    // Common setup for database and test context
    async fn setup_test_environment(
        transactions: &[&[u8]],
    ) -> (PostgresTestDatabase, SdkTestContext) {
        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();

        let test_context = SdkTestContext::new(transactions).await.unwrap();

        (db, test_context)
    }

    // Helper function to validate JSON
    fn validate_json(
        db_value: &mut serde_json::Value,
        txn_version: u64,
        processor_name: &str,
        output_path: Option<String>,
    ) {
        // Use the provided output path or fall back to the default value
        let output_path = output_path.unwrap_or_else(|| {
            "expected_db_output_files/imported_testnet_txns".to_string()  //TODO: replace with a proper default value
        });


        let expected_file_path = Path::new(&output_path)
            .join(processor_name)
            .join(format!("{}_{}.json", processor_name, txn_version));

        let mut expected_json = match read_and_parse_json(expected_file_path.to_str().unwrap()) {
            Ok(json) => json,
            Err(e) => {
                eprintln!(
                    "[ERROR] Error handling JSON for processor {} and transaction version {}: {}",
                    processor_name, txn_version, e
                );
                panic!("Failed to read and parse JSON");
            },
        };

        // Clean up non-deterministic fields (e.g., timestamps, `inserted_at`)
        remove_inserted_at(db_value);
        remove_transaction_timestamp(db_value);
        remove_inserted_at(&mut expected_json);
        remove_transaction_timestamp(&mut expected_json);

        println!("Actual JSON: {}", db_value.to_string());
        println!("Expected JSON: {}", expected_json.to_string());


        // Validate the actual vs expected JSON
        assert_json_eq!(db_value, expected_json);
    }
}
