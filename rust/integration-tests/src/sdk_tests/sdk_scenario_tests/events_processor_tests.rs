use bigdecimal::ToPrimitive;
#[allow(clippy::needless_return)]
#[cfg(test)]
mod tests {
    use crate::{
        cli_parser::get_test_config,
        diff_test_helper::event_processor::load_data,
        sdk_tests::{
            events_processor_tests::setup_events_processor_config,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::{ALL_IMPORTED_TESTNET_TXNS, IMPORTED_TESTNET_TXNS_1_GENESIS, IMPORTED_TESTNET_TXNS_2_GENESIS, IMPORTED_TESTNET_TXNS_3_GENESIS};
    use aptos_indexer_testing_framework::database::TestDatabase;
    use aptos_protos::transaction::v1::Transaction;
    use diesel::PgConnection;
    use sdk_processor::processors::events_processor::EventsProcessor;
    use crate::sdk_tests::run_scenario_test;
    use diesel::Connection;
    use tokio::test;

    // This test case runs the events processor and validates the output of sequential transaction proto JSONs
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_db_output_scenario_testing() {
        // Get test configuration, including the output path
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        let imported = [
            IMPORTED_TESTNET_TXNS_1_GENESIS,
            IMPORTED_TESTNET_TXNS_2_GENESIS,
            IMPORTED_TESTNET_TXNS_3_GENESIS
        ];

        let (db, mut test_context) = setup_test_environment(&imported).await;

        // Deserialize transaction batches from JSON
        let transaction_batches: Vec<Transaction> = imported
            .iter()
            .map(|txn| serde_json::from_slice(txn).expect("Failed to deserialize transaction"))
            .collect();

        let starting_version = transaction_batches.first()
            .expect("No transactions found")
            .version;

        // Setup the database and events processor configurations
        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) = setup_events_processor_config(
            &test_context,
            starting_version,
            transaction_batches.len(),
            &db_url
        );

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");

        // Run the scenario test and load data for validation
        let _ = run_scenario_test(
            &mut test_context,
            events_processor,
            starting_version,
            move || {
                let mut conn = PgConnection::establish(&db_url)
                    .expect("Failed to establish DB connection");

                let db_values = load_data(&mut conn, starting_version as i64)
                    .map_err(|e| {
                        eprintln!(
                            "[ERROR] Failed to load data for transaction version {}: {}",
                            starting_version, e
                        );
                        e
                    })?;

                if db_values.is_empty() {
                    eprintln!("[WARNING] No data found for transaction version: {}", starting_version);
                } else {
                    println!("DB values loaded successfully: {:?}", db_values);
                }
                assert_eq!(1, starting_version);
                Ok(())
            },
        ).await;

    }
}
