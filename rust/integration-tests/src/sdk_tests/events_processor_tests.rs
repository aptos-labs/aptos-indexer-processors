#[allow(clippy::needless_return)]
#[cfg(test)]
mod tests {
    use crate::{
        diff_test_helper::{
            processors::event_processor::EventsProcessorTestHelper,
        },
        sdk_tests::test_cli_flag_util::{parse_test_args, TestArgs},
    };
    use ahash::AHashMap;
    use aptos_indexer_test_transactions::ALL_IMPORTED_TESTNET_TXNS;
    use aptos_indexer_testing_framework::{
        database::{TestDatabase},
    };
    use aptos_protos::transaction::v1::Transaction;

    use sdk_processor::{
        config::{
            db_config::{DbConfig, PostgresConfig},
            indexer_processor_config::IndexerProcessorConfig,
            processor_config::{DefaultProcessorConfig, ProcessorConfig},
        },
        processors::events_processor::EventsProcessor,
    };
    use std::{collections::HashSet, sync::Mutex};
    use crate::sdk_tests::validate_json;
    use crate::sdk_tests::setup_test_environment;
    use crate::sdk_tests::run_processor_test;
    use crate::sdk_tests::get_test_config;
    use crate::sdk_tests::DEFAULT_OUTPUT_FOLDER;

    // This test cases runs the events processor and validates the output of all available transactions proto jsons
    #[tokio::test]
    async fn testnet_events_processor_db_output_diff_test() {
        let (diff_flag , custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| {
            DEFAULT_OUTPUT_FOLDER.to_string() + "imported_testnet_txns"});
        
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

            let transaction_stream_config = test_context.create_transaction_stream_config(txn_version);
            let postgres_config = PostgresConfig {
                connection_string: db_url.to_string(),
                db_pool_size: 100,
            };

            let db_config = DbConfig::PostgresConfig(postgres_config);
            let default_processor_config = DefaultProcessorConfig {
                per_table_chunk_sizes: AHashMap::new(),
                channel_size: 100,
                deprecated_tables: HashSet::new(),
            };

            let processor_config = ProcessorConfig::EventsProcessor(default_processor_config);
            let process_name = processor_config.name();
            let indexer_processor_config = IndexerProcessorConfig {
                processor_config,
                transaction_stream_config,
                db_config,
            };

            let events_processor = EventsProcessor::new(indexer_processor_config)
                .await
                .expect("Failed to create EventsProcessor");
            let test_helper =
                EventsProcessorTestHelper;


            match run_processor_test(
                &mut test_context,
                events_processor,
                test_helper,
                &db_url,
                txn_version,
                diff_flag,
                output_path.clone(),
            )
            .await
            {
                Ok(mut db_value) => {
                    let _ = validate_json(
                        &mut db_value,
                        txn_version,
                        process_name,
                        output_path.clone(),
                    );
                },
                Err(e) => {
                    eprintln!(
                        "[ERROR] Failed to run processor for txn version {}: {}",
                        txn_version, e
                    );
                    panic!("Test failed due to processor error");
                },
            }
        }
    }
}
