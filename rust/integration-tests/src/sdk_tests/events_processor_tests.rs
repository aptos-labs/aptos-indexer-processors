use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub fn setup_events_processor_config(
    test_context: &SdkTestContext,
    txn_version: u64,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config =
        test_context.create_transaction_stream_config(txn_version, txn_count as u64); // since this will be always 1, we can remove from the arg list
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
    let processor_name = processor_config.name();
    (
        IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
        },
        processor_name,
    )
}

#[allow(clippy::needless_return)]
#[cfg(test)]
mod tests {
    use crate::{
        diff_test_helper::event_processor::load_data,
        sdk_tests::{
            events_processor_tests::setup_events_processor_config, run_processor_test,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
    use aptos_indexer_test_transactions::{
        IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_, IMPORTED_TESTNET_TXNS_1_GENESIS,
        IMPORTED_TESTNET_TXNS_278556781_V1_COIN_REGISTER_FA_METADATA,
        IMPORTED_TESTNET_TXNS_2_NEW_BLOCK_EVENT, IMPORTED_TESTNET_TXNS_3_EMPTY_TXN,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use aptos_protos::transaction::v1::Transaction;
    use sdk_processor::processors::events_processor::EventsProcessor;
    use testing_transactions::{
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
    };

    // TODO - Add more intentional tests to validate the processor with different scenarios
    // Example test1: This test cases runs the events processor and validates the output of all available transactions proto jsons
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_db_output_diff_test() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        // Step 1: set up an input transaction that will be used
        pub const ALL_IMPORTED_TESTNET_TXNS: &[&[u8]] = &[
            IMPORTED_TESTNET_TXNS_1_GENESIS,
            IMPORTED_TESTNET_TXNS_2_NEW_BLOCK_EVENT,
            IMPORTED_TESTNET_TXNS_3_EMPTY_TXN,
            IMPORTED_TESTNET_TXNS_278556781_V1_COIN_REGISTER_FA_METADATA,
            IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_,
            IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
            IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
            IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
        ];

        let transaction_batches = ALL_IMPORTED_TESTNET_TXNS
            .iter()
            .map(|txn| serde_json::from_slice(txn).unwrap())
            .collect::<Vec<Transaction>>();

        let (db, mut test_context) = setup_test_environment(ALL_IMPORTED_TESTNET_TXNS).await;

        // Step 2: Loop over each transaction and run the test for each
        for (ind, _txn) in transaction_batches.iter().enumerate() {
            let txn_version = (ind + 1) as u64;

            // Step 3: Run the processor
            let db_url = db.get_db_url();

            let (indexer_processor_config, processor_name) =
                setup_events_processor_config(&test_context, txn_version, 1, &db_url);

            let events_processor = EventsProcessor::new(indexer_processor_config)
                .await
                .expect("Failed to create EventsProcessor");

            match run_processor_test(
                &mut test_context,
                events_processor,
                load_data,
                db_url,
                vec![txn_version as i64],
                diff_flag,
                output_path.clone(),
                None,
            )
            .await
            {
                Ok(mut db_value) => {
                    let _ = validate_json(
                        &mut db_value,
                        txn_version,
                        processor_name,
                        output_path.clone(),
                        None,
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

    // Example 2: Test for multiple transactions handling
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_db_output_scenario_testing() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        let imported = [
            IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
            IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        ];

        let (db, mut test_context) = setup_test_environment(&imported).await;

        let transaction_batches: Vec<Transaction> = imported
            .iter()
            .map(|txn| serde_json::from_slice(txn).expect("Failed to deserialize transaction"))
            .collect();

        let starting_version = 1;

        let db_url = db.get_db_url();
        let (indexer_processor_config, _processor_name) = setup_events_processor_config(
            &test_context,
            starting_version,
            transaction_batches.len(),
            &db_url,
        );

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");
        let processor_name = events_processor.name();
        match run_processor_test(
            &mut test_context,
            events_processor,
            load_data,
            db_url,
            vec![(starting_version as i64), ((starting_version + 1) as i64)], // we can pass multiple versions,
            diff_flag,
            output_path.clone(),
            Some("multi_txns_handling_test".to_string()),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    starting_version,
                    processor_name,
                    output_path.clone(),
                    Some("multi_txns_handling_test".to_string()),
                );
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] Failed to run processor for txn version {}: {}",
                    starting_version, e
                );
                panic!("Test failed due to processor error");
            },
        }
    }
}
