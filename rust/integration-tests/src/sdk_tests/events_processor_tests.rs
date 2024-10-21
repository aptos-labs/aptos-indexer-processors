use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub async fn setup_events_processor_config(
    test_context: &SdkTestContext,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config(txn_count as u64); // since this will be always 1, we can remove from the arg list
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_genesis_txn() {
        process_single_testnet_event_txn(IMPORTED_TESTNET_TXNS_1_GENESIS, "genesis_txn_test").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_new_block_event() {
        process_single_testnet_event_txn(
            IMPORTED_TESTNET_TXNS_2_NEW_BLOCK_EVENT,
            "new_block_event_test",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_empty_txn() {
        process_single_testnet_event_txn(IMPORTED_TESTNET_TXNS_3_EMPTY_TXN, "empty_txn_test").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_coin_register_fa_metadata() {
        process_single_testnet_event_txn(
            IMPORTED_TESTNET_TXNS_278556781_V1_COIN_REGISTER_FA_METADATA,
            "coin_register_fa_metadata_test",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_fa_metadata() {
        process_single_testnet_event_txn(
            IMPORTED_TESTNET_TXNS_1255836496_V2_FA_METADATA_,
            "fa_metadata_test",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_events_processor_fa_activities() {
        process_single_testnet_event_txn(
            IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
            "fa_activities_test",
        )
        .await;
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
        let (indexer_processor_config, _processor_name) =
            setup_events_processor_config(&test_context, transaction_batches.len(), &db_url).await;

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");
        let processor_name = events_processor.name();
        match run_processor_test(
            &mut test_context,
            events_processor,
            load_data,
            db_url,
            2,
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

    // Helper function to abstract out the single transaction processing
    async fn process_single_testnet_event_txn(txn: &[u8], test_case_name: &str) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_events_processor_config(&test_context, 1, &db_url).await;

        let events_processor = EventsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create EventsProcessor");

        match run_processor_test(
            &mut test_context,
            events_processor,
            load_data,
            db_url,
            1,
            diff_flag,
            output_path.clone(),
            Some(test_case_name.to_string()),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    1,
                    processor_name,
                    output_path.clone(),
                    Some(test_case_name.to_string()),
                );
            },
            Err(e) => {
                eprintln!(
                    "[ERROR] Failed to run processor for txn version {}: {}",
                    1, e
                );
                panic!("Test failed due to processor error");
            },
        }
    }
}
