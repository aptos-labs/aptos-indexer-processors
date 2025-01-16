use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub fn setup_default_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config(); // since this will be always 1, we can remove from the arg list
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

    let processor_config = ProcessorConfig::DefaultProcessor(default_processor_config);
    let processor_name = processor_config.name();
    (
        IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
            backfill_config: None,
        },
        processor_name,
    )
}

#[allow(clippy::needless_return)]
#[cfg(test)]
mod tests {
    use crate::{
        diff_test_helper::default_processor::load_data,
        sdk_tests::{
            default_processor_tests::setup_default_processor_config, run_processor_test,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_155112189_DEFAULT_TABLE_ITEMS,
        IMPORTED_MAINNET_TXNS_1845035942_DEFAULT_CURRENT_TABLE_ITEMS,
        IMPORTED_MAINNET_TXNS_423176063_ACCOUNT_TRANSACTION_DELETE,
        IMPORTED_MAINNET_TXNS_513424821_DEFAULT_BLOCK_METADATA_TRANSACTIONS,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::default_processor::DefaultProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_table_items() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_155112189_DEFAULT_TABLE_ITEMS,
            Some("test_table_items".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_current_table_items() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_1845035942_DEFAULT_CURRENT_TABLE_ITEMS,
            Some("test_current_table_items".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_block_metadata_txns() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_513424821_DEFAULT_BLOCK_METADATA_TRANSACTIONS,
            Some("block_metadata_transactions".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_resource_test() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_423176063_ACCOUNT_TRANSACTION_DELETE,
            Some("delete_resource_test".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the single transaction processing
    async fn process_single_mainnet_event_txn(txn: &[u8], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_mainnet_txns", DEFAULT_OUTPUT_FOLDER));

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_default_processor_config(&test_context, &db_url);

        let default_processor = DefaultProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create DefaultProcessor");

        match run_processor_test(
            &mut test_context,
            default_processor,
            load_data,
            db_url,
            diff_flag,
            output_path.clone(),
            test_case_name.clone(),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    test_context.get_request_start_version(),
                    processor_name,
                    output_path.clone(),
                    test_case_name,
                );
            },
            Err(e) => {
                panic!(
                    "Test failed on transactions {:?} due to processor error: {}",
                    test_context.get_test_transaction_versions(),
                    e
                );
            },
        }
    }
}
