use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub fn setup_account_restoration_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config();
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

    let processor_config = ProcessorConfig::AccountRestorationProcessor(default_processor_config);

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
mod sdk_account_restoration_processor_tests {
    use super::setup_account_restoration_processor_config;
    use crate::{
        diff_test_helper::account_restoration_processor::load_data,
        sdk_tests::{
            run_processor_test, setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_2200077591_ACCOUNT_RESTORATION_SINGLE_ED25519,
        IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY,
        IMPORTED_MAINNET_TXNS_2200077877_ACCOUNT_RESTORATION_ROTATED_TO_SINGLE_SECP256K1,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::account_restoration_processor::AccountRestorationProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_ed25519() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_2200077591_ACCOUNT_RESTORATION_SINGLE_ED25519,
            Some("test_single_key_ed25519".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_key_after_rotation() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_2200077800_ACCOUNT_RESTORATION_ROTATED_TO_MULTI_KEY,
            Some("test_multi_key_after_rotation".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_secp256k1_after_rotation() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_2200077877_ACCOUNT_RESTORATION_ROTATED_TO_SINGLE_SECP256K1,
            Some("test_single_key_secp256k1_after_rotation".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_transaction(txn: &[u8], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_account_restoration_processor_config(&test_context, &db_url);

        let account_restoration_processor =
            AccountRestorationProcessor::new(indexer_processor_config)
                .await
                .expect("Failed to create AccountRestorationProcessor");

        match run_processor_test(
            &mut test_context,
            account_restoration_processor,
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
