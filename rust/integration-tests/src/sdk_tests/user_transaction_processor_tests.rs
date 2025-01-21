use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub fn setup_user_txn_processor_config(
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

    let processor_config = ProcessorConfig::UserTransactionProcessor(default_processor_config);

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
mod sdk_user_txn_processor_tests {
    use super::setup_user_txn_processor_config;
    use crate::{
        diff_test_helper::user_transaction_processor::load_data,
        sdk_tests::{
            run_processor_test, setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_103958588_MULTI_AGENTS,
        IMPORTED_MAINNET_TXNS_1803170308_USER_TXN_MULTI_KEY_KEYLESS,
        IMPORTED_MAINNET_TXNS_2175935_USER_TXN_MULTI_ED25519,
        IMPORTED_MAINNET_TXNS_407418623_USER_TXN_SINGLE_KEY_SECP256K1_ECDSA,
        IMPORTED_MAINNET_TXNS_464961735_USER_TXN_SINGLE_KEY_ED25519,
        IMPORTED_MAINNET_TXNS_527013476_USER_TXN_SINGLE_SENDER_SECP256K1_ECDSA,
        IMPORTED_MAINNET_TXNS_551057865_USER_TXN_SINGLE_SENDER_WEBAUTH,
        IMPORTED_MAINNET_TXNS_590098441_USER_TXN_SINGLE_SENDER_ED25519,
        IMPORTED_MAINNET_TXNS_685_USER_TXN_ED25519,
        IMPORTED_MAINNET_TXNS_976087151_USER_TXN_SINGLE_SENDER_KEYLESS,
        IMPORTED_TESTNET_TXNS_769222973_MULTISIG,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::user_transaction_processor::UserTransactionProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_key_keyless_signature() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_1803170308_USER_TXN_MULTI_KEY_KEYLESS,
            Some("test_multi_key_keyless_signature".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_ed25519() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_2175935_USER_TXN_MULTI_ED25519,
            Some("test_multi_ed25519".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_secp() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_407418623_USER_TXN_SINGLE_KEY_SECP256K1_ECDSA,
            Some("test_single_key_secp".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_key_ed25519() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_464961735_USER_TXN_SINGLE_KEY_ED25519,
            Some("test_single_key_ed25519".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_sender_secp() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_527013476_USER_TXN_SINGLE_SENDER_SECP256K1_ECDSA,
            Some("test_single_sender_secp".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_sender_webauth() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_551057865_USER_TXN_SINGLE_SENDER_WEBAUTH,
            Some("test_single_sender_webauth".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_sender_ed25519() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_590098441_USER_TXN_SINGLE_SENDER_ED25519,
            Some("test_single_sender_ed25519".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_ed25519() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_685_USER_TXN_ED25519,
            Some("test_single_ed25519".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_sender_keyless() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_976087151_USER_TXN_SINGLE_SENDER_KEYLESS,
            Some("test_single_sender_keyless".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_sig() {
        process_single_transactions(
            IMPORTED_TESTNET_TXNS_769222973_MULTISIG,
            Some("test_multi_sig".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_agent() {
        process_single_transactions(
            IMPORTED_MAINNET_TXNS_103958588_MULTI_AGENTS,
            Some("test_multi_agent".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_transactions(txn: &[u8], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_user_txn_processor_config(&test_context, &db_url);

        let user_txn_processor = UserTransactionProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create UserTransactionProcessor");

        match run_processor_test(
            &mut test_context,
            user_txn_processor,
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
