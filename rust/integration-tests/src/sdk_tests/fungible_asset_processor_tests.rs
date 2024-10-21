use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use std::collections::HashSet;

pub fn setup_fa_processor_config(
    test_context: &SdkTestContext,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config(txn_count as u64);
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

    let processor_config = ProcessorConfig::FungibleAssetProcessor(default_processor_config);

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
        diff_test_helper::fungible_asset_processor::load_data,
        sdk_tests::{
            fungible_asset_processor_tests::setup_fa_processor_config, run_processor_test,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::{
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::fungible_asset_processor::FungibleAssetProcessor;

    // Test case for processing a specific testnet transaction (Validator Transaction)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_validator_txn() {
        process_single_testnet(
            IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
            "validator_txn_test",
        )
        .await;
    }

    // Test case for processing another testnet transaction (Coin Register)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_register_txn() {
        process_single_testnet(
            IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
            "coin_register_txn_test",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_activities_txn() {
        process_single_testnet(
            IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
            "fa_activities_txn_test",
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_testnet(txn: &[u8], test_case_name: &str) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_fa_processor_config(&test_context, 1, &db_url);

        let fungible_asset_processor = FungibleAssetProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create FungibleAssetProcessor");

        match run_processor_test(
            &mut test_context,
            fungible_asset_processor,
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
