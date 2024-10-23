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
    staring_version: u64,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config =
        test_context.create_transaction_stream_config(staring_version, txn_count as u64);
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
            backfill_config: None,
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
        IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS,
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use once_cell::sync::Lazy;
    use sdk_processor::processors::fungible_asset_processor::FungibleAssetProcessor;
    use std::sync::Mutex;

    // Test case for processing a specific testnet transaction (Validator Transaction)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_validator_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
            5523474016,
            Some("validator_txn_test".to_string()),
        )
        .await;
    }

    // Test case for processing another testnet transaction (Coin Register)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_register_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
            5979639459,
            Some("coin_register_txn_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_activities_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
            5992795934,
            Some("fa_activities_txn_test".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following tables:
     * - coin_supply
     * - current_fungible_asset_balances
     * - fungible_asset_balances
     * - fungible_asset_activities
     * - fungible_asset_metadata
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_and_fa_transfers() {
        process_single_testnet_fa_txns(
            IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS,
            999929475,
            Some("coin_and_fa_transfers_test".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_testnet_fa_txns(
        txn: &[u8],
        txn_version: i64,
        test_case_name: Option<String>,
    ) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| format!("{}/imported_testnet_txns", DEFAULT_OUTPUT_FOLDER));

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_fa_processor_config(&test_context, txn_version as u64, 1, &db_url);

        let fungible_asset_processor = FungibleAssetProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create FungibleAssetProcessor");

        match run_processor_test(
            &mut test_context,
            fungible_asset_processor,
            load_data,
            db_url,
            vec![txn_version],
            diff_flag,
            output_path.clone(),
            test_case_name.clone(),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    txn_version as u64,
                    processor_name,
                    output_path.clone(),
                    test_case_name,
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
