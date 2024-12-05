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
mod sdk_fungible_asset_processor_tests {
    use crate::{
        diff_test_helper::fungible_asset_processor::load_data,
        sdk_tests::{
            fungible_asset_processor_tests::setup_fa_processor_config, run_processor_test,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::{
        IMPORTED_DEVNET_TXNS_78753811_COIN_TRANSFER_WITH_V2_EVENTS,
        IMPORTED_MAINNET_TXNS_508365567_FA_V1_EVENTS,
        IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS,
        IMPORTED_TESTNET_TXNS_1200394037_FA_V2_FROZEN_EVENT,
        IMPORTED_TESTNET_TXNS_2646510387_CONCURRENT_FA,
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::fungible_asset_processor::FungibleAssetProcessor;

    // Test case for processing a specific testnet transaction (Validator Transaction)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_validator_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
            Some("validator_txn_test".to_string()),
        )
        .await;
    }

    // Test case for processing another testnet transaction (Coin Register)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_register_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
            Some("coin_register_txn_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_activities_txn() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES,
            Some("fa_activities_txn_test".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x1::fungible_asset::Supply
     *      - 0x1::fungible_asset::Metadata
     *      - 0x1::fungible_asset::FungibleStore
     * - Events
     *      - 0x1::coin::WithdrawEvent
     *      - 0x1::coin::DepositEvents
     *      - 0x1::aptos_coin::GasFeeEvent
     *      - 0x1::fungible_asset::Deposit
     *      - 0x1::fungible_asset::Withdraw
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_and_fa_transfers() {
        process_single_testnet_fa_txns(
            IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS,
            Some("coin_and_fa_transfers_test".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Events
     *      - 0x1::fungible_asset::DepositEvent
     *      - 0x1::fungible_asset::WithdrawEvent
     *      - 0x1::fungible_asset::FrozenEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_v1_events() {
        process_single_testnet_fa_txns(
            IMPORTED_MAINNET_TXNS_508365567_FA_V1_EVENTS,
            Some("v1_events_test".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Events
     *      - 0x1::fungible_asset::Frozen
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_v2_frozen_event() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_1200394037_FA_V2_FROZEN_EVENT,
            Some("v2_frozen_event_test".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x1::fungible_asset::ConcurrentSupply
     *      - 0x1::fungible_asset::ConcurrentFungibleBalance
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_concurrent_fa() {
        process_single_testnet_fa_txns(
            IMPORTED_TESTNET_TXNS_2646510387_CONCURRENT_FA,
            Some("concurrent_fa_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_v2_events() {
        process_single_testnet_fa_txns(
            IMPORTED_DEVNET_TXNS_78753811_COIN_TRANSFER_WITH_V2_EVENTS,
            Some("coin_v2_events".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_testnet_fa_txns(txn: &[u8], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(&[txn]).await;

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) =
            setup_fa_processor_config(&test_context, &db_url);

        let fungible_asset_processor = FungibleAssetProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create FungibleAssetProcessor");

        match run_processor_test(
            &mut test_context,
            fungible_asset_processor,
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
