use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::{IndexerProcessorConfig, ProcessorMode, TestingConfig},
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
    let testing_config: TestingConfig = TestingConfig {
        override_starting_version: transaction_stream_config.starting_version.unwrap(),
        ending_version: transaction_stream_config.request_ending_version.unwrap(),
    };

    (
        IndexerProcessorConfig {
            processor_config,
            transaction_stream_config,
            db_config,
            backfill_config: None,
            bootstrap_config: None,
            testing_config: Some(testing_config),
            mode: ProcessorMode::Testing,
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
            validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_DEVNET_TXNS_78753811_COIN_TRANSFER_WITH_V2_EVENTS,
        IMPORTED_MAINNET_TXNS_1680592683_FA_MIGRATION_COIN_INFO,
        IMPORTED_MAINNET_TXNS_1737056775_COIN_TRANSFER_BURN_EVENT,
        IMPORTED_MAINNET_TXNS_1957950162_FA_MIGRATION_V2_STORE_ONLY,
        IMPORTED_MAINNET_TXNS_2186504987_COIN_STORE_DELETION_NO_EVENT,
        IMPORTED_MAINNET_TXNS_2308282694_ASSET_TYPE_V1_NULL,
        IMPORTED_MAINNET_TXNS_2308283617_ASSET_TYPE_V1_NULL_2,
        IMPORTED_MAINNET_TXNS_255894550_STORAGE_REFUND,
        IMPORTED_MAINNET_TXNS_508365567_FA_V1_EVENTS,
        IMPORTED_MAINNET_TXNS_550582915_MULTIPLE_TRANSFER_EVENT,
        IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS,
        IMPORTED_TESTNET_TXNS_1200394037_FA_V2_FROZEN_EVENT,
        IMPORTED_TESTNET_TXNS_2646510387_CONCURRENT_FA,
        IMPORTED_TESTNET_TXNS_4462417704_SECONDARY_STORE_BURNT,
        IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN,
        IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER,
        IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES, IMPORTED_TESTNET_TXNS_646928741_NO_EVENTS,
    };
    use aptos_indexer_testing_framework::{
        cli_parser::get_test_config,
        database::{PostgresTestDatabase, TestDatabase},
        sdk_test_context::SdkTestContext,
    };
    use sdk_processor::processors::fungible_asset_processor::FungibleAssetProcessor;

    // Test case for processing a specific testnet transaction (Validator Transaction)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_validator_txn() {
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_5523474016_VALIDATOR_TXN],
            Some("validator_txn_test".to_string()),
        )
        .await;
    }

    // Test case for processing another testnet transaction (Coin Register)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_register_txn() {
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_5979639459_COIN_REGISTER],
            Some("coin_register_txn_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_activities_txn() {
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_5992795934_FA_ACTIVITIES],
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
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_999929475_COIN_AND_FA_TRANSFERS],
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
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_508365567_FA_V1_EVENTS],
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
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_1200394037_FA_V2_FROZEN_EVENT],
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
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_2646510387_CONCURRENT_FA],
            Some("concurrent_fa_test".to_string()),
        )
        .await;
    }

    /// Tests processing of coin v2 events
    /// Validates the handling of updated coin event formats
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_v2_events() {
        process_single_batch_txns(
            &[IMPORTED_DEVNET_TXNS_78753811_COIN_TRANSFER_WITH_V2_EVENTS],
            Some("coin_v2_events".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_store_deletion_no_event() {
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_2186504987_COIN_STORE_DELETION_NO_EVENT],
            Some("coin_store_deletion_no_event".to_string()),
        )
        .await;
    }

    /// Tests processing of secondary store burn operations
    /// Validates correct handling of burning tokens from secondary stores
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_secondary_store_burnt() {
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_4462417704_SECONDARY_STORE_BURNT],
            Some("secondary_store_burnt".to_string()),
        )
        .await;
    }

    /// Tests gas event processing when no other events are present
    /// Validates correct handling of isolated gas events
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_gas_event_when_events_is_empty() {
        process_single_batch_txns(
            &[IMPORTED_TESTNET_TXNS_646928741_NO_EVENTS],
            Some("gas_event_when_events_is_empty".to_string()),
        )
        .await;
    }

    /// Tests processing of coin transfer burn events
    /// Validates handling of burn operations during coin transfers
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_coin_transfer_burn_event() {
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_1737056775_COIN_TRANSFER_BURN_EVENT],
            Some("coin_transfer_burn_event".to_string()),
        )
        .await;
    }

    /// Tests processing of multiple transfer events in a single transaction
    /// Validates correct handling of batch transfers
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_multiple_transfer_event() {
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_550582915_MULTIPLE_TRANSFER_EVENT],
            Some("multiple_transfer_event".to_string()),
        )
        .await;
    }

    /// Tests processing of storage refund operations
    /// Validates correct handling of storage refund mechanics
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_storage_refund() {
        process_single_batch_txns(
            &[IMPORTED_MAINNET_TXNS_255894550_STORAGE_REFUND],
            Some("storage_refund".to_string()),
        )
        .await;
    }

    /// Test FA migration. It's a 2 transaction test where the
    /// first transaction creates a CoinInfo and the second
    /// processes an FA version of that coin
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_migration_same_batch() {
        process_single_batch_txns(
            &[
                IMPORTED_MAINNET_TXNS_1680592683_FA_MIGRATION_COIN_INFO,
                IMPORTED_MAINNET_TXNS_1957950162_FA_MIGRATION_V2_STORE_ONLY,
            ],
            Some("fa_migration".to_string()),
        )
        .await;
    }

    // TODO: I really want to make this work but it doesn't right now
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_fa_migration_different_batch() {
        sequential_multi_transaction_helper_function(
            &[
                &[IMPORTED_MAINNET_TXNS_1680592683_FA_MIGRATION_COIN_INFO],
                &[IMPORTED_MAINNET_TXNS_1957950162_FA_MIGRATION_V2_STORE_ONLY],
            ],
            "fa_migration_2_batch",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fungible_asset_processor_asset_type_null() {
        sequential_multi_transaction_helper_function(
            &[&[IMPORTED_MAINNET_TXNS_2308282694_ASSET_TYPE_V1_NULL], &[
                IMPORTED_MAINNET_TXNS_2308283617_ASSET_TYPE_V1_NULL_2,
            ]],
            "asset_type_null",
        )
        .await;
    }

    /// Tests processing of two transactions sequentially
    /// Validates handling of multiple transactions with shared context
    async fn sequential_multi_transaction_helper_function(
        txn_batches: &[&[&[u8]]],
        output_name: &str,
    ) {
        let (generate_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();

        for (i, txn_batch) in txn_batches.iter().enumerate() {
            let is_last = i == txn_batches.len() - 1;
            process_transactions(
                &mut db,
                txn_batch,
                output_name,
                is_last && generate_flag,
                &output_path,
                is_last,
            )
            .await;
        }
    }

    async fn process_transactions(
        db: &mut PostgresTestDatabase,
        txns: &[&[u8]],
        transaction_name: &str,
        generate_flag: bool,
        output_path: &str,
        should_validate: bool,
    ) {
        let mut test_context = SdkTestContext::new(txns);
        if test_context.init_mock_grpc().await.is_err() {
            panic!("Failed to initialize mock grpc");
        };

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
            generate_flag,
            output_path.to_string(),
            Some(transaction_name.to_string()),
        )
        .await
        {
            Ok(mut db_value) => {
                if should_validate {
                    let _ = validate_json(
                        &mut db_value,
                        test_context.get_request_start_version(),
                        processor_name,
                        output_path.to_string(),
                        Some(transaction_name.to_string()),
                    );
                }
            },
            Err(e) => {
                panic!(
                    "Test failed on {} due to processor error: {}",
                    transaction_name, e
                );
            },
        }
    }

    // Update the existing process_single_batch_txns function to use the new merged function
    async fn process_single_batch_txns(txns: &[&[u8]], test_case_name: Option<String>) {
        let (generate_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let mut db = PostgresTestDatabase::new();
        db.setup().await.unwrap();

        process_transactions(
            &mut db,
            txns,
            &test_case_name.unwrap_or_default(),
            generate_flag,
            &output_path,
            true, // Assuming we want to validate for fungible asset transactions
        )
        .await;
    }
}
