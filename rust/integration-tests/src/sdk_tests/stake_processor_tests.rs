use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::config::{
    db_config::{DbConfig, PostgresConfig},
    indexer_processor_config::IndexerProcessorConfig,
    processor_config::{DefaultProcessorConfig, ProcessorConfig},
};
use sdk_processor::processors::stake_processor::StakeProcessorConfig;
use std::collections::HashSet;

pub async fn setup_stake_processor_config(
    test_context: &SdkTestContext,
    staring_version: u64,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config =
        test_context.create_transaction_stream_config(staring_version, txn_count as u64); // since this will be always 1, we can remove from the arg list
    let postgres_config = PostgresConfig {
        connection_string: db_url.to_string(),
        db_pool_size: 100,
    };

    let db_config = DbConfig::PostgresConfig(postgres_config);
    let default_processor_config = StakeProcessorConfig {
        default_config: DefaultProcessorConfig {
            per_table_chunk_sizes: AHashMap::new(),
            channel_size: 100,
            deprecated_tables: HashSet::new(),
        },
        query_retries: 10,
        query_retry_delay_ms: 1000,
    };

    let processor_config = ProcessorConfig::StakeProcessor(default_processor_config);
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
        diff_test_helper::event_processor::load_data,
        sdk_tests::{
            run_processor_test, setup_test_environment,
            stake_processor_tests::setup_stake_processor_config, validate_json,
            DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
    use aptos_indexer_test_transactions::{
        IMPORTED_MAINNET_TXNS_121508544_STAKE_DISTRIBUTE,
        IMPORTED_MAINNET_TXNS_125600867_STAKE_DELEGATION_POOL,
        IMPORTED_MAINNET_TXNS_126043288_STAKE_DELEGRATION_WITHDRAW,
        IMPORTED_MAINNET_TXNS_139442597_STAKE_UNLOCK,
        IMPORTED_MAINNET_TXNS_139449359_STAKE_REACTIVATE,
        IMPORTED_MAINNET_TXNS_4827964_STAKE_INITIALIZE,
        IMPORTED_MAINNET_TXNS_83883373_STAKE_WITHDRAW,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::stake_processor::StakeProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_genesis_txn() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_121508544_STAKE_DISTRIBUTE,
            121508544,
            Some("stake_distribute_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_new_block_event() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_125600867_STAKE_DELEGATION_POOL,
            125600867,
            Some("stake_delegation_pool_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_empty_txn() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_126043288_STAKE_DELEGRATION_WITHDRAW,
            126043288,
            Some("stake_delegation_withdraw".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_coin_register_fa_metadata() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_139442597_STAKE_UNLOCK,
            139442597,
            Some("stake_unlock_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_fa_metadata() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_139449359_STAKE_REACTIVATE,
            139449359,
            Some("stake_reactivate_test".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_fa_activities() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_4827964_STAKE_INITIALIZE,
            4827964,
            Some("stake_initialize_test".to_string()),
        )
        .await;
    }

    /// Example test case of not using custom name
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn testnet_stake_processor_coin_register() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_83883373_STAKE_WITHDRAW,
            83883373,
            None,
        )
        .await;
    }

    // Helper function to abstract out the single transaction processing
    async fn process_single_mainnet_event_txn(
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
            setup_stake_processor_config(&test_context, txn_version as u64, 1, &db_url).await;

        let stake_processor = StakeProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create StakeProcessor");

        match run_processor_test(
            &mut test_context,
            stake_processor,
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
