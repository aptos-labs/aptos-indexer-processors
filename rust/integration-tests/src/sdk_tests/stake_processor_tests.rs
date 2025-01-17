use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::stake_processor::StakeProcessorConfig,
};
use std::collections::HashSet;

pub fn setup_stake_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config(); // since this will be always 1, we can remove from the arg list
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
        // Avoid doing long lookups in tests
        query_retries: 1,
        query_retry_delay_ms: 100,
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
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_118489_PROPOSAL_VOTE,
        IMPORTED_MAINNET_TXNS_121508544_STAKE_DISTRIBUTE,
        IMPORTED_MAINNET_TXNS_139449359_STAKE_REACTIVATE,
        IMPORTED_MAINNET_TXNS_1830706009_STAKER_GOVERNANCE_RECORD,
        IMPORTED_MAINNET_TXNS_1831971037_STAKE_DELEGATION_POOL,
        IMPORTED_MAINNET_TXNS_4827964_STAKE_INITIALIZE,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::stake_processor::StakeProcessor;

    /**
     * - 0x1::delegation_pool::DelegationPool
     * - 0x1::delegation_pool::UnlockStakeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_pool_delegation_txn() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_1831971037_STAKE_DELEGATION_POOL,
            Some("stake_pool_del_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::delegation_pool::GovernanceRecords
     * - 0x1::delegation_pool::AddStakeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_gov_record_txn() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_1830706009_STAKER_GOVERNANCE_RECORD,
            Some("stake_gov_record_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::stake::WithdrawStakeEvent
     * - 0x1::staking_contract::DistributeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_processor_genesis_txn() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_121508544_STAKE_DISTRIBUTE,
            Some("stake_distribute_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::stake::ReactivateStakeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_processor_fa_metadata() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_139449359_STAKE_REACTIVATE,
            Some("stake_reactivate_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::stake::AddStakeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_processor_fa_activities() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_4827964_STAKE_INITIALIZE,
            Some("stake_initialize_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::voting::VoteEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_proposal_vote() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_118489_PROPOSAL_VOTE,
            Some("mainnet_proposal_vote_test".to_string()),
        )
        .await;
    }

    /**
     * - 0x1::delegation_pool::DistributeCommissionEvent
     * - 0x1::delegation_pool::DistributeCommission
     * - 0x1::stake::UnlockStakeEvent
     * - 0x1::delegation_pool::UnlockStakeEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_stake_delegation_pool() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_1831971037_STAKE_DELEGATION_POOL,
            Some("mainnet_stake_delegation_pool".to_string()),
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
            setup_stake_processor_config(&test_context, &db_url);

        let stake_processor = StakeProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create StakeProcessor");

        match run_processor_test(
            &mut test_context,
            stake_processor,
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
