use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::ans_processor::AnsProcessorConfig,
};
use std::collections::HashSet;

pub fn setup_ans_processor_config(
    test_context: &SdkTestContext,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config = test_context.create_transaction_stream_config(); // since this will be always 1, we can remove from the arg list
    let postgres_config = PostgresConfig {
        connection_string: db_url.to_string(),
        db_pool_size: 100,
    };

    let db_config = DbConfig::PostgresConfig(postgres_config);

    let ans_processor_config = AnsProcessorConfig {
        ans_v1_primary_names_table_handle:
            "0x1d5f57aa505a2fa463b7a46341913b65757e3177c46a5e483a29d953627bee62".to_string(),
        ans_v1_name_records_table_handle:
            "0x21a0fd41330f3a0a38173c7c0e4ac59cd51505f0594f64d3d637c12425c3c155".to_string(),
        ans_v2_contract_address:
            "0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c".to_string(),
        default: DefaultProcessorConfig {
            per_table_chunk_sizes: AHashMap::new(),
            channel_size: 100,
            deprecated_tables: HashSet::new(),
        },
    };

    let processor_config = ProcessorConfig::AnsProcessor(ans_processor_config);
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
        diff_test_helper::ans_processor::load_data,
        sdk_tests::{
            ans_processor_tests::setup_ans_processor_config, run_processor_test,
            setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        IMPORTED_MAINNET_TXNS_1056780409_ANS_CURRENT_ANS_PRIMARY_NAME_V2,
        IMPORTED_MAINNET_TXNS_2080538_ANS_LOOKUP_V1, IMPORTED_MAINNET_TXNS_303690531_ANS_LOOKUP_V2,
        IMPORTED_MAINNET_TXNS_438536688_ANS_CURRENT_ANS_LOOKUP_V2,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::ans_processor::AnsProcessor;

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::NameRecord
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::SubdomainExt
     * - Events
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::SetReverseLookupEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_current_ans_primary_name_v2() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_1056780409_ANS_CURRENT_ANS_PRIMARY_NAME_V2,
            Some("test_current_ans_primary_name_v2".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::NameRecord
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::SubdomainExt
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_ans_lookup_v2() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_303690531_ANS_LOOKUP_V2,
            Some("test_ans_lookup_v2".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::RenewNameEvents
     *      - 0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c::v2_1_domains::NameRecord
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mainnet_current_ans_lookup_v2() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_438536688_ANS_CURRENT_ANS_LOOKUP_V2,
            Some("test_current_ans_lookup_v2".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mainnet_ans_lookup_v1() {
        process_single_mainnet_event_txn(
            IMPORTED_MAINNET_TXNS_2080538_ANS_LOOKUP_V1,
            Some("test_mainnet_ans_lookup_v1".to_string()),
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
            setup_ans_processor_config(&test_context, &db_url);

        let ans_processor = AnsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create AnsProcessor");

        match run_processor_test(
            &mut test_context,
            ans_processor,
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
