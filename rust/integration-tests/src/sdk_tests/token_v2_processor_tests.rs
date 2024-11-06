use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::token_v2_processor::TokenV2ProcessorConfig,
};
use std::collections::HashSet;

pub fn setup_token_v2_processor_config(
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
    let token_v2_processor_config = TokenV2ProcessorConfig {
        default_config: default_processor_config,
        query_retries: TokenV2ProcessorConfig::default_query_retries(),
        query_retry_delay_ms: TokenV2ProcessorConfig::default_query_retry_delay_ms(),
    };

    let processor_config = ProcessorConfig::TokenV2Processor(token_v2_processor_config);

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
mod sdk_token_v2_processor_tests {
    use super::setup_token_v2_processor_config;
    use crate::{
        diff_test_helper::token_v2_processor::load_data,
        sdk_tests::{
            get_all_version_from_test_context, get_transaction_version_from_test_context,
            run_processor_test, setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::{
        IMPORTED_MAINNET_TXNS_1058723093_TOKEN_V1_MINT_WITHDRAW_DEPOSIT_EVENTS,
        IMPORTED_MAINNET_TXNS_1080786089_TOKEN_V2_BURN_EVENT_V1,
        IMPORTED_MAINNET_TXNS_11648867_TOKEN_V1_BURN_EVENT,
        IMPORTED_MAINNET_TXNS_141135867_TOKEN_V1_OFFER,
        IMPORTED_MAINNET_TXNS_178179220_TOKEN_V1_MUTATE_EVENT,
        IMPORTED_MAINNET_TXNS_325355235_TOKEN_V2_UNLIMITED_SUPPLY_MINT,
        IMPORTED_MAINNET_TXNS_453498957_TOKEN_V2_MINT_AND_TRANSFER_EVENT_V1,
        IMPORTED_MAINNET_TXNS_537250181_TOKEN_V2_FIXED_SUPPLY_MINT,
        IMPORTED_MAINNET_TXNS_578366445_TOKEN_V2_BURN_EVENT_V2,
        IMPORTED_MAINNET_TXNS_84023785_TOKEN_V2_CLAIM_OFFER,
        IMPORTED_MAINNET_TXNS_967255533_TOKEN_V2_MUTATION_EVENT,
        IMPORTED_MAINNET_TXNS_97963136_TOKEN_V2_CANCEL_OFFER,
        IMPORTED_MAINNET_TXNS_999930475_TOKEN_V2_CONCURRENT_MINT,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::token_v2_processor::TokenV2Processor;

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::aptos_token::AptosCollection
    *      - 0x4::collection::Collection
    *      - 0x4::collection::ConcurrentSupply
    *      - 0x4::aptos_token::AptosToken
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token

    * - Events
    *      - 0x4::collection::Mint
    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_concurrent_aptos_mint() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_999930475_TOKEN_V2_CONCURRENT_MINT,
            Some("test_token_v2_concurrent_aptos_mint".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::collection::Collection
    *      - 0x4::collection::UnlimitedSupply
    * - Events
    *      - 0x4::collection::BurnEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_burn_event_v1() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_1080786089_TOKEN_V2_BURN_EVENT_V1,
            Some("test_token_v2_burn_event_v1".to_string()),
        )
        .await;
    }

    /**
     * This test includes processing for the following:
     * - Resources
     *      - 0x4::collection::UnlimitedSupply
     *      - 0x4::collection::Collection
     *      - 0x4::token::Token
     * - Events
     *      - 0x4::collection::MintEvent
     *      - 0x1::object::TransferEvent
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_unlimited_supply() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_325355235_TOKEN_V2_UNLIMITED_SUPPLY_MINT,
            Some("test_token_v2_unlimited_supply".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::aptos_token::AptosCollection
    *      - 0x4::collection::Collection
    *      - 0x4::collection::FixedSupply
    *      - 0x4::aptos_token::AptosToken
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token

    * - Events
     *      - 0x4::collection::MintEvent
     *      - 0x1::object::TransferEvent
    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_mint_and_transfer_event_v1() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_453498957_TOKEN_V2_MINT_AND_TRANSFER_EVENT_V1,
            Some("test_token_v2_mint_and_transfer_event_v1".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::aptos_token::AptosCollection
    *      - 0x4::collection::Collection
    *      - 0x4::collection::FixedSupply
    *      - 0x4::aptos_token::AptosToken
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token

    * - Events
    *      - 0x4::collection::MintEvent
    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_fixed_supply() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_537250181_TOKEN_V2_FIXED_SUPPLY_MINT,
            Some("test_token_v2_fixed_supply".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::aptos_token::AptosCollection
    *      - 0x4::collection::Collection
    *      - 0x4::collection::ConcurrentSupply
    * - Events
    *      - 0x4::collection::Burn

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_burn_event_v2() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_578366445_TOKEN_V2_BURN_EVENT_V2,
            Some("test_token_v2_burn_event_v2".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v2_mutation_event() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_967255533_TOKEN_V2_MUTATION_EVENT,
            Some("test_token_v2_mutation_event".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_events() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_1058723093_TOKEN_V1_MINT_WITHDRAW_DEPOSIT_EVENTS,
            Some("test_token_v1_events".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_burn_event() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_11648867_TOKEN_V1_BURN_EVENT,
            Some("test_token_v1_burn_event".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_offer() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_141135867_TOKEN_V1_OFFER,
            Some("test_token_v1_offer".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_mutate_event() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_178179220_TOKEN_V1_MUTATE_EVENT,
            Some("test_token_v1_mutate_event".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_claim_offer() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_84023785_TOKEN_V2_CLAIM_OFFER,
            Some("test_token_v1_claim_offer".to_string()),
        )
        .await;
    }

    /**
    * This test includes processing for the following:
    * - Resources
    *      - 0x4::property_map::PropertyMap
    *      - 0x4::token::Token
    *      - 0x4::token::TokenIdentifiers
    * - Events
    *      - 0x4::token::MutationEvent

    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_token_v1_cancel_offer() {
        process_single_transaction(
            IMPORTED_MAINNET_TXNS_97963136_TOKEN_V2_CANCEL_OFFER,
            Some("test_token_v1_cancel_offer".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_single_transaction(txn: &[u8], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(&[txn]).await;
        let txn_versions = get_all_version_from_test_context(&test_context);
        let starting_version = *txn_versions.first().unwrap();

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) = setup_token_v2_processor_config(
            &test_context,
            starting_version as u64,
            txn_versions.len(),
            &db_url,
        );

        let token_v2_processor = TokenV2Processor::new(indexer_processor_config)
            .await
            .expect("Failed to create TokenV2Processor");

        match run_processor_test(
            &mut test_context,
            token_v2_processor,
            load_data,
            db_url,
            txn_versions.clone(),
            diff_flag,
            output_path.clone(),
            test_case_name.clone(),
        )
        .await
        {
            Ok(mut db_value) => {
                let _ = validate_json(
                    &mut db_value,
                    starting_version as u64,
                    processor_name,
                    output_path.clone(),
                    test_case_name,
                );
            },
            Err(e) => {
                panic!(
                    "Test failed on transactions {:?} due to processor error: {}",
                    txn_versions, e
                );
            },
        }
    }
}
