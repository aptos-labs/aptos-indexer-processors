use ahash::AHashMap;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use sdk_processor::{
    config::{
        db_config::{DbConfig, PostgresConfig},
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::{DefaultProcessorConfig, ProcessorConfig},
    },
    processors::objects_processor::ObjectsProcessorConfig,
};
use std::collections::HashSet;

pub fn setup_objects_processor_config(
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

    let objects_processor_config = ObjectsProcessorConfig {
        default_config: default_processor_config,
        query_retries: ObjectsProcessorConfig::default_query_retries(),
        query_retry_delay_ms: ObjectsProcessorConfig::default_query_retry_delay_ms(),
    };

    let processor_config = ProcessorConfig::ObjectsProcessor(objects_processor_config);

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
mod sdk_objects_processor_tests {
    use super::setup_objects_processor_config;
    use crate::{
        diff_test_helper::objects_processor::load_data,
        sdk_tests::{
            get_all_version_from_test_context, get_transaction_version_from_test_context,
            run_processor_test, setup_test_environment, validate_json, DEFAULT_OUTPUT_FOLDER,
        },
    };
    use aptos_indexer_test_transactions::{
        IMPORTED_MAINNET_TXNS_578318306_OBJECTS_WRITE_RESOURCE,
        IMPORTED_MAINNET_TXNS_578366445_TOKEN_V2_BURN_EVENT_V2,
    };
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use sdk_processor::processors::objects_processor::ObjectsProcessor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_objects_write_and_delete_resource() {
        // Need two transactions because the processor performs a lookup on previous transaction's
        // object address when parsing delete resource
        let txns = &[
            IMPORTED_MAINNET_TXNS_578318306_OBJECTS_WRITE_RESOURCE,
            IMPORTED_MAINNET_TXNS_578366445_TOKEN_V2_BURN_EVENT_V2,
        ];
        process_multiple_transactions(
            txns,
            Some("test_objects_write_and_delete_resource".to_string()),
        )
        .await;
    }

    // Helper function to abstract out the transaction processing
    async fn process_multiple_transactions(txns: &[&[u8]], test_case_name: Option<String>) {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());

        let (db, mut test_context) = setup_test_environment(txns).await;

        let starting_version = *get_transaction_version_from_test_context(&test_context)
            .first()
            .unwrap();
        let all_txn_versions = get_all_version_from_test_context(&test_context);

        let db_url = db.get_db_url();
        let (indexer_processor_config, processor_name) = setup_objects_processor_config(
            &test_context,
            starting_version,
            all_txn_versions.len(),
            &db_url,
        );

        let objects_processor = ObjectsProcessor::new(indexer_processor_config)
            .await
            .expect("Failed to create ObjectsProcessor");

        match run_processor_test(
            &mut test_context,
            objects_processor,
            load_data,
            db_url,
            all_txn_versions,
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
                eprintln!(
                    "[ERROR] Failed to run processor for txn version {}: {}",
                    1, e
                );
                panic!("Test failed due to processor error");
            },
        }
    }
}
