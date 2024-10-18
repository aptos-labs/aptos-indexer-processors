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
    txn_version: u64,
    txn_count: usize,
    db_url: &str,
) -> (IndexerProcessorConfig, &'static str) {
    let transaction_stream_config =
        test_context.create_transaction_stream_config(txn_version, txn_count as u64);
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
    use aptos_indexer_test_transactions::ALL_IMPORTED_TESTNET_TXNS;
    use aptos_indexer_testing_framework::{cli_parser::get_test_config, database::TestDatabase};
    use aptos_protos::transaction::v1::Transaction;
    use sdk_processor::processors::fungible_asset_processor::FungibleAssetProcessor;

    // TODO - Add more intentional tests to validate the processor with different scenarios
    #[tokio::test]
    async fn fa_processor_db_output_diff_test() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "imported_testnet_txns");

        let transaction_batches = ALL_IMPORTED_TESTNET_TXNS
            .iter()
            .map(|txn| serde_json::from_slice(txn).unwrap())
            .collect::<Vec<Transaction>>();

        let (db, mut test_context) = setup_test_environment(ALL_IMPORTED_TESTNET_TXNS).await;

        // Step 2: Loop over each transaction and run the test for each
        for txn in transaction_batches.iter() {
            let txn_version = txn.version;

            // Step 3: Run the processor
            let db_url = db.get_db_url();

            let (indexer_processor_config, processor_name) =
                setup_fa_processor_config(&test_context, txn_version, 1, &db_url);

            let fungible_asset_processor = FungibleAssetProcessor::new(indexer_processor_config)
                .await
                .expect("Failed to create FungibleAssetProcessor");

            match run_processor_test(
                &mut test_context,
                fungible_asset_processor,
                load_data,
                db_url,
                vec![txn_version as i64],
                diff_flag,
                output_path.clone(),
                None,
            )
            .await
            {
                Ok(mut db_value) => {
                    let _ = validate_json(
                        &mut db_value,
                        txn_version,
                        processor_name,
                        output_path.clone(),
                    );
                },
                Err(e) => {
                    eprintln!(
                        "[ERROR] Failed to run processor for txn version {}: {}",
                        txn_version, e
                    );
                    panic!("Test failed due to processor error");
                },
            }
        }
    }
}
