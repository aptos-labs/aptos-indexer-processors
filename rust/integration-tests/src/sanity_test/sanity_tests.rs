#[allow(clippy::needless_return)]
#[cfg(test)]
mod tests {
    use crate::{
        diff_test_helper::{
            account_transaction_processor::load_data as load_acc_txn_data,
            ans_processor::load_data as load_ans_data,
            default_processor::load_data as load_default_data,
            event_processor::load_data as load_event_data,
            fungible_asset_processor::load_data as load_fungible_asset_data,
            objects_processor::load_data as load_object_data,
            stake_processor::load_data as load_stake_data,
            token_v2_processor::load_data as load_token_v2_data,
            user_transaction_processor::load_data as load_ut_data,
        },
        sanity_test::ProcessorWrapper,
        sdk_tests::{
            account_transaction_processor_tests::setup_acc_txn_processor_config,
            ans_processor_tests::setup_ans_processor_config,
            default_processor_tests::setup_default_processor_config,
            events_processor_tests::setup_events_processor_config,
            fungible_asset_processor_tests::setup_fa_processor_config,
            objects_processor_tests::setup_objects_processor_config, setup_test_environment,
            stake_processor_tests::setup_stake_processor_config,
            token_v2_processor_tests::setup_token_v2_processor_config,
            user_transaction_processor_tests::setup_user_txn_processor_config,
        },
    };
    use aptos_indexer_test_transactions::json_transactions::generated_transactions::{
        ALL_IMPORTED_MAINNET_TXNS, ALL_IMPORTED_TESTNET_TXNS, ALL_SCRIPTED_TRANSACTIONS,
    };
    use aptos_indexer_testing_framework::{
        cli_parser::get_test_config, database::TestDatabase, sdk_test_context::SdkTestContext,
    };
    use diesel::pg::PgConnection;
    use sdk_processor::processors::{
        account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
        default_processor::DefaultProcessor, events_processor::EventsProcessor,
        fungible_asset_processor::FungibleAssetProcessor, objects_processor::ObjectsProcessor,
        stake_processor::StakeProcessor, token_v2_processor::TokenV2Processor,
        user_transaction_processor::UserTransactionProcessor,
    };
    use serde_json::Value;
    use std::collections::HashMap;

    const DEFAULT_OUTPUT_FOLDER: &str = "expected_db_output_files";

    #[tokio::test]
    async fn test_all_testnet_txns_for_all_processors() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/imported_testnet_txns");

        let (db, test_context) = setup_test_environment(ALL_IMPORTED_TESTNET_TXNS).await;
        let db_url = db.get_db_url();

        run_processors_with_test_context(test_context, db_url, diff_flag, output_path).await;
    }

    #[tokio::test]
    async fn test_all_mainnet_txns_for_all_processors() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/imported_testnet_txns");

        let (db, test_context) = setup_test_environment(ALL_IMPORTED_MAINNET_TXNS).await;
        let db_url = db.get_db_url();

        run_processors_with_test_context(test_context, db_url, diff_flag, output_path).await;
    }

    #[tokio::test]
    async fn test_all_scripted_txns_for_all_processors() {
        let (diff_flag, custom_output_path) = get_test_config();
        let output_path = custom_output_path
            .unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string() + "/imported_testnet_txns");

        let (db, test_context) = setup_test_environment(ALL_SCRIPTED_TRANSACTIONS).await;
        let db_url = db.get_db_url();

        run_processors_with_test_context(test_context, db_url, diff_flag, output_path).await;
    }

    async fn run_processors_with_test_context(
        mut test_context: SdkTestContext,
        db_url: String,
        diff_flag: bool,
        output_path: String,
    ) {
        let processors_map: HashMap<String, ProcessorWrapper> = [
            (
                "events_processor".to_string(),
                ProcessorWrapper::EventsProcessor(
                    EventsProcessor::new(setup_events_processor_config(&test_context, &db_url).0)
                        .await
                        .expect("Failed to create EventsProcessor"),
                ),
            ),
            (
                "fungible_asset_processor".to_string(),
                ProcessorWrapper::FungibleAssetProcessor(
                    FungibleAssetProcessor::new(
                        setup_fa_processor_config(&test_context, &db_url).0,
                    )
                    .await
                    .expect("Failed to create FungibleAssetProcessor"),
                ),
            ),
            (
                "ans_processor".to_string(),
                ProcessorWrapper::AnsProcessor(
                    AnsProcessor::new(setup_ans_processor_config(&test_context, &db_url).0)
                        .await
                        .expect("Failed to create AnsProcessor"),
                ),
            ),
            (
                "default_processor".to_string(),
                ProcessorWrapper::DefaultProcessor(
                    DefaultProcessor::new(setup_default_processor_config(&test_context, &db_url).0)
                        .await
                        .expect("Failed to create DefaultProcessor"),
                ),
            ),
            (
                "objects_processor".to_string(),
                ProcessorWrapper::ObjectsProcessor(
                    ObjectsProcessor::new(setup_objects_processor_config(&test_context, &db_url).0)
                        .await
                        .expect("Failed to create ObjectsProcessor"),
                ),
            ),
            (
                "stake_processor".to_string(),
                ProcessorWrapper::StakeProcessor(
                    StakeProcessor::new(setup_stake_processor_config(&test_context, &db_url).0)
                        .await
                        .expect("Failed to create StakeProcessor"),
                ),
            ),
            (
                "user_transactions_processor".to_string(),
                ProcessorWrapper::UserTransactionProcessor(
                    UserTransactionProcessor::new(
                        setup_user_txn_processor_config(&test_context, &db_url).0,
                    )
                    .await
                    .expect("Failed to create UserTransactionProcessor"),
                ),
            ),
            (
                "token_v2_processor".to_string(),
                ProcessorWrapper::TokenV2Processor(
                    TokenV2Processor::new(
                        setup_token_v2_processor_config(&test_context, &db_url).0,
                    )
                    .await
                    .expect("Failed to create TokenV2Processor"),
                ),
            ),
            (
                "account_transactions_processor".to_string(),
                ProcessorWrapper::AccountTransactionsProcessor(
                    AccountTransactionsProcessor::new(
                        setup_acc_txn_processor_config(&test_context, &db_url).0,
                    )
                    .await
                    .expect("Failed to create AccountTransactionProcessor"),
                ),
            ),
        ]
        .into_iter()
        .collect();

        // Loop through all processors and run tests
        for (processor_name, processor) in processors_map {
            let db_values_fn = get_db_values_fn_for_processor(&processor_name);

            match processor
                .run(
                    &mut test_context,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                )
                .await
            {
                Ok(_) => {
                    println!("Processor ran successfully for {}", processor_name);
                },
                Err(e) => {
                    panic!(
                        "Error running processor test for {} with error: {}",
                        processor_name, e
                    );
                },
            }
        }
    }

    fn get_db_values_fn_for_processor(
        processor_name: &str,
    ) -> fn(&mut PgConnection, Vec<i64>) -> anyhow::Result<HashMap<String, Value>> {
        match processor_name {
            "events_processor" => load_event_data,
            "fungible_asset_processor" => load_fungible_asset_data,
            "ans_processor" => load_ans_data,
            "default_processor" => load_default_data,
            "objects_processor" => load_object_data,
            "stake_processor" => load_stake_data,
            "user_transactions_processor" => load_ut_data,
            "token_v2_processor" => load_token_v2_data,
            "account_transactions_processor" => load_acc_txn_data,
            _ => panic!("Unknown processor: {}", processor_name),
        }
    }
}
