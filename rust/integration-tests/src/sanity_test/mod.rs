mod sanity_tests;

use crate::sdk_tests::run_processor_test;
use aptos_indexer_testing_framework::sdk_test_context::SdkTestContext;
use diesel::PgConnection;
use sdk_processor::processors::{
    account_transactions_processor::AccountTransactionsProcessor, ans_processor::AnsProcessor,
    default_processor::DefaultProcessor, events_processor::EventsProcessor,
    fungible_asset_processor::FungibleAssetProcessor, objects_processor::ObjectsProcessor,
    stake_processor::StakeProcessor, token_v2_processor::TokenV2Processor,
    user_transaction_processor::UserTransactionProcessor,
};
use serde_json::Value;
use std::collections::HashMap;

/// Wrapper for the different processors to run the tests
#[allow(dead_code)]
pub enum ProcessorWrapper {
    EventsProcessor(EventsProcessor),
    FungibleAssetProcessor(FungibleAssetProcessor),
    AnsProcessor(AnsProcessor),
    DefaultProcessor(DefaultProcessor),
    ObjectsProcessor(ObjectsProcessor),
    StakeProcessor(StakeProcessor),
    UserTransactionProcessor(UserTransactionProcessor),
    TokenV2Processor(TokenV2Processor),
    AccountTransactionsProcessor(AccountTransactionsProcessor),
}

#[allow(dead_code)]
impl ProcessorWrapper {
    async fn run<F>(
        self,
        test_context: &mut SdkTestContext,
        db_values_fn: F,
        db_url: String,
        diff_flag: bool,
        output_path: String,
    ) -> anyhow::Result<HashMap<String, Value>>
    where
        F: Fn(&mut PgConnection, Vec<i64>) -> anyhow::Result<HashMap<String, Value>>
            + Send
            + Sync
            + 'static,
    {
        match self {
            ProcessorWrapper::EventsProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::FungibleAssetProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::AnsProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::DefaultProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::ObjectsProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::StakeProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::UserTransactionProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::TokenV2Processor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
            ProcessorWrapper::AccountTransactionsProcessor(processor) => {
                run_processor_test(
                    test_context,
                    processor,
                    db_values_fn,
                    db_url.clone(),
                    diff_flag,
                    output_path.clone(),
                    None,
                )
                .await
            },
        }
    }
}
