mod sanity_tests;

use crate::sdk_tests::run_processor_test;
use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
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
    Events(EventsProcessor),
    FungibleAsset(FungibleAssetProcessor),
    Ans(AnsProcessor),
    Default(DefaultProcessor),
    Objects(ObjectsProcessor),
    Stake(StakeProcessor),
    UserTransaction(UserTransactionProcessor),
    TokenV2(TokenV2Processor),
    AccountTransactions(AccountTransactionsProcessor),
}

#[allow(dead_code)]
impl ProcessorWrapper {
    async fn run<F>(
        self,
        test_context: &mut SdkTestContext,
        db_values_fn: F,
        db_url: &str,
        diff_flag: bool,
        output_path: &str,
    ) -> anyhow::Result<HashMap<String, Value>>
    where
        F: Fn(&mut PgConnection, Vec<i64>) -> anyhow::Result<HashMap<String, Value>>
            + Send
            + Sync
            + 'static,
    {
        // Helper function to avoid code duplication
        async fn run_test<P>(
            test_context: &mut SdkTestContext,
            processor: P,
            db_values_fn: impl Fn(&mut PgConnection, Vec<i64>) -> anyhow::Result<HashMap<String, Value>>
                + Send
                + Sync
                + 'static,
            db_url: &str,
            diff_flag: bool,
            output_path: &str,
        ) -> anyhow::Result<HashMap<String, Value>>
        where
            P: Send + Sync + 'static + ProcessorTrait,
        {
            run_processor_test(
                test_context,
                processor,
                db_values_fn,
                db_url.to_string(),
                diff_flag,
                output_path.to_string(),
                None,
            )
            .await
        }

        match self {
            ProcessorWrapper::Events(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::FungibleAsset(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::Ans(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::Default(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::Objects(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::Stake(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::UserTransaction(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::TokenV2(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
            ProcessorWrapper::AccountTransactions(processor) => run_test(test_context, processor, db_values_fn, db_url, diff_flag, output_path).await,
        }
    }
}