use anyhow::Result;
use aptos_processor_framework::{
    indexer_protos::transaction::v1::Transaction, ProcessingResult, ProcessorTrait,
};
use tracing::debug;

#[derive(Debug)]
pub struct ExampleProcessor {}

/// A processor that just prints the txn version.
#[async_trait::async_trait]
impl ProcessorTrait for ExampleProcessor {
    fn name(&self) -> &'static str {
        "ExampleProcessor"
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult> {
        for transaction in transactions {
            debug!(
                "{}: Processing transaction {}",
                self.name(),
                transaction.version
            );
        }
        Ok((start_version, end_version))
    }
}
