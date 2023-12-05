use anyhow::Result;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

type StartVersion = u64;
type EndVersion = u64;
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct ProcessingResult {
    pub start_version: StartVersion,
    pub end_version: EndVersion,
    pub processing_duration_in_secs: f64,
    pub db_insertion_duration_in_secs: f64,
}

/// Base trait for all processors
#[async_trait]
#[enum_dispatch]
pub trait ProcessorTrait: Send + Sync + Debug {
    fn name(&self) -> &'static str;

    /// Process all transactions including writing to the database
    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        db_chain_id: Option<u8>,
    ) -> Result<ProcessingResult>;
}
