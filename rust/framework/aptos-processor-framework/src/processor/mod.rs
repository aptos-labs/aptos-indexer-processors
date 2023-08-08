// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use std::fmt::Debug;

type StartVersion = u64;
type EndVersion = u64;

pub type ProcessingResult = (StartVersion, EndVersion);

/// Base trait for all processors.
#[async_trait]
pub trait ProcessorTrait: Debug + Send + Sync + Debug {
    fn name(&self) -> &'static str;

    /// Process all transactions including writing to the database
    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult>;
}
