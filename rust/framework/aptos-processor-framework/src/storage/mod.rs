// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

mod config;

use anyhow::Result;
pub use config::CommonStorageConfig;
use std::fmt::Debug;

#[async_trait::async_trait]
pub trait StorageTrait: 'static + Debug + Send + Sync {
    /// todo
    async fn read_chain_id(&self) -> Result<Option<u8>>;

    /// todo
    async fn write_chain_id(&self, chain_id: u8) -> Result<()>;

    /// todo
    async fn read_last_processed_version(&self, processor_name: &str) -> Result<Option<u64>>;

    /// Store last processed version from database. We can assume that all previously processed
    /// versions are successful because any gap would cause the processor to panic
    async fn write_last_processed_version(&self, processor_name: &str, version: u64) -> Result<()>;
}
