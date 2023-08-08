use anyhow::Result;
use aptos_processor_framework::StorageTrait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// An in-memory, transient storage implementation.
#[derive(Debug)]
pub struct MemoryStorage {
    chain_id: Arc<Mutex<Option<u64>>>,
    last_processed_version: Arc<Mutex<Option<u64>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            chain_id: Arc::new(Mutex::new(None)),
            last_processed_version: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl StorageTrait for MemoryStorage {
    async fn read_chain_id(&self) -> Result<Option<u64>> {
        Ok(*self.chain_id.lock().await)
    }

    async fn write_chain_id(&self, chain_id: u64) -> Result<()> {
        *self.chain_id.lock().await = Some(chain_id);
        Ok(())
    }

    async fn read_last_processed_version(&self, _processor_name: &str) -> Result<Option<u64>> {
        Ok(*self.last_processed_version.lock().await)
    }

    async fn write_last_processed_version(&self, version: u64) -> Result<()> {
        *self.last_processed_version.lock().await = Some(version);
        Ok(())
    }
}
