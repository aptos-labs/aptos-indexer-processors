use diesel::pg::PgConnection;
use serde_json::Value;

pub mod processors;

pub trait ProcessorTestHelper: Send + Sync {
    // TODO: This only supports one table per processor. We need to support multiple tables.
    #[allow(dead_code)]
    fn load_data(&self, conn: &mut PgConnection, txn_version: &str) -> anyhow::Result<Value>;
}
