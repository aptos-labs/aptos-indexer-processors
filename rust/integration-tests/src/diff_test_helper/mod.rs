use diesel::pg::PgConnection;
use serde_json::Value;

pub mod processors;

pub trait ProcessorTestHelper: Send + Sync {
    fn load_data(&self, conn: &mut PgConnection, txn_version: &str) -> anyhow::Result<Value>;
}
