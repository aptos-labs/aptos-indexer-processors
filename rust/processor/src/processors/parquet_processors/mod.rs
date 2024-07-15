use std::time::Duration;

pub mod parquet_ans_processor;
pub mod parquet_default_processor;
pub mod parquet_events_processor;
pub mod parquet_fungible_asset_processor;
pub mod parquet_token_v2_processor;
pub mod parquet_transaction_metadata_processor;

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";

pub trait ParquetProcessorTrait {
    fn parquet_upload_interval_in_secs(&self) -> Duration;

    fn set_google_credentials(&self, credentials: Option<String>) {
        if let Some(credentials) = credentials {
            std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS, credentials);
        }
    }
}
