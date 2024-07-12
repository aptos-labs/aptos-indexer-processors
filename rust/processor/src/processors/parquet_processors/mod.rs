use std::time::Duration;

pub mod parquet_default_processor;

pub mod parquet_fungible_asset_processor;

pub const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";

pub trait UploadIntervalConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration;
}
