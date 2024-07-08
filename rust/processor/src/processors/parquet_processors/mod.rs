use std::time::Duration;

pub mod parquet_default_processor;

pub trait UploadIntervalConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration;
}

#[derive(Debug)]
pub enum ParquetProcessingState {
    Buffered,
    Uploaded,
}
