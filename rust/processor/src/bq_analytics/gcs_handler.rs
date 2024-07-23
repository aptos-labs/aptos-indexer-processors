use crate::{bq_analytics::ParquetProcessorError, utils::counters::PARQUET_BUFFER_SIZE};
use anyhow::{Context, Result};
use chrono::{Datelike, Timelike};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
};
use hyper::{body::HttpBody, Body};
use std::path::{Path, PathBuf};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, error, info};

const MAX_RETRIES: usize = 3;
const INITIAL_DELAY_MS: u64 = 500;
const TIMEOUT_SECONDS: u64 = 300;
pub async fn upload_parquet_to_gcs(
    client: &GCSClient,
    buffer: Vec<u8>,
    table_name: &str,
    bucket_name: &str,
    bucket_root: &Path,
    processor_name: String,
) -> Result<(), ParquetProcessorError> {
    if buffer.is_empty() {
        error!("The file is empty and has no data to upload.",);
        return Err(ParquetProcessorError::Other(
            "The file is empty and has no data to upload.".to_string(),
        ));
    }

    let now = chrono::Utc::now();
    let start_of_month = now
        .with_day(1)
        .unwrap()
        .with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let highwater_s = start_of_month.timestamp_millis();
    let highwater_ms = now.timestamp_millis();
    let counter = 0; // THIS NEED TO BE REPLACED OR REIMPLEMENTED WITH AN ACTUAL LOGIC TO ENSURE FILE UNIQUENESS.
    let object_name: PathBuf =
        generate_parquet_file_path(bucket_root, table_name, highwater_s, highwater_ms, counter);

    let file_name = object_name.to_str().unwrap().to_owned();
    let upload_type: UploadType = UploadType::Simple(Media::new(file_name.clone()));

    let upload_request = UploadObjectRequest {
        bucket: bucket_name.to_string(),
        ..Default::default()
    };

    let mut retry_count = 0;
    let mut delay = INITIAL_DELAY_MS;

    loop {
        let data = Body::from(buffer.clone());
        let size_hint = data.size_hint();
        let size = size_hint.exact().context("Failed to get size hint")?;
        PARQUET_BUFFER_SIZE
            .with_label_values(&[&processor_name, table_name])
            .set(size as i64);

        let upload_result = timeout(
            Duration::from_secs(TIMEOUT_SECONDS),
            client.upload_object(&upload_request, data, &upload_type),
        )
        .await;

        match upload_result {
            Ok(Ok(result)) => {
                info!(
                    table_name = table_name,
                    file_name = result.name,
                    "File uploaded successfully to GCS",
                );
                return Ok(());
            },
            Ok(Err(e)) => {
                error!("Failed to upload file to GCS: {}", e);
                if retry_count >= MAX_RETRIES {
                    return Err(ParquetProcessorError::StorageError(e));
                }
            },
            Err(e) => {
                error!("Upload timed out: {}", e);
                if retry_count >= MAX_RETRIES {
                    return Err(ParquetProcessorError::TimeoutError(e));
                }
            },
        }

        retry_count += 1;
        sleep(Duration::from_millis(delay)).await;
        delay *= 2;
        debug!("Retrying upload operation. Retry count: {}", retry_count);
    }
}

fn generate_parquet_file_path(
    gcs_bucket_root: &Path,
    table: &str,
    highwater_s: i64,
    highwater_ms: i64,
    counter: u32,
) -> PathBuf {
    gcs_bucket_root.join(format!(
        "{}/{}/{}_{}.parquet",
        table, highwater_s, highwater_ms, counter
    ))
}
