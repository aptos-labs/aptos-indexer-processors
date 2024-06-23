pub mod generic_parquet_processor;

use ahash::AHashMap;
use anyhow::{anyhow, Result};
use chrono::{Datelike, Timelike};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::{
        objects::upload::{Media, UploadObjectRequest, UploadType},
        Error as StorageError,
    },
};
use hyper::Body;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result as FormatResult},
    path::PathBuf,
};
use tokio::io::AsyncReadExt; // for read_to_end()
use tokio::{
    fs::File as TokioFile,
    io,
    time::{sleep, timeout, Duration},
};
use tracing::{debug, error, info};

// TODO: make it configurable, write now there is no difference between running parquet for backfill and regular traffic
const BUCKET_REGULAR_TRAFFIC: &str = "devnet-airflow-continue";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ParquetProcessingResult {
    pub start_version: i64,
    pub end_version: i64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub txn_version_to_struct_count: AHashMap<i64, i64>,
}

#[derive(Debug)]
pub enum ParquetProcessorError {
    ParquetError(parquet::errors::ParquetError),
    StorageError(StorageError),
    TimeoutError(tokio::time::error::Elapsed),
    IoError(io::Error),
    Other(String),
}

impl std::error::Error for ParquetProcessorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ParquetProcessorError::ParquetError(ref err) => Some(err),
            ParquetProcessorError::StorageError(ref err) => Some(err),
            ParquetProcessorError::TimeoutError(ref err) => Some(err),
            ParquetProcessorError::IoError(ref err) => Some(err),
            ParquetProcessorError::Other(_) => None,
        }
    }
}

impl Display for ParquetProcessorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match *self {
            ParquetProcessorError::ParquetError(ref err) => write!(f, "Parquet error: {}", err),
            ParquetProcessorError::StorageError(ref err) => write!(f, "Storage error: {}", err),
            ParquetProcessorError::TimeoutError(ref err) => write!(f, "Timeout error: {}", err),
            ParquetProcessorError::IoError(ref err) => write!(f, "IO error: {}", err),
            ParquetProcessorError::Other(ref desc) => write!(f, "Error: {}", desc),
        }
    }
}

impl From<std::io::Error> for ParquetProcessorError {
    fn from(err: std::io::Error) -> Self {
        ParquetProcessorError::IoError(err)
    }
}

impl From<anyhow::Error> for ParquetProcessorError {
    fn from(err: anyhow::Error) -> Self {
        ParquetProcessorError::Other(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for ParquetProcessorError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        ParquetProcessorError::ParquetError(err)
    }
}

pub async fn upload_parquet_to_gcs(
    client: &GCSClient,
    file_path: &PathBuf,
    table_name: &str,
    bucket_name: &str,
) -> Result<(), ParquetProcessorError> {
    let mut file = TokioFile::open(&file_path)
        .await
        .map_err(|e| anyhow!("Failed to open file for reading: {}", e))?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .await
        .map_err(|e| anyhow!("Failed to read file: {}", e))?;

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
    let object_name: PathBuf = generate_parquet_file_path(
        BUCKET_REGULAR_TRAFFIC,
        table_name,
        highwater_s,
        highwater_ms,
        counter,
    );

    let file_name = object_name.to_str().unwrap().to_owned();
    let upload_type: UploadType = UploadType::Simple(Media::new(file_name.clone()));

    let upload_request = UploadObjectRequest {
        bucket: bucket_name.to_string(),
        ..Default::default()
    };

    let max_retries = 3;
    let mut retry_count = 0;
    let mut delay = 500;

    loop {
        let data = Body::from(buffer.clone());
        let upload_result = timeout(
            Duration::from_secs(300),
            client.upload_object(&upload_request, data, &upload_type),
        )
        .await;

        match upload_result {
            Ok(Ok(result)) => {
                info!("File uploaded successfully to GCS: {}", result.name);
                return Ok(());
            },
            Ok(Err(e)) => {
                error!("Failed to upload file to GCS: {}", e);
                if retry_count >= max_retries {
                    return Err(ParquetProcessorError::StorageError(e));
                }
            },
            Err(e) => {
                error!("Upload timed out: {}", e);
                if retry_count >= max_retries {
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
    gcs_bucket_root: &str,
    table: &str,
    highwater_s: i64,
    highwater_ms: i64,
    counter: u32,
) -> PathBuf {
    PathBuf::from(format!(
        "{}/{}/{}/{}_{}.parquet",
        gcs_bucket_root, table, highwater_s, highwater_ms, counter
    ))
}
