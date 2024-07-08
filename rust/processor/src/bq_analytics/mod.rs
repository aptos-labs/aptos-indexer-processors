pub mod gcs_handler;
pub mod generic_parquet_processor;

use crate::{
    bq_analytics::generic_parquet_processor::{
        GetTimeStamp, HasParquetSchema, HasVersion, NamedTable, ParquetDataGeneric,
        ParquetHandler as GenericParquetHandler,
    },
    gap_detectors::ProcessingResult,
    processors::parquet_processors::ParquetProcessingState,
    worker::PROCESSOR_SERVICE_TYPE,
};
use ahash::AHashMap;
use allocative::Allocative;
use google_cloud_storage::{
    client::{Client as GCSClient, ClientConfig as GcsClientConfig},
    http::Error as StorageError,
};
use kanal::AsyncSender;
use parquet::record::RecordWriter;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result as FormatResult},
    sync::Arc,
    time::Instant,
};
use tokio::{io, time::Duration};
use tracing::{debug, error, info};

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

pub fn create_parquet_handler_loop<ParquetType>(
    new_gap_detector_sender: AsyncSender<ProcessingResult>,
    processor_name: &str,
    bucket_name: String,
    bucket_root: String,
    parquet_handler_response_channel_size: usize,
    max_buffer_size: usize,
    upload_interval: Duration,
) -> AsyncSender<ParquetDataGeneric<ParquetType>>
where
    ParquetType: GetTimeStamp
        + HasVersion
        + HasParquetSchema
        + NamedTable
        + Send
        + Sync
        + 'static
        + Allocative,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    let processor_name = processor_name.to_owned();
    let (parquet_sender, parquet_receiver) = kanal::bounded_async::<ParquetDataGeneric<ParquetType>>(
        parquet_handler_response_channel_size,
    );

    debug!(
        processor_name = processor_name.clone(),
        service_type = PROCESSOR_SERVICE_TYPE,
        "[Parquet Handler] Starting parquet handler loop",
    );

    let mut parquet_manager = GenericParquetHandler::new(
        bucket_name.clone(),
        bucket_root.clone(),
        new_gap_detector_sender.clone(),
        ParquetType::schema(),
        upload_interval,
        max_buffer_size,
    )
    .expect("Failed to create parquet manager");

    tokio::spawn(async move {
        let gcs_config = GcsClientConfig::default()
            .with_auth()
            .await
            .expect("Failed to create GCS client config");
        let gcs_client = Arc::new(GCSClient::new(gcs_config));

        let mut last_upload_time = Instant::now();

        loop {
            let now = Instant::now();
            // Check if the interval has elapsed
            if now.duration_since(last_upload_time) >= upload_interval {
                info!("Time has elamped more since last update.");
                match parquet_manager.upload_buffer(&gcs_client).await {
                    Ok(_) => {
                        info!(
                            processor_name = processor_name.clone(),
                            service_type = PROCESSOR_SERVICE_TYPE,
                            "[Parquet Handler] Successfully uploaded buffer",
                        );
                        last_upload_time = Instant::now();
                    },
                    Err(e) => {
                        error!("Failed to upload buffer: {}", e);
                        panic!("Error processing parquet files: {:?}", e);
                    },
                }
            }

            match parquet_receiver.recv().await {
                Ok(txn_pb_res) => {
                    let result = parquet_manager.handle(&gcs_client, txn_pb_res).await;

                    match result {
                        Ok(ParquetProcessingState::Buffered) => {
                            info!(
                                processor_name = processor_name.clone(),
                                service_type = PROCESSOR_SERVICE_TYPE,
                                "[Parquet Handler] Successfully processed structs to buffer",
                            );
                        },
                        Ok(ParquetProcessingState::Uploaded) => {
                            info!(
                                processor_name = processor_name.clone(),
                                service_type = PROCESSOR_SERVICE_TYPE,
                                "[Parquet Handler] Successfully processed structs to buffer and uploaded",
                            );
                            last_upload_time = Instant::now();
                        },
                        Err(e) => {
                            error!(
                                processor_name = processor_name.clone(),
                                service_type = PROCESSOR_SERVICE_TYPE,
                                "[Parquet Handler] Error processing parquet files: {:?}",
                                e
                            );
                            panic!("Error processing parquet files: {:?}", e);
                        },
                    }
                },
                Err(e) => {
                    error!(
                        processor_name = processor_name.clone(),
                        service_type = PROCESSOR_SERVICE_TYPE,
                        "[Parquet Handler] Error receiving parquet files: {:?}",
                        e
                    );
                },
            }
        }
    });

    parquet_sender
}
