use crate::{
    bq_analytics::generic_parquet_processor::{
        HasParquetSchema, HasVersion, NamedTable, ParquetDataGeneric,
        ParquetHandler as GenericParquetHandler,
    },
    gap_detectors::ProcessingResult,
    worker::PROCESSOR_SERVICE_TYPE,
};
use allocative::Allocative;
use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
use kanal::AsyncSender;
use parquet::record::RecordWriter;
use std::sync::Arc;
use tracing::{debug, error, info};

pub fn create_parquet_handler_loop<ParquetType>(
    new_gap_detector_sender: AsyncSender<ProcessingResult>,
    processor_name: &str,
    bucket_name: String,
    parquet_handler_response_channel_size: usize,
    max_buffer_size: usize,
) -> AsyncSender<ParquetDataGeneric<ParquetType>>
where
    ParquetType: NamedTable + HasVersion + HasParquetSchema + Send + Sync + 'static + Allocative,
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
        new_gap_detector_sender.clone(),
        ParquetType::schema(),
    )
    .expect("Failed to create parquet manager");

    tokio::spawn(async move {
        let gcs_config = GcsClientConfig::default()
            .with_auth()
            .await
            .expect("Failed to create GCS client config");
        let gcs_client = Arc::new(GCSClient::new(gcs_config));

        loop {
            let txn_pb_res = parquet_receiver.recv().await.unwrap(); // handle error properly

            let result = parquet_manager
                .handle(&gcs_client, txn_pb_res, max_buffer_size)
                .await;
            match result {
                Ok(_) => {
                    info!(
                        processor_name = processor_name.clone(),
                        service_type = PROCESSOR_SERVICE_TYPE,
                        "[Parquet Handler] Successfully processed parquet files",
                    );
                },
                Err(e) => {
                    error!(
                        processor_name = processor_name.clone(),
                        service_type = PROCESSOR_SERVICE_TYPE,
                        "[Parquet Handler] Error processing parquet files: {:?}",
                        e
                    );
                },
            }
        }
    });

    parquet_sender
}
