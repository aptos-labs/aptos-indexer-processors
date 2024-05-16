

use crate::{
    processors::{ProcessingResult, Processor, ProcessorTrait},
    utils::counters::PROCESSOR_DATA_GAP_COUNT,
    worker::PROCESSOR_SERVICE_TYPE,
    models::default_models::write_set_changes::WriteSetChangeModel,
    processors::{parquet_default_processor::process_transactions}
};
use crate::models::default_models::write_set_changes::DataSize;
use aptos_protos::transaction::v1::{Transaction, WriteSetChange};
use parquet::{file::{writer::SerializedFileWriter, properties::WriterProperties}, schema::types::Type, record::RecordWriter};
use tracing::{error, info};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::{objects::upload::{Media, UploadObjectRequest, UploadType}, Error as StorageError},
};
use std::path::PathBuf;
use anyhow::{Result as AnyhowResult, anyhow};
use std::{fmt::Debug, fmt::{Display, Formatter, Result}, fs::{File, remove_file, rename}, sync::Arc};
use tokio::fs::File as TokioFile;
use hyper::Body;
use tokio::time::{timeout, Duration};
use chrono::Datelike;
use chrono::Timelike;
use tokio::io::AsyncReadExt; // for read_to_end()

const MAX_FILE_SIZE: u64 = 1000 * 1024 * 1024; // 200 MB
const BUCKET_REGULAR_TRAFFIC: &str = "devnet-airflow-continue";
const BATCH_SIZE: usize = 5000;
const TABLE: &str = "write_set_changes";
const BUCKET_NAME: &str = "aptos-indexer-data-etl-yuun";

#[derive(Debug, Clone)]
pub struct ParquetData {
    pub data: Vec<WriteSetChangeModel>,  
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

struct ParquetManager {
    // writers: Arc<AHashMap<String, Option<SerializedFileWriter<File>>>>,
    // file_sizes: Arc<AHashMap<String, u64>>,
    writer: Option<SerializedFileWriter<File>>,
    file_path: PathBuf,
    buffer: Vec<crate::models::default_models::write_set_changes::WriteSetChange>,
    buffer_size_bytes: usize,
}

impl ParquetManager {
    pub fn new() -> Self {
        Self {
            writer: None,
            file_path: PathBuf::from("write_set_changes.parquet"),
            buffer: Vec::new(),
            buffer_size_bytes: 0,
        }
    }

    // TODO: add a logic to handle file recreate in case we want to reset the file.
    // but for now, we will just keep appending to the file until it reaches the max size.
    pub async fn write_and_upload(&mut self, changes: ParquetData, gcs_client: &GCSClient) -> anyhow::Result<ProcessingResult> {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::LZ4)
            .build();
        let props_arc = Arc::new(props.clone());

        let wscs: Vec<crate::models::default_models::write_set_changes::WriteSetChange> = changes.data;
        let schema = wscs.as_slice().schema().unwrap().clone(); 
        let file_path = self.file_path.clone();
        let last_transaction_timestamp = changes.last_transaction_timestamp;

        info!("File path: {:?}", file_path);
        let mut file: File = File::options().create(true).write(true).open(&file_path)?;
        if self.writer.is_none() {
            self.writer = Some(SerializedFileWriter::new(file.try_clone()?, schema.clone(), props_arc)?);
        }

        let file_cloned = file.try_clone()?;
        
        let mut writer: SerializedFileWriter<File> = self.writer.take().unwrap(); // Take out the writer temporarily
        // let mut rows: Vec<crate::models::default_models::write_set_changes::WriteSetChange> = Vec::with_capacity(BATCH_SIZE); 
        info!("Current file size: {:?}", self.buffer_size_bytes);
        for wsc in wscs {
            self.buffer.push(wsc.clone());
            self.buffer_size_bytes += self.buffer.last().unwrap().size_of();
        }

        if self.buffer_size_bytes >= MAX_FILE_SIZE.try_into().unwrap() {
            info!("buffer size {} bytes", self.buffer_size_bytes);
            info!("File size reached max size, uploading to GCS...");    
            let new_file_path: PathBuf = PathBuf::from(format!("write_set_changes_{}.parquet", chrono::Utc::now().timestamp()));
            info!("Renaming file to: {:?}", new_file_path);
            rename(self.file_path.clone(), new_file_path.clone())?;

            
            // upload to gcs and create a new file 
            let mut row_group_writer = writer.next_row_group()?;
            self.buffer.as_slice().write_to_row_group(&mut row_group_writer).unwrap();

            row_group_writer.close()?;
            writer.close()?;

            self.buffer.clear();
            self.buffer_size_bytes = 0;        

            drop(file);
            let _upload_result = self.upload_parquet_to_gcs(&new_file_path, TABLE, gcs_client).await?;

            // if upload is successful, we also need to send the result to the gap detector
            // let gap_detector_sender = gap_detector_senders.get(&processor_name).unwrap();
            // gap_detector_sender.send(result).await.unwrap();
            // delete file or clean up so that we can create a new file


            // delete file
            remove_file(&new_file_path)?;
            // create a new file
            file = File::options().append(true).create(true).open(&self.file_path)?;
            let cloned_file = file.try_clone()?;
            writer = SerializedFileWriter::new(cloned_file, schema.clone(), Arc::new(props.clone()))?;

            info!("self file path after upload and reset: {:?}", self.file_path);  
        }

        let processing_start: std::time::Instant = std::time::Instant::now();
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_duration_in_secs: f64 = 0.0;
        let start_version = 0;  // maybe store this in a state
        let end_version = 0;  // maybe store this in a state
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            last_transaction_timestamp,
            db_insertion_duration_in_secs,
        })
    }

    pub async fn upload_parquet_to_gcs(
        &self,
        file_path: &PathBuf,
        table_name: &str,
        client: &GCSClient,
    ) -> AnyhowResult<String> {
        
        // Re-open the file for reading
        let mut file = TokioFile::open(&file_path).await.map_err(|e| anyhow!("Failed to open file for reading: {}", e))?;
    
        info!("File opened for reading at path: {:?}", file_path);
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await.map_err(|e| anyhow!("Failed to read file: {}", e))?;
    
        if buffer.is_empty() {
            error!("The file is empty and has no data to upload.");
            return Err(anyhow!("No data to upload. The file buffer is empty."));
        }

        info!("File read successfully, size: {} bytes", buffer.len());
        let now = chrono::Utc::now();
        let start_of_month = now.with_day(1).unwrap().with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
        let highwater_s = start_of_month.timestamp_millis();
        let highwater_ms = now.timestamp_millis();  // milliseconds
        let counter = 0; // THIS NEED TO BE REPLACED OR REIMPLEMENTED WITH AN ACTUAL LOGIC TO ENSURE FILE UNIQUENESS. 
        let object_name: PathBuf = generate_parquet_file_path(BUCKET_REGULAR_TRAFFIC, table_name, highwater_s, highwater_ms, counter);
    
        // let table_path = PathBuf::from(format!("{}/{}/{}/", bucket_name, gcs_bucket_root, table));
        let file_name = object_name.to_str().unwrap().to_owned(); 
        info!("Generated GCS object name: {}", file_name);
        
        let upload_type: UploadType = UploadType::Simple(Media::new(file_name.clone()));
        let data = Body::from(buffer);
    
        let upload_request = UploadObjectRequest {
            bucket: BUCKET_NAME.to_owned(),
            ..Default::default()
        };
    
        info!("uploading file to GCS...");
        let upload_result = timeout(Duration::from_secs(300), client.upload_object(&upload_request, data, &upload_type)).await;
        
        
        match upload_result {
            Ok(Ok(result)) => {
                info!("File uploaded successfully to GCS: {}", result.name);
                Ok(result.name)
            },
            Ok(Err(e)) => {
                error!("Failed to upload file to GCS: {}", e);
                Err(anyhow!("Failed to upload file: {}", e))
            },
            Err(e) => {
                error!("Upload timed out: {}", e);
                Err(anyhow!("Upload operation timed out"))
            }
        }
    }
}

pub async fn create_parquet_manager_loop(
    gap_detector_senders: kanal::AsyncSender<ProcessingResult>,
    parquet_manager_receiver: kanal::AsyncReceiver<ParquetData>,
    gap_detection_batch_size: u64,
    processor: Processor,
    gcs_client: &GCSClient,
) {
    let processor_name: &str = processor.name();

    let mut parquet_manager = ParquetManager::new();

    loop {
        let txn_pb_res = match parquet_manager_receiver.recv().await {
            Ok(txn_pb_res) => {
                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    "Received transaction data from processor",
                );
                txn_pb_res
            },
            Err(_e) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?_e,
                    "[Parser] Parquet manager channel has been closed",
                );
                panic!(
                    "{}: Parquet manager channel has been closed",
                    processor_name
                );
            }
        };

        let result = parquet_manager.write_and_upload(txn_pb_res, &gcs_client).await.unwrap();
    }
}

fn generate_parquet_file_path(
    gcs_bucket_root: &str,
    table: &str,
    highwater_s: i64,
    highwater_ms: i64,
    counter: u32,
) -> PathBuf {
    let file_path = PathBuf::from(format!("{}/{}/{}/{}_{}.parquet", gcs_bucket_root, table, highwater_s, highwater_ms, counter));
    info!("file_path generated: {:?}", file_path);
    file_path
}
