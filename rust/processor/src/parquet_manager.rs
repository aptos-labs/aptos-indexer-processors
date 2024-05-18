

use crate::{
    gap_detector, models::default_models::parquet_move_resources::MoveResource, processors::{parquet_default_processor::process_transactions, ProcessingResult, Processor, ProcessorTrait}, worker::PROCESSOR_SERVICE_TYPE
};
use crate::models::default_models::parquet_move_resources::DataSize;
use parquet::{file::{writer::SerializedFileWriter, properties::WriterProperties}, record::RecordWriter};
use tracing::{error, info};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::{objects::upload::{Media, UploadObjectRequest, UploadType} },
};
use std::path::PathBuf;
use anyhow::{Result, anyhow};
use std::{fmt::Debug, fmt::{Display, Formatter}, fs::{File, remove_file, rename}, sync::Arc};
use tokio::fs::File as TokioFile;
use hyper::Body;
use tokio::time::{timeout, Duration};
use chrono::Datelike;
use chrono::Timelike;
use tokio::io::AsyncReadExt; // for read_to_end()
use serde::{Deserialize, Serialize};
use anyhow::bail;

const MAX_FILE_SIZE: u64 = 2000 * 1024 * 1024; // 200 MB
const BUCKET_REGULAR_TRAFFIC: &str = "devnet-airflow-continue";
const TABLE: &str = "move_resources";
const BUCKET_NAME: &str = "aptos-indexer-data-etl-yuun";

#[derive(Debug, Clone)]
pub struct ParquetData {
    pub data: Vec<MoveResource>,  
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}


struct ParquetManager {
    writer: Option<SerializedFileWriter<File>>,
    file_path: PathBuf,
    buffer: Vec<crate::models::default_models::parquet_move_resources::MoveResource>,
    buffer_size_bytes: usize,
}


#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ParquetProcessingResult {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub processing_duration_in_secs: f64,
    pub parquet_insertion_duration_in_secs: Option<f64>,
}

impl ParquetManager {
    pub fn new() -> Self {
        Self {
            writer: None,
            file_path: PathBuf::from("move_resource.parquet"),
            buffer: Vec::new(),
            buffer_size_bytes: 0,
        }
    }

    pub async fn write_and_upload(&mut self, changes: ParquetData, gcs_client: &GCSClient, gap_detector_sender: &kanal::AsyncSender<ProcessingResult>) -> Result<ParquetProcessingResult> {
        let processing_start: std::time::Instant = std::time::Instant::now();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::LZ4)
            .build();
        let props_arc = Arc::new(props.clone());

        let move_resources: Vec<crate::models::default_models::parquet_move_resources::MoveResource> = changes.data;
        let schema = move_resources.as_slice().schema().unwrap().clone(); 
        let file_path = self.file_path.clone();
        let last_transaction_timestamp = changes.last_transaction_timestamp;

        info!("File path: {:?}", file_path);
        let mut file: File = File::options().create(true).write(true).open(&file_path)?;
        if self.writer.is_none() {
            self.writer = Some(SerializedFileWriter::new(file.try_clone()?, schema.clone(), props_arc)?);
        }

        let file_cloned = file.try_clone()?;
        
        let mut writer: SerializedFileWriter<File> = self.writer.take().unwrap(); // Take out the writer temporarily
        
        info!("Current file size: {:?}", self.buffer_size_bytes);
        for move_resource in move_resources.clone() {
            self.buffer.push(move_resource.clone());
            self.buffer_size_bytes += self.buffer.last().unwrap().size_of();
        }

        // when channel is empty, we still need to upload what we have in the buffer. 
        if self.buffer_size_bytes >= MAX_FILE_SIZE.try_into().unwrap() {  
            info!("buffer size reached max size: {}, uploading to GCS", self.buffer_size_bytes);
            
            let starting_version = self.buffer.first().unwrap().transaction_version;
            let ending_version = self.buffer.last().unwrap().transaction_version;
            info!("uplaoding parquet file with starting version: {} and ending version: {}", starting_version.clone(), ending_version.clone());

            let new_file_path: PathBuf = PathBuf::from(format!("move_resources{}.parquet", chrono::Utc::now().timestamp()));
            rename(self.file_path.clone(), new_file_path.clone())?;
            
            
            let mut row_group_writer = writer.next_row_group()?;
            self.buffer.as_slice().write_to_row_group(&mut row_group_writer).unwrap();

            row_group_writer.close()?;
            writer.close()?;

            self.buffer.clear();
            self.buffer_size_bytes = 0;        

            drop(file);
        
            let _upload_result = self.upload_parquet_to_gcs(&new_file_path, TABLE, gcs_client).await?;

            let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

            let processing_result = ProcessingResult {
                start_version: starting_version.clone() as u64,
                end_version: ending_version.clone() as u64,
                processing_duration_in_secs,
                last_transaction_timestamp: last_transaction_timestamp.clone(),
                db_insertion_duration_in_secs: 0.0,
                parquet_insertion_duration_in_secs: None,
            };

            // delete file after the upload
            remove_file(&new_file_path)?;
            
            // create a new file for the next batch
            file = File::options().append(true).create(true).open(&self.file_path)?;
            let cloned_file = file.try_clone()?;
            writer = SerializedFileWriter::new(cloned_file, schema.clone(), Arc::new(props.clone()))?;

            info!("self file path after upload and reset: {:?}", self.file_path);  

            info!("[Parquet Handler]Sending result to gap detector...");
            gap_detector_sender
                .send(processing_result)
                .await
                .expect("[Parser] Failed to send versions to gap detector");
        }

        let processing_duration_in_secs: f64 = processing_start.elapsed().as_secs_f64();
        

        Ok(ParquetProcessingResult {
            start_version : 0, //move_resources.first().unwrap().transaction_version.clone() as u64,
            end_version : 0, // move_resources.last().unwrap().transaction_version.clone() as u64,
            processing_duration_in_secs,
            last_transaction_timestamp,
            parquet_insertion_duration_in_secs: None,
        })
    }

    pub async fn upload_parquet_to_gcs(
        &self,
        file_path: &PathBuf,
        table_name: &str,
        client: &GCSClient,
    ) -> Result<String> {
        
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
    new_gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
    parquet_manager_receiver: kanal::AsyncReceiver<ParquetData>,
    processor: Processor,
    gcs_client: &GCSClient,
) {
    let processor_name: &str = processor.name();
    let mut parquet_manager = ParquetManager::new();

    loop {
        let txn_pb_res = match tokio::time::timeout(Duration::from_secs(5), parquet_manager_receiver.recv()).await {
            Ok(Ok(txn_pb_res)) => {
                info!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    "[Parquet Handler] Received transaction data from processor",
                );
                txn_pb_res
            },
            Ok(Err(_e)) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?_e,
                    "[Parquet Handler] Parquet manager channel has been closed",
                );
                ParquetData {                  // maybe add a flag that can tell it's empty
                    data: Vec::new(),
                    last_transaction_timestamp: None,
                }
            },
            Err(_e) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?_e,
                    "[Parquet Handler] Parquet manager channel timed out due to empty channel",
                );
                ParquetData {
                    data: Vec::new(),
                    last_transaction_timestamp: None,
                }
            },
        };

        let result = parquet_manager.write_and_upload(txn_pb_res, &gcs_client, &new_gap_detector_sender).await;

        let processing_result = match result {
            Ok(result) => {
                info!(
                    start_version = result.start_version,
                    end_version = result.end_version,
                    "[Parquet Handler] successfully processed transactions");
            },
            Err(e) => {
                error!(
                    processor_name = processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?e,
                    "[Parquet Handler] Error processing parquet files",
                );
                panic!("[Parquet Handler] Error processing parquet files");
            },
        };

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
