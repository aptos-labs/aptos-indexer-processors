

use crate::{
    models::default_models::{DataSize}, worker::PROCESSOR_SERVICE_TYPE
};

use ahash::AHashMap;
use parquet::{file::{writer::SerializedFileWriter, properties::WriterProperties}, record::RecordWriter};
use tracing::{error, info, debug};
use google_cloud_storage::{
    client::Client as GCSClient,
    http::{objects::upload::{Media, UploadObjectRequest, UploadType} },
};
use std::path::PathBuf;
use anyhow::{Result, anyhow};
use std::{fmt::Debug, fs::{File, remove_file, rename}, sync::Arc};
use tokio::fs::File as TokioFile;
use hyper::Body;
use tokio::time::{timeout, Duration, sleep};
use chrono::Datelike;
use chrono::Timelike;
use tokio::io::AsyncReadExt; // for read_to_end()
use serde::{Deserialize, Serialize};

const MAX_FILE_SIZE: u64 = 500 * 1024 * 1024; // 500 MB in bytes, maybe reduce this to 300 MB. use different value for struct type
const BUCKET_REGULAR_TRAFFIC: &str = "devnet-airflow-continue"; // add to parquet processor config.
const BUCKET_NAME: &str = "aptos-indexer-data-etl-yuun"; // add to parquet processor config or maybe this could be fixed? add a bucket for each stage of the pipeline. 

#[derive(Debug, Clone)]
pub struct ParquetData {
    pub data: Vec<ParquetStruct>,  
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub transaction_version_to_struct_count: AHashMap<i64, i64>, 
}
struct ParquetManager {
    writer: Option<SerializedFileWriter<File>>,
    buffer: Vec<ParquetStruct>,
    buffer_size_bytes: usize,
    // maybe we should track all trasaction_Version to counter here and then send it to gap detector? 
    transaction_version_to_struct_count: AHashMap<i64, i64>, 
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ParquetProcessingResult {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub parquet_insertion_duration_in_secs: f64,
    pub txn_version_to_struct_count: AHashMap<u64, u64>,
}

#[derive(Debug, Clone)]
pub enum ParquetStruct {
    Transaction(crate::models::default_models::parquet_transactions::Transaction),  
    MoveResource(crate::models::default_models::parquet_move_resources::MoveResource),
    WriteSetChange(crate::models::default_models::parquet_write_set_changes::WriteSetChange),
}

impl ParquetManager {
    pub fn new() -> Self {
        Self {
            writer: None,
            buffer: Vec::new(),
            buffer_size_bytes: 0,
            transaction_version_to_struct_count: AHashMap::new(),
        }
    }

    pub fn update_buffer_size(&mut self, parquet_struct: &ParquetStruct) {
        match parquet_struct {
            ParquetStruct::MoveResource(move_resource) => {
                self.buffer_size_bytes += move_resource.size_of();
            },
            ParquetStruct::Transaction(transaction) => {
                self.buffer_size_bytes += transaction.size_of();
            },
            ParquetStruct::WriteSetChange(write_set_change) => {
                self.buffer_size_bytes += write_set_change.size_of();
            },
        };
        debug!("buffer size after adding struct to buffer : {} bytes.", self.buffer_size_bytes);
    }

    pub async fn write_and_upload(&mut self, changes: ParquetData, gcs_client: &GCSClient, gap_detector_sender: kanal::AsyncSender<ParquetProcessingResult>) -> Result<ParquetProcessingResult> {
    
        let parquet_structs = changes.data.first();
        // update the transaction counter? 
        // from map in the data, we have 
        let _processing_result: Result<ParquetProcessingResult> = match parquet_structs.unwrap().clone() {
            ParquetStruct::MoveResource(_move_resource) => {
                return self.handle_move_resource(gcs_client, &gap_detector_sender, changes.clone()).await;
            },
            ParquetStruct::Transaction(_transaction) => {
                return self.handle_transaction(gcs_client, &gap_detector_sender, changes.clone()).await;
            },
            // ParquetStruct::WriteSetChange(write_set_change) => {
            //     return self.handle_move_resource(&gcs_client, &gap_detector_sender, changes.clone()).await;
            // },
            _ => {
                error!("No size found for the parquet struct");
                panic!("No size found for the parquet struct");
            }
        };
    }

    async fn handle_transaction(
        &mut self,
        gcs_client: &GCSClient,
        gap_detector_sender: &kanal::AsyncSender<ParquetProcessingResult>, 
        changes: ParquetData) -> Result<ParquetProcessingResult>
    {
        let processing_start: std::time::Instant = std::time::Instant::now();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::LZ4) 
            .build();
        let props_arc = Arc::new(props.clone());
        let last_transaction_timestamp = changes.last_transaction_timestamp;
        let parquet_structs: Vec<ParquetStruct> = changes.data.clone();
        let txn_version_to_struct_count = changes.transaction_version_to_struct_count.clone();
        self.transaction_version_to_struct_count.extend(txn_version_to_struct_count.clone());
        let schema = get_schema(parquet_structs.first().unwrap().clone());
        
        let table_name = get_file_path_str_from_struct(&parquet_structs.clone().first().unwrap().clone());
        let file_path = table_name.clone() + ".parquet";
        info!("opening the file at path: {:?}", file_path.clone());
        let mut file: File = File::options().create(true).write(true).open(&file_path)?;
        if self.writer.is_none() {
            self.writer = Some(SerializedFileWriter::new(file.try_clone()?, schema.clone(), props_arc)?);
        }

        // let file_cloned = file.try_clone()?;
        let mut writer: SerializedFileWriter<File> = self.writer.take().unwrap(); // Take out the writer temporarily
        info!("Current buffer size:  {:?}", self.buffer_size_bytes);
        
        for parquet_struct in parquet_structs.clone() {
            self.buffer.push(parquet_struct.clone());
            self.update_buffer_size(&parquet_struct);

            if self.buffer_size_bytes >= MAX_FILE_SIZE.try_into().unwrap() {  
                info!("buffer size reached max size: {}, uploading to GCS", self.buffer_size_bytes);
                
                let new_file_path: PathBuf = PathBuf::from(format!("{}_{}.parquet", table_name.clone(), chrono::Utc::now().timestamp()));
                rename(file_path.clone(), new_file_path.clone())?;
                
                
                let mut row_group_writer = writer.next_row_group()?;
                // let mut struct_buffer = Vec::new();
                let struct_buffer: Vec<_> = self.buffer.iter()
                    .filter_map(|item| {
                        if let ParquetStruct::Transaction(transaction) = item {
                            Some(transaction.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                struct_buffer.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
                row_group_writer.close()?;
                writer.close()?;
  

                drop(file);
            
                
                let _upload_result = upload_parquet_to_gcs(gcs_client, &new_file_path, table_name.clone().as_str()).await?; // replace with the actual table name

                let _processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
                
                let processing_result = build_parquet_processing_result(self.buffer.clone(), self.transaction_version_to_struct_count.clone(), last_transaction_timestamp.clone());
                self.buffer.clear();
                self.buffer_size_bytes = 0;      

                // delete file after the upload
                remove_file(&new_file_path)?;
                
                // create a new file for the next batch
                file = File::options().append(true).create(true).open(&file_path)?;
                let cloned_file = file.try_clone()?;
                writer = SerializedFileWriter::new(cloned_file, schema.clone(), Arc::new(props.clone()))?;

                info!("self file path after upload and reset: {:?}", file_path);  

                info!("[Parquet Handler]Sending result to gap detector...");
                gap_detector_sender
                    .send(processing_result)
                    .await
                    .expect("[Parser] Failed to send versions to gap detector");
            }
        }
        
        let _processing_duration_in_secs: f64 = processing_start.elapsed().as_secs_f64();

        Ok(ParquetProcessingResult {
            start_version : 0, //move_resources.first().unwrap().transaction_version.clone() as u64,
            end_version : 0, // move_resources.last().unwrap().transaction_version.clone() as u64,
            // processing_duration_in_secs,
            last_transaction_timestamp,
            parquet_insertion_duration_in_secs: 0.0,
            txn_version_to_struct_count: AHashMap::new(),
        })
    }

    async fn handle_move_resource(
        &mut self,
        gcs_client: &GCSClient,
        gap_detector_sender: &kanal::AsyncSender<ParquetProcessingResult>, 
        changes: ParquetData) -> Result<ParquetProcessingResult>
    {
        let processing_start: std::time::Instant = std::time::Instant::now();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::LZ4) 
            .build();
        let props_arc = Arc::new(props.clone());
        let last_transaction_timestamp = changes.last_transaction_timestamp;
        let parquet_structs = changes.data.clone();
        let txn_version_to_struct_count = changes.transaction_version_to_struct_count.clone();
        self.transaction_version_to_struct_count.extend(txn_version_to_struct_count.clone());

        let schema = get_schema(parquet_structs.first().unwrap().clone());
        let table_name = get_file_path_str_from_struct(&parquet_structs.clone().first().unwrap().clone());

        let file_path = table_name.clone() + ".parquet";
        info!("opening the file at path: {:?}", file_path.clone());
        let mut file: File = File::options().create(true).write(true).open(&file_path)?;
        if self.writer.is_none() {
            self.writer = Some(SerializedFileWriter::new(file.try_clone()?, schema.clone(), props_arc)?);
        }

        // let file_cloned = file.try_clone()?;
        let mut writer: SerializedFileWriter<File> = self.writer.take().unwrap(); // Take out the writer temporarily
        info!("Current buffer size: {:?}", self.buffer_size_bytes);

        // loop the vector of struct and write to the parquet file 
        for parquet_struct in parquet_structs.clone() {
            self.buffer.push(parquet_struct.clone());
            self.update_buffer_size(&parquet_struct);
    

            if self.buffer_size_bytes >= MAX_FILE_SIZE.try_into().unwrap() {  
                let mut starting_version: Option<u64> = None;
                
                if let Some(ParquetStruct::MoveResource(move_resource)) = self.buffer.first() {
                    starting_version = Some(move_resource.transaction_version as u64);
                }
                let mut ending_version: Option<u64> = None;
                if let Some(ParquetStruct::MoveResource(move_resource)) = self.buffer.last() {
                    ending_version = Some(move_resource.transaction_version  as u64);
                }

                info!("uplaoding parquet file with starting version: {} and ending version: {}", 
                    starting_version.unwrap(),
                    ending_version.unwrap()
                );
                
                let new_file_path: PathBuf = PathBuf::from(format!("{}_{}.parquet", table_name.clone(), chrono::Utc::now().timestamp()));
                rename(file_path.clone(), new_file_path.clone())?;
                
                
                let mut row_group_writer = writer.next_row_group()?;
                let mut struct_buffer = Vec::new();
                
                for (_i, item) in self.buffer.iter().enumerate() {
                    if let ParquetStruct::MoveResource(move_resource) = item {
                        struct_buffer.push(move_resource.clone());
                    }
                }

                struct_buffer.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
                row_group_writer.close()?;
                writer.close()?;
                // self.buffer.clear();

                // self.buffer_size_bytes = 0;        

                drop(file);
            
                let _upload_result = upload_parquet_to_gcs(gcs_client, &new_file_path, table_name.clone().as_str()).await?; // replace with the actual table name

                let _processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

                // let processing_result = ParquetProcessingResult {
                //     start_version: starting_version.unwrap(),
                //     end_version: ending_version.unwrap(),
                //     last_transaction_timestamp: last_transaction_timestamp.clone(),
                //     parquet_insertion_duration_in_secs: None,
                //     txn_version_to_struct_count: AHashMap::new(),
                // };
                let processing_result = build_parquet_processing_result(self.buffer.clone(), self.transaction_version_to_struct_count.clone(), last_transaction_timestamp.clone());
                self.buffer.clear();
                self.buffer_size_bytes = 0;        
// 
                // delete file after the upload
                remove_file(&new_file_path)?;
                
                // create a new file for the next batch
                file = File::options().append(true).create(true).open(&file_path)?;
                let cloned_file = file.try_clone()?;
                writer = SerializedFileWriter::new(cloned_file, schema.clone(), Arc::new(props.clone()))?;

                info!("self file path after upload and reset: {:?}", file_path);  

                info!("[Parquet Handler]Sending result to gap detector...");
                gap_detector_sender
                    .send(processing_result)
                    .await
                    .expect("[Parser] Failed to send versions to gap detector");
            }
        }
        info!("Current file size after added structs to buffer: {:?}", self.buffer_size_bytes);
        let _processing_duration_in_secs: f64 = processing_start.elapsed().as_secs_f64();

        Ok(ParquetProcessingResult {
            start_version : 0, //move_resources.first().unwrap().transaction_version.clone() as u64,
            end_version : 0, // move_resources.last().unwrap().transaction_version.clone() as u64,
            last_transaction_timestamp,
            parquet_insertion_duration_in_secs: 0.0,
            txn_version_to_struct_count: AHashMap::new(),
        })
    }
    
}

pub async fn upload_parquet_to_gcs(
    client: &GCSClient,
    file_path: &PathBuf,
    table_name: &str,
) -> Result<String> {
    let mut file = TokioFile::open(&file_path).await.map_err(|e| anyhow!("Failed to open file for reading: {}", e))?;

    info!("File opened for reading at path: {:?}", file_path);
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.map_err(|e| anyhow!("Failed to read file: {}", e))?;

    if buffer.is_empty() {
        error!("The file is empty and has no data to upload.");
        return Err(anyhow!("No data to upload. The file buffer is empty."));
    }

    info!("Parquet file read successfully, size: {} bytes", buffer.len());
    
    let now = chrono::Utc::now();
    let start_of_month = now.with_day(1).unwrap().with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
    let highwater_s = start_of_month.timestamp_millis();
    let highwater_ms = now.timestamp_millis(); 
    let counter = 0; // THIS NEED TO BE REPLACED OR REIMPLEMENTED WITH AN ACTUAL LOGIC TO ENSURE FILE UNIQUENESS. 
    let object_name: PathBuf = generate_parquet_file_path(BUCKET_REGULAR_TRAFFIC, table_name, highwater_s, highwater_ms, counter);

    let file_name = object_name.to_str().unwrap().to_owned(); 
    info!("Generated GCS object name: {}", file_name);
    let upload_type: UploadType = UploadType::Simple(Media::new(file_name.clone()));

    let upload_request = UploadObjectRequest {
        bucket: BUCKET_NAME.to_owned(), 
        ..Default::default()
    };

    let max_retries = 3; // Maximum number of retries
    let mut retry_count = 0;
    let mut delay = 500; // Initial delay in milliseconds

    loop {
        let data = Body::from(buffer.clone()); 
        let upload_result = timeout(Duration::from_secs(300), client.upload_object(&upload_request, data, &upload_type)).await;
        
        match upload_result {
            Ok(Ok(result)) => {
                info!("File uploaded successfully to GCS: {}", result.name);
                return Ok(result.name);
            },
            Ok(Err(e)) => {
                error!("Failed to upload file to GCS: {}", e);
                if retry_count >= max_retries {
                    return Err(anyhow!("Failed to upload file after retries: {}", e));
                }
            },
            Err(e) => {
                error!("Upload timed out: {}", e);
                if retry_count >= max_retries {
                    return Err(anyhow!("Upload operation timed out after retries"));
                }
            }
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
    let file_path = PathBuf::from(format!("{}/{}/{}/{}_{}.parquet", gcs_bucket_root, table, highwater_s, highwater_ms, counter));
    info!("file_path generated: {:?}", file_path);
    file_path
}

fn get_file_path_str_from_struct(parquet_struct: &ParquetStruct) -> String {
    match parquet_struct {
        ParquetStruct::MoveResource(_) => "move_resources".to_string(),
        ParquetStruct::WriteSetChange(_) => "write_set_changes".to_string(),
        ParquetStruct::Transaction(_) => "transactions".to_string(),
    }
}


fn get_schema(parquet_struct: ParquetStruct) -> Arc<parquet::schema::types::Type> {
    let schema = match Some(parquet_struct) {
        Some(ParquetStruct::MoveResource(struct_model)) => {
            let mut parquet_rust_struct_vec = Vec::new();
            parquet_rust_struct_vec.push(struct_model.clone());
            parquet_rust_struct_vec.as_slice().schema().unwrap().clone()
        },
        Some(ParquetStruct::Transaction(struct_model)) => {
            let mut  parquet_rust_struct_vec = Vec::new();
            parquet_rust_struct_vec.push(struct_model.clone());
            parquet_rust_struct_vec.as_slice().schema().unwrap().clone()    
        },
        Some(ParquetStruct::WriteSetChange(struct_model)) => {
            let mut  parquet_rust_struct_vec = Vec::new();
            parquet_rust_struct_vec.push(struct_model.clone());
            parquet_rust_struct_vec.as_slice().schema().unwrap().clone()
        },
        _ => {
            error!("No schema found for the parquet struct");
            panic!("No schema found for the parquet struct");
        }
    };
    schema
}

pub async fn create_parquet_manager_loop(
    gcs_client: Arc<GCSClient>,
    new_gap_detector_sender: kanal::AsyncSender<ParquetProcessingResult>, 
    parquet_struct_receivers: Vec<kanal::AsyncReceiver<ParquetData>>, 
    processor_name: &str,
) {

    let processor_name = processor_name.to_owned();

    for receiver in parquet_struct_receivers {
        let mut parquet_manager = ParquetManager::new();
        let gcs_client = gcs_client.clone();
        let processor_name = processor_name.clone();
        let new_gap_detector_sender = new_gap_detector_sender.clone();
        // spawan a thread per struct and per struct , we know the type of the struct and we can write it to the parquet file.
        tokio::spawn(async move {            
            loop {
                let txn_pb_res = match receiver.recv().await {
                    Ok(txn_pb_res) => {
                        info!(
                            processor_name = processor_name.clone(),
                            service_type = PROCESSOR_SERVICE_TYPE,
                            "[Parquet Handler] Received transaction data from processor",
                        );
                        txn_pb_res
                    },
                    Err(_e) => {
                        error!(
                            processor_name = processor_name.clone(),
                            service_type = PROCESSOR_SERVICE_TYPE,
                            error = ?_e,
                            "[Parquet Handler] Parquet manager channel has been closed",
                        );
                        ParquetData {                  // maybe add a flag that can tell it's empty
                            data: Vec::new(),
                            last_transaction_timestamp: None,
                            transaction_version_to_struct_count: AHashMap::new(),
                        }
                    },
                };

                let result = parquet_manager.write_and_upload(txn_pb_res, &gcs_client, new_gap_detector_sender.clone()).await;

                match result {
                    Ok(result) => {
                        info!(
                            start_version = result.start_version,
                            end_version = result.end_version,
                            "[Parquet Handler] successfully processed transactions");
                    },
                    Err(e) => {
                        error!(
                            processor_name = processor_name.clone(),
                            service_type = PROCESSOR_SERVICE_TYPE,
                            error = ?e,
                            "[Parquet Handler] Error processing parquet files",
                        );
                        panic!("[Parquet Handler] Error processing parquet files");
                    },
                };

            }
        });
    }
}

pub fn build_parquet_processing_result(
    buffer: Vec<ParquetStruct>,
    txn_version_to_struct_count: AHashMap<i64, i64>,
    last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
) -> ParquetProcessingResult {
    let mut txn_version_to_struct_count_for_gap_detector = AHashMap::new();

    let mut starting_version: Option<u64> = None;
    let mut ending_version: Option<u64> = None;

    for item in buffer.iter() {
        let current_version = match item {
            ParquetStruct::MoveResource(move_resource) => Some(move_resource.transaction_version as u64),
            ParquetStruct::Transaction(transaction) => Some(transaction.version as u64),
            _ => None,
        };

        if let Some(version) = current_version {
            starting_version = match starting_version {
                Some(v) => Some(std::cmp::min(v, version)),
                None => Some(version),
            };
            ending_version = match ending_version {
                Some(v) => Some(std::cmp::max(v, version)),
                None => Some(version),
            };

            if let Some(count) = txn_version_to_struct_count.get(&(version as i64)) {
                txn_version_to_struct_count_for_gap_detector.insert(version, *count as u64);
            }
        }
    }

    
    ParquetProcessingResult {
        start_version: starting_version.unwrap(),
        end_version: ending_version.unwrap(),
        last_transaction_timestamp: last_transaction_timestamp.clone(),
        parquet_insertion_duration_in_secs: 0.0,
        txn_version_to_struct_count: txn_version_to_struct_count_for_gap_detector,
    }
}