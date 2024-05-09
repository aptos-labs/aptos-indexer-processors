// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::{
        block_metadata_transactions::{BlockMetadataTransaction, BlockMetadataTransactionModel},
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        new_transactions::TransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    utils::database::{PgDbPool},
    // schemas::default_schemas::new_transactions::TRANSACTION_SCHEMA,
};
use ahash::AHashMap;

use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;

use std::{fmt::Debug, fmt::{Display, Formatter, Result}, fs::{File, remove_file, rename}, sync::Arc};
use tracing::{error, info};

use tokio::io::{self, AsyncReadExt};



use parquet::{file::{properties::WriterProperties}};


use google_cloud_storage::{
    client::Client,
    http::{objects::upload::{Media, UploadObjectRequest, UploadType}, Error as StorageError},
};
use serde::{Deserialize, Serialize};
use anyhow::{Result as AnyhowResult, anyhow};

use hyper::Body;
use std::path::PathBuf;
use tokio::fs::File as TokioFile;
use tokio::time::{timeout, Duration};


use parquet::record::RecordWriter; // requires deriving a reordWriter for the struct

use parquet::file::reader::{FileReader};


use chrono::{TimeZone};
use arrow::error::ArrowError;

use parquet::file::writer::{SerializedFileWriter};
use chrono::Datelike;
use chrono::Timelike;
use parquet::schema::types::Type as ParquetType;

const BATCH_SIZE: usize = 5000; // define BATCH_SIZE
const MAX_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024; // 200MB
const BUCKET_REGULAR_TRAFFIC: &str = "devnet-airflow-continue";

const TABLE: &str = "transactions";


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetProcessorConfig {
    pub bucket: String,
    pub google_application_credentials: Option<String>,
}

pub struct ParquetProcessor {
    connection_pool: PgDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    config: ParquetProcessorConfig,
}

#[derive(Debug)]
// #[derive(Debug, Error)]
pub enum ParquetProcessorError {
    // #[error("ArrowError: {0}")]
    ArrowError(ArrowError),
    // #[error("ParquetError: {0}")]
    ParquetError(parquet::errors::ParquetError),
    // #[error("StorageError: {0}")]
    StorageError(StorageError),
    // #[error("IoError: {0}")]
    IoError(io::Error),
    //     #[error("Other error: {0}")]
    Other(String),
}

impl std::error::Error for ParquetProcessorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ParquetProcessorError::ArrowError(ref err) => Some(err),
            ParquetProcessorError::ParquetError(ref err) => Some(err),
            ParquetProcessorError::StorageError(ref err) => Some(err),
            ParquetProcessorError::IoError(ref err) => Some(err),
            ParquetProcessorError::Other(_) => None,
        }
    }
}



impl Display for ParquetProcessorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match *self {
            ParquetProcessorError::ArrowError(ref err) => write!(f, "Arrow error: {}", err),
            ParquetProcessorError::ParquetError(ref err) => write!(f, "Parquet error: {}", err),
            ParquetProcessorError::StorageError(ref err) => write!(f, "Storage error: {}", err),
            ParquetProcessorError::IoError(ref err) => write!(f, "IO error: {}", err),
            ParquetProcessorError::Other(ref desc) => write!(f, "Error: {}", desc),
        }
    }
}

// Implement From trait for converting from std::io::Error
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

// Implement From trait for converting from std::io::Error
impl From<parquet::errors::ParquetError> for ParquetProcessorError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        ParquetProcessorError::ParquetError(err)
    }
}

impl ParquetProcessor {
    pub fn new(connection_pool: PgDbPool, per_table_chunk_sizes: AHashMap<String, usize>, config: ParquetProcessorConfig) -> Self {

        tracing::info!("init ParquetProcessor");

        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }

        Self {
            connection_pool,
            per_table_chunk_sizes,
            config
        }
    }
}

impl Debug for ParquetProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "ParquetProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>, 
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
        client: &Client,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (
            txns,
            block_metadata_transactions,
            write_set_changes,
            (move_modules, move_resources, table_items, current_table_items, table_metadata),
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let _parquet_conversion_start = std::time::Instant::now();
        let db_insertion_duration_in_secs = 0.0;

        // Start the Parquet file handling and GCS upload
        let _upload_result = convert_to_parquet_and_upload_to_gcs(
            client,
            "transactions",
            &self.config.bucket,
            start_version,
            end_version,
            &txns,
            &block_metadata_transactions,
            &write_set_changes,
            (
                &move_modules,
                &move_resources,
                &table_items,
                &current_table_items,
                &table_metadata,
            ),
        ).await?;

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            last_transaction_timestamp,
            db_insertion_duration_in_secs,
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

pub async fn upload_parquet_to_gcs(
    file_path: &PathBuf,
    table_name: &str,
    bucket_name: &str,
    client: &Client,
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
        bucket: bucket_name.to_owned(),
        ..Default::default()
    };

    info!("uploading file to GCS...");
    let upload_result = timeout(Duration::from_secs(120), client.upload_object(&upload_request, data, &upload_type)).await;
        // .map_err(|e| anyhow!("Failed to upload file: {}", e))?;
    
    
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

async fn convert_to_parquet_and_upload_to_gcs(
    client: &Client,
    _name: &'static str,
    bucket_name: &str,
    _start_version: u64,
    _end_version: u64,
    txns: &Vec<crate::models::default_models::new_transactions::Transaction>,
    _block_metadata_transactions: &[BlockMetadataTransactionModel],
    _wscs: &[WriteSetChangeModel],
    (_move_modules, _move_resources, _table_items, _current_table_items, _table_metadata): (
        &[MoveModule],
        &[MoveResource],
        &[TableItem],
        &[CurrentTableItem],
        &[TableMetadata],
    ),
) -> AnyhowResult<(), ParquetProcessorError> {
    
     let schema = txns.as_slice().schema().unwrap().clone();
     let props = WriterProperties::builder()
             .set_compression(parquet::basic::Compression::LZ4)
             .build();        
 
     let local_parquet_file_path = PathBuf::from("transactions.parquet");
     info!("creating file...");
     let mut file: File = File::options().append(true).create(true).open(&local_parquet_file_path)?;
     info!("file created...");

     info!("Processing transactions...");
     // let mut writer = create_parquet_writer(&file, &schema, &props)?;
     let props_arc = Arc::new(props.clone());
     
     let mut writer = SerializedFileWriter::new(file.try_clone()?, schema.clone(), props_arc)?;

     let mut rows = Vec::with_capacity(BATCH_SIZE); 
     let _db_insertion_duration_in_secs = 0.0;
     let current_version = txns.first().unwrap().version;
     info!("current file size    : {:?}", file.metadata()?.len());
     for txn in txns {
         // when we reach the max file size, we upload and create a new process and continue processing
         
         if file.metadata()?.len() >= 200 * 1024 * 1024 {
             let new_file_path: PathBuf = PathBuf::from(format!("transactions_{}.parquet", current_version));
             rename(local_parquet_file_path.clone(), new_file_path.clone())?;
             info!("File size reached max size, uploading to GCS...");
             // upload to gcs and create a new file 
             let mut row_group_writer = writer.next_row_group()?;
             rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
             row_group_writer.close().unwrap();
             rows.clear();
             
             drop(writer);
             drop(file);
             let _upload_result = upload_parquet_to_gcs(&new_file_path, TABLE, bucket_name, client).await?;

             // delete file
             remove_file(&new_file_path)?;
             // create a new file
             file = File::options().append(true).create(true).open(&local_parquet_file_path)?;
             let cloned_file = file.try_clone()?;
             writer = SerializedFileWriter::new(cloned_file, schema.clone(), Arc::new(props.clone()))?
         }
         rows.push(txn.clone());   
         // latest_version = txn.version;
         // start_version += 1;
     }

     let mut row_group_writer = writer.next_row_group()?;
     rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
     row_group_writer.close().unwrap();
     rows.clear();


    Ok(())     

     // In places where you handle errors manually, you can construct `ProcessingError::Other`

}

fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<crate::models::default_models::new_transactions::Transaction>,
    Vec<BlockMetadataTransaction>,
    Vec<WriteSetChangeModel>,
    (
        Vec<MoveModule>,
        Vec<MoveResource>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    ),
) {
    let (txns, block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(&transactions);
    let mut block_metadata_transactions = vec![];
    for block_metadata_txn in block_metadata_txns {
        block_metadata_transactions.push(block_metadata_txn.clone());
    }
    let mut move_modules = vec![];
    let mut move_resources = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata = AHashMap::new();
    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
            WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
            WriteSetChangeDetail::Table(item, current_item, metadata) => {
                table_items.push(item.clone());
                current_table_items.insert(
                    (
                        current_item.table_handle.clone(),
                        current_item.key_hash.clone(),
                    ),
                    current_item.clone(),
                );
                if let Some(meta) = metadata {
                    table_metadata.insert(meta.handle.clone(), meta.clone());
                }
            },
        }
    }

    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<CurrentTableItem>>();
    let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

    (
        txns,
        block_metadata_transactions,
        write_set_changes,
        (
            move_modules,
            move_resources,
            table_items,
            current_table_items,
            table_metadata,
        ),
    )
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

fn create_parquet_writer(
    file: &File,
    parquet_schema: &Arc<ParquetType>,
    props: &WriterProperties
) -> AnyhowResult<SerializedFileWriter<File>, parquet::errors::ParquetError> {
    let file_clone = file.try_clone()?;
    let props_arc = Arc::new(props.clone());

    SerializedFileWriter::new(file_clone, parquet_schema.clone(), props_arc)
}