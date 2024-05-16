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
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, PgDbPool},
    // schemas::default_schemas::new_transactions::TRANSACTION_SCHEMA,
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::{fmt::Debug, fs::File, sync::Arc};
use tracing::{error, info};

use arrow::{datatypes::{DataType, Field, Schema}};
use arrow::array::{Int64Array, StringArray, BooleanArray, Float64Array, ArrayRef, ListArray, StringBuilder};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use arrow::record_batch::RecordBatch;
use bigdecimal::{BigDecimal, ToPrimitive};
use google_cloud_storage::{
    client::Client,
    http::{buckets::list::ListBucketsRequest, objects::upload::{Media, UploadObjectRequest, UploadType}},
};
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use tokio::{io::AsyncReadExt, join};
use hyper::Body;
use std::path::PathBuf;
use tokio::fs::File as TokioFile;

use parquet::record::RecordWriter; // requires deriving a reordWriter for the struct
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};

use parquet::schema::parser::parse_message_type;
use chrono::{DateTime, TimeZone, Utc};
use arrow::error::ArrowError;
use std::io::Cursor;
use parquet::file::writer::{SerializedFileWriter};
use chrono::Datelike;
use chrono::Timelike;

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

        // we can modify this to only process the transactions that we need 
        let (
            txns,
            // block_metadata_transactions,
            // write_set_changes,
            // (move_modules, move_resources, table_items, current_table_items, table_metadata),
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

        // transaction protobuf -> arrow record batch -> buffer -> upload to gcs

        
        let parquet_conversion_start = std::time::Instant::now();

        let schema = txns.as_slice().schema().unwrap().clone();
        let props = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::LZ4)
                .build();        
        
        // Create an in-memory buffer
        let mut buffer = Cursor::new(Vec::new());

        // get the latest local parquet file 
        // we need to know the name for the latest file for each table/struct. strucType could be the name of the file.
        // with local file
        // check size < 200 MB
        // yes -> we append until it reaches 200 MB,
        // no -> upload to gcs and create a new file, renaming has to be done _> when we upload we just use the timestamp
        // we can use the timestamp as the file name

        let local_parquet_file = PathBuf::from("transactions.parquet");
        let file = fs::File::open(&local_parquet_file)?;

        if (file.metadata()?.len() > 200 * 1024 * 1024) {
            // upload to gcs
            // create a new file
        } else {
            // append to the file
            
        }

        let reader = SerializedFileReader::new(file)?;




        let file_path = PathBuf::from(format!("{}/{}/{}/{}_{}.parquet", gcs_bucket_root, table, highwater_s, highwater_ms, counter));

        let path = PathBuf::from("transactions.parquet");
        let file = fs::File::create(&path)?;

        
        
        // Create a ParquetRecordWriter
        let mut writer = SerializedFileWriter::new(buffer, schema, Arc::new(props))?;

        const BATCH_SIZE: usize = 5000; // define BATCH_SIZE
        const MAX_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024; // 200MB
        let mut rows = Vec::with_capacity(BATCH_SIZE); 
        info!("txns len: {}", txns.len());

        for txn in txns {
            rows.push(txn); //
            
            if rows.len() == BATCH_SIZE {
                let mut row_group_writer = writer.next_row_group()?;
                rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
                row_group_writer.close().unwrap();
                rows.clear();
            }
        }
        

        // if rows.len() != BATCH_SIZE {
        //     let mut row_group_writer = writer.next_row_group()?;
        //     // println!("len = {} {} {} {}", rows.len(), rows[1].a_int, rows[1].b_int, rows[1].a_string);
        //     rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
        //     row_group_writer.close().unwrap();
        //     rows.clear();
        // }
            
        // Write remaining rows if any
        if !rows.is_empty() {
            let mut row_group_writer = writer.next_row_group()?;
            rows.as_slice().write_to_row_group(&mut row_group_writer).unwrap();
            row_group_writer.close().unwrap();
        }

        let buffer = writer.into_inner();
        // writer.close()?;

        let data_buffer = buffer?.into_inner();
        
        let parquet_conversion_duration_in_secs = parquet_conversion_start.elapsed().as_secs_f64();

        // GCS Uploading logic 
        let gcs_upload_start = std::time::Instant::now();
        tracing::info!("upload to gcs...");

        let upload_result = upload_parquet_to_gcs(data_buffer, &self.config.bucket, client).await?;
        
        let gcs_upload_duration_in_secs = gcs_upload_start.elapsed().as_secs_f64();
    
        let db_insertion_duration_in_secs = 0.0;

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

fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<crate::models::default_models::new_transactions::Transaction>,
    // Vec<BlockMetadataTransaction>,
    // Vec<WriteSetChangeModel>,
    // (
    //     Vec<MoveModule>,
    //     Vec<MoveResource>,
    //     Vec<TableItem>,
    //     Vec<CurrentTableItem>,
    //     Vec<TableMetadata>,
    // ),
) {
    let (txns, block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(&transactions);


    let mut block_metadata_transactions = vec![];
    for block_metadata_txn in block_metadata_txns {
        block_metadata_transactions.push(block_metadata_txn.clone());
    }
    // let mut move_modules = vec![];
    // let mut move_resources = vec![];
    // let mut table_items = vec![];
    // let mut current_table_items = AHashMap::new();
    // let mut table_metadata = AHashMap::new();
    // for detail in wsc_details {
    //     match detail {
    //         WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
    //         WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
    //         WriteSetChangeDetail::Table(item, current_item, metadata) => {
    //             table_items.push(item.clone());
    //             current_table_items.insert(
    //                 (
    //                     current_item.table_handle.clone(),
    //                     current_item.key_hash.clone(),
    //                 ),
    //                 current_item.clone(),
    //             );
    //             if let Some(meta) = metadata {
    //                 table_metadata.insert(meta.handle.clone(), meta.clone());
    //             }
    //         },
    //     }
    // }

    // // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    // let mut current_table_items = current_table_items
    //     .into_values()
    //     .collect::<Vec<CurrentTableItem>>();
    // let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // // Sort by PK
    // current_table_items
    //     .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    // table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

    (
        txns,
        // block_metadata_transactions,
        // write_set_changes,
        // (
        //     move_modules,
        //     move_resources,
        //     table_items,
        //     current_table_items,
        //     table_metadata,
        // ),
    )
}


async fn create_and_upload_parquet_file(
    transactions: &[TransactionModel],
    schema: Arc<Schema>,
    file_path: PathBuf,
    bucket_name: &str,
    client: &Client,
) -> Result<String> {

    // Here, include the logic to:
    // 1. Convert transactions into a RecordBatch


    // 2. Write the RecordBatch to a Parquet file
    // 3. Upload the Parquet file to GCS
    // (This will include most of the logic currently within `process_transactions` related to Parquet processing)
    // Note: The `schema` parameter allows flexibility for schema definition from outside this function.

    // For simplicity, the example below skips directly to the return statement
    // You'll implement the detailed logic as per your existing code
    Ok("parquet_file_path_or_upload_identifier".to_string())
}

pub async fn upload_parquet_to_gcs(
    // file_path: &PathBuf,
    buffer: Vec<u8>,
    bucket_name: &str,
    client: &Client,
) -> Result<String> {
    // Re-open the file for reading
    // let mut file = TokioFile::open(&file_path).await.map_err(|e| anyhow!("Failed to open file for reading: {}", e))?;

    // Read the file into a Vec<u8>
    // let mut buffer: Vec<u8> = Vec::new();
    // file.read_to_end(&mut buffer).await.map_err(|e| anyhow!("Failed to read file: {}", e))?;

    // Convert the buffer into a Body for the upload
    let data = Body::from(buffer);

    
    let bucket_name = bucket_name.to_owned();
    let gcs_bucket_root = "devnet-airflow-continue";
    let table: &str = "transactions";
    let now = chrono::Utc::now();
    let start_of_month = now.with_day(1).unwrap().with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
    info!("start_of_month ------- : {:?}", start_of_month);

    let highwater_s = start_of_month.timestamp_millis();

    let highwater_ms = now.timestamp_millis();  // milliseconds
    let counter = 0;
    let file_path = generate_parquet_file_path(gcs_bucket_root, table, highwater_s, highwater_ms, counter);
    let table_path = PathBuf::from(format!("{}/{}/{}/", bucket_name, gcs_bucket_root, table));
    // Convert the OsStr to a &str and ensure it's owned by storing it in a variable
    let file_name = file_path.to_str().unwrap().to_owned();
    // Specify the upload type as simple
    let upload_type: UploadType = UploadType::Simple(Media::new(file_name));
    info!("table_path generated: {:?}", table_path);
    // Create the UploadObjectRequest
    let upload_request = UploadObjectRequest {
        bucket: bucket_name.to_owned(),
        // bucket: table_path.to_str().unwrap().to_owned(),
        ..Default::default()
    };


    // Perform the upload, providing all three expected arguments
    let result = client.upload_object(&upload_request, data, &upload_type).await
        .map_err(|e| anyhow!("Failed to upload file: {}", e))?;

    // The result object contains information about the uploaded file; you might want to return something specific from it
    Ok(result.name)
}

// fn generate_file_name(base_path: &str, table_name: &str) -> String {
//     let now: DateTime<Utc> = Utc::now();
//     format!("{}/{}_{}.parquet", base_path, table_name, now.format("%Y%m%d%H%M%S"))
//     PathBuf::from({$} + ".parquet");
// }

    /**
     *  
        Generate the file path for storing Parquet files in Google Cloud Storage.
        
        Parameters:
        - gcs_bucket: The name of the GCS bucket.
        - gcs_bucket_root: The root folder within the bucket.
        - table: The table name associated with the data.
        - highwater_s: The timestamp rounded to the nearest second.
        - highwater_ms: The more precise timestamp in milliseconds.
        - counter: A counter to ensure file uniqueness within the same timestamp.

        Returns:
        - The fully formatted GCS file path.
     */

fn generate_parquet_file_path(
    gcs_bucket_root: &str,
    table: &str,
    highwater_s: i64,
    highwater_ms: i64,
    counter: u32,
) -> PathBuf {
    let file_path = PathBuf::from(format!("{}/{}/{}/{}_{}.parquet", gcs_bucket_root, table, highwater_s, highwater_ms, counter));
    // let file_path: PathBuf = PathBuf::from(format!("{}_{}.parquet", highwater_ms, counter));
    info!("file_path generated: {:?}", file_path);


    file_path
    // format!(
    //     "gs://{}/{}/{}/{}/{}_{}.parquet",
    //     gcs_bucket, gcs_bucket_root, table, highwater_s, highwater_ms, counter
    // )
}


fn _write_memory_to_gcs3(

) -> () {
}

const BATCH_SIZE: usize = 5000;
const MAX_FILE_SIZE_BYTES: usize = 200 * 1024 * 1024;

let local_parquet_file_path = PathBuf::from("transactions.parquet");

// Use proper error handling or propagate the error
let mut file = File::options().append(true).open(&local_parquet_file_path)?;

let mut rows = Vec::with_capacity(BATCH_SIZE); 
if file.metadata()?.len() > MAX_FILE_SIZE_BYTES {
    // Upload to GCS and handle errors appropriately
    upload_parquet_to_gcs(&local_parquet_file_path, &self.config.bucket, client).await?;
    // delete current one and 
    // Create a new file, consider using timestamp for naming
    file = File::create(&local_parquet_file_path)?;
} else {
    // Create a writer for the existing file
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
    for txn in txns.iter() {
        rows.push(txn);
        if rows.len() == BATCH_SIZE || file.metadata()?.len() >= MAX_FILE_SIZE_BYTES {
            write_to_parquet(&mut writer, &rows)?;
            rows.clear();
        }
    }
    // Ensure all remaining rows are written
    if !rows.is_empty() {
        write_to_parquet(&mut writer, &rows)?;
    }
}

// Function to handle parquet writing
fn write_to_parquet(writer: &mut SerializedFileWriter<File>, rows: &[Transaction]) -> Result<(), Box<dyn Error>> {
    let mut row_group_writer = writer.next_row_group()?;
    rows.write_to_row_group(&mut row_group_writer)?;
    row_group_writer.close()?;
    Ok(())
}
