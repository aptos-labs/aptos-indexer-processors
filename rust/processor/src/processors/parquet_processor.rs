// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::{
        block_metadata_transactions::{BlockMetadataTransaction, BlockMetadataTransactionModel},
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        transactions::TransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, PgDbPool},
    schemas::default_schemas::transactions::TRANSACTION_SCHEMA,
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
            block_metadata_transactions,
            write_set_changes,
            (move_modules, move_resources, table_items, current_table_items, table_metadata),
        ) = tokio::task::spawn_blocking(move || process_transactions(transactions))
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            (
                &table_items,
                &current_table_items,
            ),
            &self.per_table_chunk_sizes,
        )
        .await;

        info!(
            start_version = start_version,
            end_version = end_version,
            processor_name = self.name(),
            db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64(),
            "converting transactions to record batch and writing to parquet file",
        );
        // // transaction schema for the parquet file 
        // let schema = Arc::new(Schema::new(vec![
        //     Field::new("version", DataType::Int64, false),
        //     Field::new("block_height", DataType::Int64, false),
        //     Field::new("hash", DataType::Utf8, false),
        //     Field::new("type_", DataType::Utf8, false),
        //     // Representing `serde_json::Value` as a string. Adjust based on your actual serialization needs.
        //     // Utf8 (json text), binary (serialized json), or  nested structure (expensive)
        //     Field::new("payload", DataType::Utf8, true),
        //     Field::new("state_change_hash", DataType::Utf8, false),
        //     Field::new("event_root_hash", DataType::Utf8, false),
        //     // Option<String> represented as nullable UTF8
        //     Field::new("state_checkpoint_hash", DataType::Utf8, true),
        //     // BigDecimal as String due to lack of direct support in Arrow. Consider Float64 if applicable.
        //     Field::new("gas_used", DataType::Utf8, false),
        //     Field::new("success", DataType::Boolean, false),
        //     Field::new("vm_status", DataType::Utf8, false),
        //     Field::new("accumulator_root_hash", DataType::Utf8, false),
        //     Field::new("num_events", DataType::Int64, false),
        //     Field::new("num_write_set_changes", DataType::Int64, false),
        //     Field::new("epoch", DataType::Int64, false),
        //     // Option<String> represented as nullable UTF8
        //     Field::new("payload_type", DataType::Utf8, true),
        // ]));

        // convert a vec Transaction into a RecordBatch
        let mut version = Int64Array::from(txns.iter().map(|x| x.version).collect::<Vec<i64>>());
        let mut block_height = Int64Array::from(txns.iter().map(|x| x.block_height).collect::<Vec<i64>>());
        let mut hash = StringArray::from(txns.iter().map(|x| x.hash.clone()).collect::<Vec<String>>());
        let mut type_ = StringArray::from(txns.iter().map(|x| x.type_.clone()).collect::<Vec<String>>());
        // For the `payload` field, you might serialize `serde_json::Value` to a JSON string
        let mut payload_builder = StringBuilder::new();
        for payload in txns.iter().map(|x| &x.payload) {
            match payload {
                Some(value) => {
                    let serialized = serde_json::to_string(value).unwrap(); // Handle errors as needed
                    payload_builder.append_value(&serialized);
                },
                None => payload_builder.append_null(),
            }
        }
        let payload = payload_builder.finish();

        let mut state_change_hash = StringArray::from(txns.iter().map(|x| x.state_change_hash.clone()).collect::<Vec<String>>());

        // For fields that are `Option<String>`, handle `None` as null. Showing `state_checkpoint_hash` as an example:
        let mut state_checkpoint_hash_builder = StringBuilder::new();
        for hash in txns.iter().map(|x| &x.state_checkpoint_hash) {
            match hash {
                Some(value) => state_checkpoint_hash_builder.append_value(value),
                None => state_checkpoint_hash_builder.append_null(),
            }
        }
        
        let state_checkpoint_hash = state_checkpoint_hash_builder.finish();
        // if we want to filter None values
        // let state_checkpoint_hash = StringArray::from(
        //     txns.iter()
        //         .filter_map(|x| x.state_checkpoint_hash.clone()) 
        //         .collect::<Vec<String>>() 
        // );

        let mut event_root_hash = StringArray::from(txns.iter().map(|x| x.event_root_hash.clone()).collect::<Vec<String>>());
        
        let gas_used = StringArray::from(txns.iter().map(|x| x.gas_used.to_string()).collect::<Vec<String>>());
        
        let success = BooleanArray::from(txns.iter().map(|x| x.success).collect::<Vec<bool>>());
        let mut vm_status = StringArray::from(txns.iter().map(|x| x.vm_status.clone()).collect::<Vec<String>>());
        let mut accumulator_root_hash = StringArray::from(txns.iter().map(|x| x.accumulator_root_hash.clone()).collect::<Vec<String>>());
        let mut num_events = Int64Array::from(txns.iter().map(|x| x.num_events).collect::<Vec<i64>>());
        let mut num_write_set_changes = Int64Array::from(txns.iter().map(|x| x.num_write_set_changes).collect::<Vec<i64>>());
        let mut epoch = Int64Array::from(txns.iter().map(|x| x.epoch).collect::<Vec<i64>>());

        let mut payload_type_builder = StringBuilder::new();
        for payload_type in txns.iter().map(|x| &x.payload_type) {
            match payload_type {
                Some(value) => {
                    let serialized = serde_json::to_string(value).unwrap(); // Handle errors as needed
                    payload_type_builder.append_value(&serialized)
                },
                None => payload_type_builder.append_null(),
            }
        }
        let payload_type = payload_type_builder.finish();
        
        let record_batch = RecordBatch::try_new(Arc::new((&*TRANSACTION_SCHEMA).clone()), vec![
            Arc::new(version),
            Arc::new(block_height),
            Arc::new(hash),
            Arc::new(type_),
            Arc::new(payload),
            Arc::new(state_change_hash),
            Arc::new(event_root_hash),
            Arc::new(state_checkpoint_hash),
            Arc::new(gas_used),
            Arc::new(success),
            Arc::new(vm_status),
            Arc::new(accumulator_root_hash),
            Arc::new(num_events),
            Arc::new(num_write_set_changes),
            Arc::new(epoch),
            Arc::new(payload_type),
        ]).unwrap();

        info!(record_batch = ?record_batch, "Record batch created");
        let result = client.list_buckets(&ListBucketsRequest{
            project: "aptos-yuun-playground".to_string(),
            ..Default::default()
        }).await;
        // log the result
        info!(result = ?result, "List buckets result");
        
        // write the record_batch into parquet file
        let file_path = PathBuf::from("some_proper_name_goes_here");
        let file = File::create(&file_path).unwrap(); 

        let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();

        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch).expect("write record batch");
        // writer must be closed to write the footer
        writer.close().unwrap();
            
        // write the parquet file into gcs
        let upload_result = upload_parquet_to_gcs(&file_path, &self.config.bucket, client).await?;


        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp,
            }),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

fn process_transactions(
    transactions: Vec<Transaction>,
) -> (
    Vec<crate::models::default_models::transactions::Transaction>,
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



async fn insert_to_db(
    conn: PgDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    (table_items, current_table_items): (
        &[TableItem],
        &[CurrentTableItem],
    ),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let ti_res = execute_in_chunks(
        conn.clone(),
        insert_table_items_query,
        table_items,
        get_config_table_chunk_size::<TableItem>("table_items", per_table_chunk_sizes),
    );

    let cti_res = execute_in_chunks(
        conn.clone(),
        insert_current_table_items_query,
        current_table_items,
        get_config_table_chunk_size::<CurrentTableItem>(
            "current_table_items",
            per_table_chunk_sizes,
        ),
    );

    let (ti_res, cti_res) =
        join!(ti_res, cti_res);

    for res in [
        ti_res, cti_res,
    ] {
        res?;
    }

    Ok(())
}


fn insert_table_items_query(
    items_to_insert: Vec<TableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::table_items::dsl::*;

    (
        diesel::insert_into(schema::table_items::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_table_items_query(
    items_to_insert: Vec<CurrentTableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_table_items::dsl::*;

    (
        diesel::insert_into(schema::current_table_items::table)
            .values(items_to_insert)
            .on_conflict((table_handle, key_hash))
            .do_update()
            .set((
                key.eq(excluded(key)),
                decoded_key.eq(excluded(decoded_key)),
                decoded_value.eq(excluded(decoded_value)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_table_items.last_transaction_version <= excluded.last_transaction_version "),
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
    file_path: &PathBuf,
    bucket_name: &str,
    client: &Client,
) -> Result<String> {
    // Re-open the file for reading
    let mut file = TokioFile::open(&file_path).await.map_err(|e| anyhow!("Failed to open file for reading: {}", e))?;

    // Read the file into a Vec<u8>
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.map_err(|e| anyhow!("Failed to read file: {}", e))?;

    // Convert the buffer into a Body for the upload
    let data = Body::from(buffer);

    // Create the UploadObjectRequest
    let upload_request = UploadObjectRequest {
        bucket: bucket_name.to_owned(),
        ..Default::default()
    };
    
    // Clone file_path and get the file name part as an OsStr
    let file_name_os_str = file_path.file_name().unwrap();
    // Convert the OsStr to a &str and ensure it's owned by storing it in a variable
    let file_name_str = file_name_os_str.to_str().unwrap().to_owned();
    // Specify the upload type as simple
    let upload_type = UploadType::Simple(Media::new(file_name_str.clone()));

    // Perform the upload, providing all three expected arguments
    let result = client.upload_object(&upload_request, data, &upload_type).await
        .map_err(|e| anyhow!("Failed to upload file: {}", e))?;

    // The result object contains information about the uploaded file; you might want to return something specific from it
    Ok(result.name)
}