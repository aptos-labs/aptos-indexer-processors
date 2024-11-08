use crate::parquet_processors::{ParquetStruct, ParquetTypeEnum, ParquetTypeStructs};
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    traits::parquet_extract_trait::{GetTimeStamp, HasParquetSchema, HasVersion},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use processor::bq_analytics::gcs_handler::upload_parquet_to_gcs;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::{debug, error};

pub struct GCSUploader {
    gcs_client: Arc<GCSClient>,
    parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>>,
    parquet_type_to_writer: HashMap<ParquetTypeEnum, SerializedFileWriter<Vec<u8>>>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub processor_name: String,
}

#[async_trait]
pub trait Uploadable {
    async fn handle_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError>;
}

#[async_trait]
impl Uploadable for GCSUploader {
    async fn handle_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError> {
        // Directly call `upload_generic` for each buffer type
        let table_name = buffer.get_table_name();

        let result = match buffer {
            ParquetTypeStructs::Transaction(transactions) => {
                self.upload_generic(&transactions[..], ParquetTypeEnum::Transaction, table_name)
                    .await
            },
            ParquetTypeStructs::MoveResource(resources) => {
                self.upload_generic(&resources[..], ParquetTypeEnum::MoveResource, table_name)
                    .await
            },
            ParquetTypeStructs::WriteSetChange(changes) => {
                self.upload_generic(&changes[..], ParquetTypeEnum::WriteSetChange, table_name)
                    .await
            },
            ParquetTypeStructs::TableItem(items) => {
                self.upload_generic(&items[..], ParquetTypeEnum::TableItem, table_name)
                    .await
            },
            ParquetTypeStructs::MoveModule(modules) => {
                self.upload_generic(&modules[..], ParquetTypeEnum::MoveModule, table_name)
                    .await
            },
        };

        if let Err(e) = result {
            error!("Failed to upload buffer: {}", e);
            return Err(ProcessorError::ProcessError {
                message: format!("Failed to upload buffer: {}", e),
            });
        }
        Ok(())
    }
}

pub fn create_new_writer(schema: Arc<Type>) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();
    let props_arc = Arc::new(props);

    SerializedFileWriter::new(Vec::new(), schema, props_arc).context("Failed to create new writer")
}

impl GCSUploader {
    pub fn new(
        gcs_client: Arc<GCSClient>,
        parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>>,
        parquet_type_to_writer: HashMap<ParquetTypeEnum, SerializedFileWriter<Vec<u8>>>,
        bucket_name: String,
        bucket_root: String,
        processor_name: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            gcs_client,
            parquet_type_to_schemas,
            parquet_type_to_writer,
            bucket_name,
            bucket_root,
            processor_name,
        })
    }

    fn create_new_writer(
        &self,
        parquet_type: ParquetTypeEnum,
    ) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
        let schema = self
            .parquet_type_to_schemas
            .get(&parquet_type)
            .context("Parquet type not found in schemas")?
            .clone();

        create_new_writer(schema)
    }

    fn close_writer(
        &mut self,
        parquet_type: ParquetTypeEnum,
    ) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
        let old_writer = self
            .parquet_type_to_writer
            .remove(&parquet_type)
            .context("Writer for specified Parquet type not found")?;

        // Create a new writer and replace the old writer with it
        let new_writer = self.create_new_writer(parquet_type)?;
        self.parquet_type_to_writer.insert(parquet_type, new_writer);

        // Return the old writer so its contents can be used
        Ok(old_writer)
    }

    // Generic upload function to handle any data type
    async fn upload_generic<ParquetType>(
        &mut self,
        data: &[ParquetType],
        parquet_type: ParquetTypeEnum,
        table_name: &'static str,
    ) -> anyhow::Result<()>
    where
        ParquetType: ParquetStruct + HasVersion + GetTimeStamp + HasParquetSchema,
        for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
    {
        if data.is_empty() {
            debug!("Buffer is empty, skipping upload.");
            return Ok(());
        }

        let writer = self
            .parquet_type_to_writer
            .get_mut(&parquet_type)
            .context("Writer not found for specified parquet type")?;

        // Extract metadata from the first and last items
        // let first_item = &data[0];
        // let last_item = data.last().unwrap();
        // let start_version = first_item.version();
        // let first_timestamp = first_item.get_timestamp();
        // let end_version = last_item.version();
        // let last_timestamp = last_item.get_timestamp();

        let mut row_group_writer = writer.next_row_group().context("Failed to get row group")?;

        data.write_to_row_group(&mut row_group_writer)
            .context("Failed to write to row group")?;

        row_group_writer
            .close()
            .context("Failed to close row group")?;

        let old_writer = self
            .close_writer(parquet_type)
            .context("Failed to close writer")?;
        let upload_buffer = old_writer
            .into_inner()
            .context("Failed to get inner buffer")?;

        let bucket_root = PathBuf::from(&self.bucket_root);
        upload_parquet_to_gcs(
            &self.gcs_client,
            upload_buffer,
            table_name,
            &self.bucket_name,
            &bucket_root,
            self.processor_name.clone(),
        )
        .await?;

        // Update metadata
        // metadata.start_version = start_version as u64;
        // metadata.end_version = end_version as u64;
        // metadata.start_transaction_timestamp = Some(first_timestamp);
        // metadata.end_transaction_timestamp = Some(last_timestamp);

        Ok(())
    }
}
