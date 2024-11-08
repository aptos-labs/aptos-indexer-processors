use allocative::Allocative;
use anyhow::Context;
use anyhow::anyhow;
use aptos_indexer_processor_sdk::{
    common_steps::timed_size_buffer_step::BufferHandler,
    traits::parquet_extract_trait::{GetTimeStamp, HasParquetSchema, HasVersion, NamedTable},
    types::transaction_context::TransactionMetadata,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use processor::{
    bq_analytics::gcs_handler::upload_parquet_to_gcs, utils::util::naive_datetime_to_timestamp,
};
use std::{marker::PhantomData, path::PathBuf, sync::Arc};
use tracing::{debug, error};
use crate::parquet_processors::{ParquetTypeEnum, ParquetTypeStructs};
use std::collections::HashMap;

pub struct GCSUploader
{
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
impl Uploadable for GCSUploader  {
    async fn handle_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError> {
        if let Err(e) = self.upload_buffer(buffer).await {
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
    fn create_new_writer(&self, parquet_type: ParquetTypeEnum) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
        let schema = self.parquet_type_to_schemas.get(&parquet_type).map_or_else(|| Err(anyhow!("Parquet type not found in schemas")), |schema| create_new_writer(schema.clone())).unwrap();
        Ok(schema)
        // processor::bq_analytics::generic_parquet_processor::create_new_writer(schema);
    }

    // fn close_writer(&mut self) -> anyhow::Result<SerializedFileWriter<Vec<u8>>> {
    //
    //     // let new_writer = self.create_new_writer()?;
    //     // let old_writer = std::mem::replace(&mut self.writer, new_writer);
    //     // Ok(old_writer)
    // }

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

    async fn upload_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<()> {
        
        // let buffer  = match buffer {
        //     ParquetTypeStructs::TransactionStructs(buffer) => buffer,
        //     _ => return Err(anyhow!("Buffer is not a transaction struct")),
        // };
        // if buffer.is_empty() {
        //     debug!("Buffer is empty, skipping upload.");
        //     return Ok(());
        // }
        // let struts = buffer.get_structs();
        //
        // let first = struts
        //     .first()
        //     .context("Buffer is not empty but has no first element")?;
        // let first_transaction_timestamp = naive_datetime_to_timestamp(first.get_timestamp());
        // let start_version = first.version();
        // let last = buffer
        //     .last()
        //     .context("Buffer is not empty but has no last element")?;
        // let end_version = last.version();
        // let last_transaction_timestamp = naive_datetime_to_timestamp(last.get_timestamp());
        // let mut row_group_writer = self
        //     .writer
        //     .next_row_group()
        //     .context("Failed to get row group")?;
        //
        // buffer
        //     .as_slice()
        //     .write_to_row_group(&mut row_group_writer)
        //     .context("Failed to write to row group")?;
        // row_group_writer
        //     .close()
        //     .context("Failed to close row group")?;
        //
        // let old_writer = self.close_writer().context("Failed to close writer")?;
        // let upload_buffer = old_writer
        //     .into_inner()
        //     .context("Failed to get inner buffer")?;
        //
        // let bucket_root = PathBuf::from(&self.bucket_root);
        //
        // upload_parquet_to_gcs(
        //     &gcs_client,
        //     upload_buffer,
        //     ParquetType::TABLE_NAME,
        //     &self.bucket_name,
        //     &bucket_root,
        //     self.processor_name.clone(),
        // )
        // .await?;
        //
        // metadata.start_version = start_version as u64;
        // metadata.end_version = end_version as u64;
        // metadata.start_transaction_timestamp = Some(first_transaction_timestamp);
        // metadata.end_transaction_timestamp = Some(last_transaction_timestamp);

        Ok(())
    }
}
