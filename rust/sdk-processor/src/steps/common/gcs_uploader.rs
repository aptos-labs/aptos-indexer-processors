use crate::parquet_processors::{ParquetTypeEnum, ParquetTypeStructs, ParquetTypeTrait};
use anyhow::Context;
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use processor::bq_analytics::{
    gcs_handler::upload_parquet_to_gcs,
    generic_parquet_processor::{GetTimeStamp, HasParquetSchema, HasVersion},
};
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
    async fn upload_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError>;
}

#[async_trait]
impl Uploadable for GCSUploader {
    async fn upload_buffer(
        &mut self,
        buffer: ParquetTypeStructs,
    ) -> anyhow::Result<(), ProcessorError> {
        let parquet_type = buffer.parquet_type();
        let table_name = parquet_type.to_string();

        let result = buffer.upload_to_gcs(self, parquet_type, &table_name).await;
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

    /// # Context: Why we replace our writer
    ///
    /// Once we’re ready to upload (either because the buffer is full or enough time has passed),
    /// we don’t want to keep adding new data to that same writer. we want a clean slate for the next batch.
    /// So, we replace the old writer with a new one to empty the writer buffer without losing any data.
    fn get_and_replace_writer(
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
    pub async fn upload_generic<ParquetType>(
        &mut self,
        data: &[ParquetType],
        parquet_type: ParquetTypeEnum,
        table_name: &str,
    ) -> anyhow::Result<()>
    where
        ParquetType: HasVersion + GetTimeStamp + HasParquetSchema,
        for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
    {
        if data.is_empty() {
            println!("Buffer is empty, skipping upload.");
            return Ok(());
        }

        let writer = self
            .parquet_type_to_writer
            .get_mut(&parquet_type)
            .context("Writer not found for specified parquet type")?;

        let mut row_group_writer = writer.next_row_group().context("Failed to get row group")?;

        data.write_to_row_group(&mut row_group_writer)
            .context("Failed to write to row group")?;

        row_group_writer
            .close()
            .context("Failed to close row group")?;

        let old_writer = self
            .get_and_replace_writer(parquet_type)
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

        debug!(
            "Uploaded parquet to GCS for table: {}, start_version: {}, end_version: {}",
            table_name,
            data[0].version(),
            data[data.len() - 1].version()
        );

        Ok(())
    }
}
