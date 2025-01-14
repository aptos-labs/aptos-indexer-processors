use crate::parquet_processors::ParquetTypeTrait;
#[allow(unused_imports)]
use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    steps::common::gcs_uploader::{GCSUploader, Uploadable},
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use std::{collections::HashMap, time::Duration};
use tracing::debug;

/// `ParquetBuffer` is a struct that holds `ParquetTypeStructs` data
/// and tracks the buffer size in bytes, along with metadata about the data in the buffer.
struct ParquetBuffer {
    pub buffer: ParquetTypeStructs,
    pub buffer_size_bytes: usize,
    current_batch_metadata: Option<TransactionMetadata>,
}

impl ParquetBuffer {
    fn new(parquet_type: &ParquetTypeEnum) -> Self {
        Self {
            buffer: ParquetTypeStructs::default_for_type(parquet_type),
            buffer_size_bytes: 0,
            current_batch_metadata: None,
        }
    }

    /// Updates the metadata of the internal buffer with the latest information from `cur_batch_metadata`.
    /// This is used to track the end version and timestamp of the batch data in the buffer.
    pub fn update_current_batch_metadata(
        &mut self,
        cur_batch_metadata: &TransactionMetadata,
    ) -> Result<(), ProcessorError> {
        if let Some(buffer_metadata) = &mut self.current_batch_metadata {
            if buffer_metadata.end_version + 1 != cur_batch_metadata.start_version {
                // this shouldn't happen but if it does, we want to know
                return Err(ProcessorError::ProcessError {
                    message: format!(
                        "Gap founded: Buffer metadata end_version mismatch: {} != {}",
                        buffer_metadata.end_version, cur_batch_metadata.start_version
                    ),
                });
            }

            // Update metadata fields with the current batch's end information
            buffer_metadata.end_version = cur_batch_metadata.end_version;
            buffer_metadata.total_size_in_bytes = cur_batch_metadata.total_size_in_bytes;
            buffer_metadata.end_transaction_timestamp =
                cur_batch_metadata.end_transaction_timestamp;
        } else {
            // Initialize the metadata with the current batch's start information
            self.current_batch_metadata = Some(cur_batch_metadata.clone());
        }
        Ok(())
    }
}

/// `ParquetBufferStep` is a step that accumulates data in buffers until they reach a specified size limit.
///
/// It then uploads the buffered data to Google Cloud Storage (GCS) through an uploader.
/// This step is typically used to manage large data volumes efficiently by buffering and uploading
/// only when necessary.
///
///
/// # Type Parameters
/// - `U`: A type that implements the `Uploadable` trait, providing the uploading functionality.
pub struct ParquetBufferStep {
    internal_buffers: HashMap<ParquetTypeEnum, ParquetBuffer>,
    pub poll_interval: Duration,
    pub buffer_uploader: GCSUploader,
    pub buffer_max_size: usize,
}

impl ParquetBufferStep {
    pub fn new(
        poll_interval: Duration,
        buffer_uploader: GCSUploader,
        buffer_max_size: usize,
    ) -> Self {
        Self {
            internal_buffers: HashMap::new(),
            poll_interval,
            buffer_uploader,
            buffer_max_size,
        }
    }

    fn append_to_buffer(
        buffer: &mut ParquetBuffer,
        parquet_data: ParquetTypeStructs,
    ) -> Result<(), ProcessorError> {
        buffer.buffer_size_bytes += parquet_data.calculate_size();
        buffer.buffer.append(parquet_data)?;
        Ok(())
    }

    /// Handles the addition of `parquet_data` to the buffer for a specified `ParquetTypeEnum`.
    ///
    /// We check the size of the buffer + the size of the incoming data before appending it.
    /// If the sum of the two exceeds the maximum limit size, it uploads the buffer content to GCS to avoid
    /// spliting the batch data, allowing for more efficient and simpler version tracking.
    async fn upload_buffer_append(
        &mut self,
        parquet_type: ParquetTypeEnum,
        parquet_data: ParquetTypeStructs,
        cur_batch_metadata: &TransactionMetadata,
        upload_metadata_map: &mut HashMap<ParquetTypeEnum, TransactionMetadata>,
    ) -> Result<(), ProcessorError> {
        // Get or initialize the buffer for the specific ParquetTypeEnum
        let buffer = self
            .internal_buffers
            .entry(parquet_type)
            .or_insert_with(|| {
                debug!(
                    "Initializing buffer for ParquetTypeEnum: {:?}",
                    parquet_type,
                );
                ParquetBuffer::new(&parquet_type)
            });

        let curr_batch_size_bytes = parquet_data.calculate_size();

        debug!(
            "Current batch size for {:?}: {} bytes, buffer size before append: {} bytes",
            parquet_type, curr_batch_size_bytes, buffer.buffer_size_bytes,
        );

        // If the current buffer size + new batch exceeds max size, upload the buffer
        if buffer.buffer_size_bytes + curr_batch_size_bytes > self.buffer_max_size {
            println!(
                "Buffer size {} + batch size {} exceeds max size {}. Uploading buffer for {:?}.",
                buffer.buffer_size_bytes, curr_batch_size_bytes, self.buffer_max_size, parquet_type
            );

            // Take the current buffer to upload and reset the buffer in place
            let struct_buffer = std::mem::replace(
                &mut buffer.buffer,
                ParquetTypeStructs::default_for_type(&parquet_type),
            );
            self.buffer_uploader.upload_buffer(struct_buffer).await?;

            // update this metadata before insert
            upload_metadata_map
                .insert(parquet_type, buffer.current_batch_metadata.clone().unwrap());
            buffer.buffer_size_bytes = 0;
            buffer.current_batch_metadata = None;
        }

        // Append new data to the buffer
        Self::append_to_buffer(buffer, parquet_data)?;
        buffer.update_current_batch_metadata(cur_batch_metadata)?;

        debug!(
            "Updated buffer size for {:?}: {} bytes",
            parquet_type, buffer.buffer_size_bytes,
        );
        Ok(())
    }
}

#[async_trait]
impl Processable for ParquetBufferStep {
    type Input = HashMap<ParquetTypeEnum, ParquetTypeStructs>;
    type Output = HashMap<ParquetTypeEnum, TransactionMetadata>;
    type RunType = PollableAsyncRunType;

    /// Processes incoming `TransactionContext` data by appending it to the appropriate buffers.
    ///
    /// If any buffer exceeds the maximum size, its contents are uploaded.
    /// Returns metadata information on what was uploaded.
    async fn process(
        &mut self,
        item: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        debug!("Starting process for {} data items", item.data.len());

        let mut upload_metadata_map = HashMap::new();
        for (parquet_type, parquet_data) in item.data {
            self.upload_buffer_append(
                parquet_type,
                parquet_data,
                &item.metadata,
                &mut upload_metadata_map,
            )
            .await?;
        }

        if !upload_metadata_map.is_empty() {
            return Ok(Some(TransactionContext {
                data: upload_metadata_map,
                metadata: item.metadata, // cur_batch_metadata,
            }));
        }
        Ok(None)
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        let mut metadata_map = HashMap::new();
        debug!("Starting cleanup: uploading all remaining buffers.");
        for (parquet_type, mut buffer) in self.internal_buffers.drain() {
            if buffer.buffer_size_bytes > 0 {
                let struct_buffer = std::mem::replace(
                    &mut buffer.buffer,
                    ParquetTypeStructs::default_for_type(&parquet_type),
                );

                self.buffer_uploader.upload_buffer(struct_buffer).await?;

                if let Some(buffer_metadata) = &mut buffer.current_batch_metadata {
                    buffer_metadata.total_size_in_bytes = buffer.buffer_size_bytes as u64;
                    metadata_map.insert(parquet_type, buffer_metadata.clone());
                } else {
                    // This should never happen
                    panic!(
                        "Buffer metadata is missing for ParquetTypeEnum: {:?}",
                        parquet_type
                    );
                }
            }
        }
        self.internal_buffers.clear();

        debug!("Cleanup complete: all buffers uploaded.");
        if !metadata_map.is_empty() {
            return Ok(Some(vec![TransactionContext {
                data: metadata_map,
                metadata: TransactionMetadata::default(),
            }]));
        }
        Ok(None)
    }
}

#[async_trait]
impl PollableAsyncStep for ParquetBufferStep {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Polls all buffers to check if any should be uploaded based on the current size.
    /// Uploads data and clears the buffer if necessary, and returns upload metadata.
    async fn poll(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        let mut metadata_map = HashMap::new();
        debug!("Polling to check if any buffers need uploading.");

        for (parquet_type, mut buffer) in self.internal_buffers.drain() {
            if buffer.buffer_size_bytes > 0 {
                let struct_buffer = std::mem::replace(
                    &mut buffer.buffer,
                    ParquetTypeStructs::default_for_type(&parquet_type),
                );

                self.buffer_uploader.upload_buffer(struct_buffer).await?;

                let metadata = buffer.current_batch_metadata.clone().unwrap();
                metadata_map.insert(parquet_type, metadata);

                buffer.buffer_size_bytes = 0;
                buffer.current_batch_metadata = None;
            }
        }

        if !metadata_map.is_empty() {
            return Ok(Some(vec![TransactionContext {
                data: metadata_map,
                metadata: TransactionMetadata::default(),
            }]));
        }
        Ok(None)
    }
}

impl NamedStep for ParquetBufferStep {
    fn name(&self) -> String {
        "ParquetBufferStep".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::db_config::ParquetConfig,
        steps::common::{
            gcs_uploader::{create_new_writer, GCSUploader},
            parquet_buffer_step::{ParquetBufferStep, ParquetTypeEnum, ParquetTypeStructs},
        },
    };
    use aptos_indexer_processor_sdk::{
        traits::Processable,
        types::transaction_context::{TransactionContext, TransactionMetadata},
    };
    use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
    use parquet::schema::types::Type;
    use processor::{
        bq_analytics::generic_parquet_processor::HasParquetSchema,
        db::parquet::models::default_models::parquet_move_resources::MoveResource,
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_parquet_buffer_step_no_upload() -> anyhow::Result<()> {
        let db_config = create_parquet_db_config();
        let buffer_uploader = create_parquet_uploader(&db_config).await?;
        let mut parquet_step =
            ParquetBufferStep::new(Duration::from_secs(10), buffer_uploader, 100);

        let data = HashMap::from([(
            ParquetTypeEnum::MoveResources,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResources),
        )]);
        let metadata = TransactionMetadata::default();

        let result = parquet_step
            .process(TransactionContext { data, metadata })
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Expected no upload for data below buffer_max_size"
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_parquet_buffer_step_trigger_upload() -> anyhow::Result<()> {
        let buffer_max_size = 25; // Default ParquetTypeStructs for MoveResource is 24 bytes
        let db_config = create_parquet_db_config();

        let buffer_uploader = create_parquet_uploader(&db_config).await?;
        let mut parquet_step =
            ParquetBufferStep::new(Duration::from_secs(10), buffer_uploader, buffer_max_size);

        // Test data below `buffer_max_size`
        let data = HashMap::from([(
            ParquetTypeEnum::MoveResources,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResources),
        )]);
        let metadata = TransactionMetadata::default();

        let result = parquet_step
            .process(TransactionContext { data, metadata })
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "Expected no upload for data below buffer_max_size"
        );

        // Test buffer + data > `buffer_max_size`
        let data = HashMap::from([(
            ParquetTypeEnum::MoveResources,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResources),
        )]);
        let metadata = TransactionMetadata::default();

        let result = parquet_step
            .process(TransactionContext { data, metadata })
            .await
            .unwrap();

        assert!(
            result.is_some(),
            "Expected upload when data exceeds buffer_max_size"
        );

        Ok(())
    }

    async fn create_parquet_uploader(db_config: &ParquetConfig) -> anyhow::Result<GCSUploader> {
        let gcs_config = GcsClientConfig::default()
            .with_auth()
            .await
            .expect("Failed to create GCS client config");
        let gcs_client = Arc::new(GCSClient::new(gcs_config));

        let parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>> =
            [(ParquetTypeEnum::MoveResources, MoveResource::schema())]
                .into_iter()
                .collect();

        let parquet_type_to_writer = parquet_type_to_schemas
            .iter()
            .map(|(key, schema)| {
                let writer = create_new_writer(schema.clone()).expect("Failed to create writer");
                (*key, writer)
            })
            .collect();

        GCSUploader::new(
            gcs_client,
            parquet_type_to_schemas,
            parquet_type_to_writer,
            db_config.bucket_name.clone(),
            db_config.bucket_root.clone(),
            "processor_name".to_string(),
        )
    }

    fn create_parquet_db_config() -> ParquetConfig {
        ParquetConfig {
            connection_string: "connection_string".to_string(),
            db_pool_size: 10,
            bucket_name: "bucket_name".to_string(),
            bucket_root: "bucket_root".to_string(),
            google_application_credentials: None,
        }
    }
}
