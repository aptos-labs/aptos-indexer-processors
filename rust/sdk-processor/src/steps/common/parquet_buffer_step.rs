use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    steps::common::gcs_uploader::Uploadable,
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
use mockall::mock;
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
    pub fn update_current_batch_metadata(&mut self, cur_batch_metadata: &TransactionMetadata) {
        if let Some(buffer_metadata) = &mut self.current_batch_metadata {
            // Update metadata fields with the current batch's end information
            buffer_metadata.end_version = cur_batch_metadata.end_version;
            buffer_metadata.total_size_in_bytes = cur_batch_metadata.total_size_in_bytes;
            buffer_metadata.end_transaction_timestamp =
                cur_batch_metadata.end_transaction_timestamp.clone();
        } else {
            // Initialize the metadata with the current batch's start information
            self.current_batch_metadata = Some(cur_batch_metadata.clone());
        }
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
pub struct ParquetBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    internal_buffers: HashMap<ParquetTypeEnum, ParquetBuffer>,
    pub poll_interval: Duration,
    pub buffer_uploader: U,
    pub buffer_max_size: usize,
}

impl<U> ParquetBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    pub fn new(poll_interval: Duration, buffer_uploader: U, buffer_max_size: usize) -> Self {
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
        buffer.buffer.append(parquet_data)
    }

    /// Handles the addition of `parquet_data` to the buffer for a specified `ParquetTypeEnum`.
    ///
    /// We check the size of the buffer + the size of the incoming data before appending it.
    /// If the sum of the two exceeds the maximum limit size, it uploads the buffer content to GCS to avoid
    /// spliting the batch data, allowing for more efficient and simpler version tracking.
    async fn handle_buffer_append(
        &mut self,
        parquet_type: ParquetTypeEnum,
        parquet_data: ParquetTypeStructs,
        cur_batch_metadata: &TransactionMetadata,
        upload_metadata_map: &mut HashMap<ParquetTypeEnum, TransactionMetadata>,
    ) -> Result<(), ProcessorError> {
        let mut file_uploaded = false;

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
            self.buffer_uploader.handle_buffer(struct_buffer).await?;
            file_uploaded = true;

            // update this metadata before insert
            upload_metadata_map
                .insert(parquet_type, buffer.current_batch_metadata.clone().unwrap());
            buffer.buffer_size_bytes = 0;
        }

        // Append new data to the buffer
        Self::append_to_buffer(buffer, parquet_data)?;
        buffer.update_current_batch_metadata(cur_batch_metadata);

        // if it wasn't uploaded -> we update only end_version, size, and last timestamp
        if file_uploaded {
            if let Some(buffer_metadata) = &mut buffer.current_batch_metadata {
                buffer_metadata.start_version = cur_batch_metadata.start_version;
                buffer_metadata.start_transaction_timestamp =
                    cur_batch_metadata.start_transaction_timestamp.clone();
            }
        }

        debug!(
            "Updated buffer size for {:?}: {} bytes",
            parquet_type, buffer.buffer_size_bytes,
        );
        Ok(())
    }
}

#[async_trait]
impl<U> Processable for ParquetBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
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
            self.handle_buffer_append(
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

                self.buffer_uploader.handle_buffer(struct_buffer).await?;

                if let Some(buffer_metadata) = &mut buffer.current_batch_metadata {
                    buffer_metadata.total_size_in_bytes = buffer.buffer_size_bytes as u64;
                    metadata_map.insert(parquet_type, buffer_metadata.clone());
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
impl<U> PollableAsyncStep for ParquetBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
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

                self.buffer_uploader.handle_buffer(struct_buffer).await?;

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

impl<U> NamedStep for ParquetBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    fn name(&self) -> String {
        "ParquetBufferStep".to_string()
    }
}

mock! {
    pub Uploadable {}
    #[async_trait]
    impl Uploadable for Uploadable {
        async fn handle_buffer(&mut self, buffer: ParquetTypeStructs) -> Result<(), ProcessorError>;
    }
}

#[cfg(test)]
mod tests {
    use crate::steps::common::parquet_buffer_step::{
        MockUploadable, ParquetBufferStep, ParquetTypeStructs,
    };
    use aptos_indexer_processor_sdk::{
        traits::Processable,
        types::transaction_context::{TransactionContext, TransactionMetadata},
    };
    use std::{collections::HashMap, time::Duration};

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_parquet_buffer_step_no_upload() {
        let poll_interval = Duration::from_secs(10);
        let buffer_max_size = 100;
        use crate::steps::common::parquet_buffer_step::ParquetTypeEnum;

        let mock_uploader = MockUploadable::new();

        let mut parquet_step =
            ParquetBufferStep::new(poll_interval, mock_uploader, buffer_max_size);

        // Test the `process` method with data below `buffer_max_size`
        let data = HashMap::from([(
            ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResource),
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
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_parquet_buffer_step() {
        let poll_interval = Duration::from_secs(10);
        let buffer_max_size = 25; // default parquetTYpeStruct for move resource is 24 bytes
        use crate::steps::common::parquet_buffer_step::ParquetTypeEnum;

        let mut mock_uploader = MockUploadable::new();

        // Set up expectations for the mock uploader
        mock_uploader
            .expect_handle_buffer()
            .times(1)
            .returning(|_| Ok(()));

        let mut parquet_step =
            ParquetBufferStep::new(poll_interval, mock_uploader, buffer_max_size);
        // Test the `process` method with data below `buffer_max_size`
        let data = HashMap::from([(
            ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResource),
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

        // Test the `process` method with data below `buffer_max_size`
        let data = HashMap::from([(
            ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::default_for_type(&ParquetTypeEnum::MoveResource),
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
    }
}
