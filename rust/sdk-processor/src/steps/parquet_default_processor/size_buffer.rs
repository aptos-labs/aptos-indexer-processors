use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    steps::parquet_default_processor::gcs_handler::Uploadable,
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

struct ParquetBuffer {
    pub buffer: ParquetTypeStructs,
    pub buffer_size_bytes: usize,
    pub max_size: usize,
    current_batch_metadata: TransactionMetadata,
}

impl ParquetBuffer {
    pub fn update_current_batch_metadata(&mut self, cur_batch_metadata: &TransactionMetadata, uploaded: bool) {
        let buffer_metadata = &mut self.current_batch_metadata;
        buffer_metadata.end_version = cur_batch_metadata.end_version;
        buffer_metadata.total_size_in_bytes = cur_batch_metadata.total_size_in_bytes;
        buffer_metadata.end_transaction_timestamp = cur_batch_metadata.end_transaction_timestamp.clone();
        // if it wasn't uploaded -> we update only end_verion, size, and last timttesampe
        if uploaded {
            buffer_metadata.start_version = cur_batch_metadata.start_version;
            buffer_metadata.start_transaction_timestamp = cur_batch_metadata.start_transaction_timestamp.clone();
        }
    }
}

pub struct SizeBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    internal_buffers: HashMap<ParquetTypeEnum, ParquetBuffer>,
    pub internal_buffer_size_bytes: usize,
    pub poll_interval: Duration,
    pub buffer_uploader: U,
}

impl<U> SizeBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    pub fn new(poll_interval: Duration, buffer_uploader: U) -> Self {
        Self {
            internal_buffers: HashMap::new(),
            internal_buffer_size_bytes: 0,
            poll_interval,
            buffer_uploader,
        }
    }

    fn calculate_batch_size(parquet_data: &ParquetTypeStructs) -> usize {
        match parquet_data {
            ParquetTypeStructs::MoveResource(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::WriteSetChange(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::Transaction(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::TableItem(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::MoveModule(data) => allocative::size_of_unique(data),
        }
    }

    fn append_to_buffer(buffer: &mut ParquetBuffer, parquet_data: ParquetTypeStructs) -> Result<(), ProcessorError> {
        buffer.buffer_size_bytes += Self::calculate_batch_size(&parquet_data);
        match (&mut buffer.buffer, parquet_data) {
            (ParquetTypeStructs::MoveResource(buf), ParquetTypeStructs::MoveResource(mut data)) => buf.append(&mut data),
            (ParquetTypeStructs::WriteSetChange(buf), ParquetTypeStructs::WriteSetChange(mut data)) => buf.append(&mut data),
            (ParquetTypeStructs::Transaction(buf), ParquetTypeStructs::Transaction(mut data)) => buf.append(&mut data),
            (ParquetTypeStructs::TableItem(buf), ParquetTypeStructs::TableItem(mut data)) => buf.append(&mut data),
            (ParquetTypeStructs::MoveModule(buf), ParquetTypeStructs::MoveModule(mut data)) => buf.append(&mut data),
            _ => return Err(ProcessorError::ProcessError { message: "Failed to upload buffer".to_string() }),
        };
        Ok(())
    }
}


#[async_trait]
impl<U> Processable for SizeBufferStep<U>
    where
        U: Uploadable + Send + 'static + Sync,
{
    type Input = HashMap<ParquetTypeEnum, ParquetTypeStructs>;
    type Output = HashMap<ParquetTypeEnum, TransactionMetadata>;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        println!("Starting process for {} data items", item.data.len());

        let mut parquet_types = item.data;
        let cur_batch_metadata = item.metadata;
        let mut file_uploaded = false;
        let mut upload_metadata_map = HashMap::new();

        for (parquet_type, parquet_data) in parquet_types.drain() {
            println!("Processing ParquetTypeEnum: {:?}", parquet_type);

            // Get or initialize the buffer for the specific ParquetTypeEnum
            let buffer = self
                .internal_buffers
                .entry(parquet_type)
                .or_insert_with(|| {
                    println!(
                        "Initializing buffer for ParquetTypeEnum: {:?}",
                        parquet_type
                    );
                    // TODO: revisit.
                    ParquetBuffer {
                        buffer: ParquetTypeStructs::default_for_type(&parquet_type), // Maybe this could be replaced with actual value
                        buffer_size_bytes: 0,
                        max_size: self.internal_buffer_size_bytes, // maybe we can extend this to more configurable size per table.
                        current_batch_metadata: TransactionMetadata::default(),
                    }
                });

            let curr_batch_size_bytes = Self::calculate_batch_size(&parquet_data);

            println!(
                "Current batch size for {:?}: {} bytes, buffer size before append: {} bytes",
                parquet_type, curr_batch_size_bytes, buffer.buffer_size_bytes
            );

            // If the current buffer size + new batch exceeds max size, upload the buffer
            if buffer.buffer_size_bytes + curr_batch_size_bytes > buffer.max_size {
                println!(
                    "Buffer size {} + batch size {} exceeds max size {}. Uploading buffer for {:?}.",
                    buffer.buffer_size_bytes,
                    curr_batch_size_bytes,
                    buffer.max_size,
                    parquet_type
                );

                // Take the current buffer to upload and reset the buffer in place
                let struct_buffer = std::mem::replace(
                    &mut buffer.buffer,
                    ParquetTypeStructs::default_for_type(&parquet_type),
                );
                self.buffer_uploader.handle_buffer(struct_buffer).await?;
                file_uploaded = true;

                // update this metadata before insert
                let metadata = buffer.current_batch_metadata.clone();
                upload_metadata_map.insert(parquet_type, metadata);
                buffer.buffer_size_bytes = 0; // Reset buffer size
            }

            // Append new data to the buffer
            Self::append_to_buffer(buffer, parquet_data)?;
            // update the buffer metadadata
            buffer.update_current_batch_metadata(&cur_batch_metadata, file_uploaded);
            println!(
                "Updated buffer size for {:?}: {} bytes",
                parquet_type, buffer.buffer_size_bytes
            );
        }

        if !upload_metadata_map.is_empty() {
            // println!(
            //     "File uploaded for {:?}. Returning updated metadata.",
            //     parquet_type
            // );
            return Ok(Some(TransactionContext { data: upload_metadata_map, metadata: cur_batch_metadata }));
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

                let mut metadata = buffer.current_batch_metadata.clone();
                metadata.total_size_in_bytes = buffer.buffer_size_bytes as u64;
                metadata_map.insert(parquet_type, metadata);
            }
        }

        debug!("Cleanup complete: all buffers uploaded.");
        if !metadata_map.is_empty() {
            return Ok(Some(vec![TransactionContext { data: metadata_map, metadata: TransactionMetadata::default() }]));
        }
        Ok(None)
    }
}

#[async_trait]
impl<U> PollableAsyncStep for SizeBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

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

                let metadata = buffer.current_batch_metadata.clone();
                metadata_map.insert(parquet_type, metadata);

                // Reset ParquetBuffer or maybe remove?
                buffer.buffer_size_bytes = 0; 
                buffer.current_batch_metadata = TransactionMetadata::default();
            }
        }

        if !metadata_map.is_empty() {
            return Ok(Some(vec![TransactionContext { data: metadata_map, metadata: TransactionMetadata::default() }]));
        }
        Ok(None)
    }
}

impl<U> NamedStep for SizeBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    fn name(&self) -> String {
        "DefaultTimedSizeBuffer".to_string()
    }
}
