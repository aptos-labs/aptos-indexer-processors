use aptos_indexer_processor_sdk::{
    traits::{
        pollable_async_step::PollableAsyncRunType,
        NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{time::Duration};
use std::collections::HashMap;
use crate::parquet_processors::ParquetTypeEnum;
use crate::parquet_processors::ParquetTypeStructs;
use crate::steps::parquet_default_processor::gcs_handler::Uploadable;

struct ParquetBuffer {
    pub buffer: ParquetTypeStructs,
    pub buffer_size_bytes: usize,
    pub max_size: usize,
    current_batch_metadata: TransactionMetadata,
    upload_interval: Duration, // not sure if needed here
}

pub struct SizeBufferStep<U>
where
    U: Uploadable + Send + 'static + Sync,
{
    pub internal_buffers: HashMap<ParquetTypeEnum, ParquetBuffer>,
    pub internal_buffer_size_bytes: usize,
    pub poll_interval: Duration,
    pub buffer_uploader: U,
}

#[async_trait]
impl<U> Processable for SizeBufferStep<U>
where
    U: Uploadable + Send + 'static  + Sync,
{
    type Input = HashMap<ParquetTypeEnum, ParquetTypeStructs>;
    type Output = ();
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        println!("Starting process for {} data items", item.data.len());

        let mut parquet_types = item.data; // Extracts the data from the input
        let mut metadata = item.metadata;
        let mut file_uploaded = false;

        for (parquet_type, mut parquet_data) in parquet_types.drain() {
            println!("Processing ParquetTypeEnum: {:?}", parquet_type);

            
            // Get or initialize the buffer for the specific ParquetTypeEnum
            let buffer = self
                .internal_buffers
                .entry(parquet_type.clone())
                .or_insert_with(|| {
                    println!("Initializing buffer for ParquetTypeEnum: {:?}", parquet_type);
                    // TODO: revisit. 
                    ParquetBuffer {
                        buffer: ParquetTypeStructs::default_for_type(&parquet_type),
                        buffer_size_bytes: 0,   
                        max_size: self.internal_buffer_size_bytes,// maybe not needed ?
                        current_batch_metadata: metadata.clone(), // update this before
                        upload_interval: self.poll_interval, // not sure if needed here
                    }
                });

            
            // Calculate the current batch size, we can probably replace with Sizeable trait
            let curr_batch_size_bytes: usize = match &parquet_data {
                ParquetTypeStructs::MoveResource(data) => allocative::size_of_unique(data),
                ParquetTypeStructs::WriteSetChange(data) => allocative::size_of_unique(data),
                ParquetTypeStructs::Transaction(data) => allocative::size_of_unique(data),
                ParquetTypeStructs::TableItem(data) => allocative::size_of_unique(data),
                ParquetTypeStructs::MoveModule(data) => allocative::size_of_unique(data),
            };
            
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
                metadata.total_size_in_bytes = buffer.buffer_size_bytes as u64;
                buffer.buffer_size_bytes = 0; // Reset buffer size
                file_uploaded = true;
            }

            // Append new data to the buffer
            buffer.buffer_size_bytes += curr_batch_size_bytes;
            match (&mut buffer.buffer, parquet_data) {
                (ParquetTypeStructs::MoveResource(buf), ParquetTypeStructs::MoveResource(mut data)) => buf.append(&mut data),
                (ParquetTypeStructs::WriteSetChange(buf), ParquetTypeStructs::WriteSetChange(mut data)) => buf.append(&mut data),
                (ParquetTypeStructs::Transaction(buf), ParquetTypeStructs::Transaction(mut data)) => buf.append(&mut data),
                (ParquetTypeStructs::TableItem(buf), ParquetTypeStructs::TableItem(mut data)) => buf.append(&mut data),
                (ParquetTypeStructs::MoveModule(buf), ParquetTypeStructs::MoveModule(mut data)) => buf.append(&mut data),
                _ => return Err(ProcessorError::ProcessError {
                               message: format!("Failed to upload buffer"),
                   })
            };

            println!(
                "Updated buffer size for {:?}: {} bytes",
                parquet_type, buffer.buffer_size_bytes
            );

            // If file was uploaded, return metadata update
            // TODO:  if multiple files were uploaded, we should return mulitple TransactionContext. 
            if file_uploaded {
                println!("File uploaded for {:?}. Returning updated metadata.", parquet_type);

                return Ok(Some(TransactionContext { data: (), metadata }));
            }
        }

        // Return None if no upload occurred
        Ok(None)
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        Ok(None)
    }
}

impl<U> SizeBufferStep<U>
where
    U: Uploadable + Send + 'static  + Sync,
{
    pub fn new(
        poll_interval: Duration,
        buffer_uploader: U
    ) -> Self {
        Self {
            internal_buffers: HashMap::new(),
            internal_buffer_size_bytes: 0,
            poll_interval,
            buffer_uploader,
        }
    }

}


#[async_trait]
impl<U> PollableAsyncStep for SizeBufferStep<U>
    where
        U: Uploadable + Send + 'static  + Sync,
{
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        Ok(None)
    }
}

impl<U> NamedStep for SizeBufferStep<U>
where
    U: Uploadable + Send + 'static  + Sync,
{
    fn name(&self) -> String {
        "DefaultTimedSizeBuffer".to_string()
    }
}
