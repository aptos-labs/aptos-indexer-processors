use crate::parquet_processors::parquet_default_processor::ExtractResources;
use allocative::Allocative;
use anyhow::{Result};
use aptos_indexer_processor_sdk::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use google_cloud_storage::client::Client as GCSClient;
use aptos_indexer_processor_sdk::traits::parquet_extract_trait::{GetTimeStamp, HasVersion, NamedTable, HasParquetSchema};
use parquet::{record::RecordWriter};
use std::{marker::PhantomData, sync::Arc, time::Duration};

pub struct TableConfig {
    pub table_name: String,
    pub bucket_name: String,
    pub bucket_root: String,
    pub max_size: usize,
}

#[async_trait]
pub trait BufferHandler<ParquetType> {
    async fn handle_buffer(
        &mut self,
        gcs_client: &GCSClient,
        buffer: Vec<ParquetType>,
        metadata: &mut TransactionMetadata,
    ) -> Result<(), ProcessorError>;
}

pub struct TimedSizeBufferStep<Input, ParquetType, B>
where
    Input: Send + 'static + Sized,
    B: BufferHandler<ParquetType> + Send + 'static + Sync,
    ParquetType: NamedTable + HasVersion + HasParquetSchema + 'static + Allocative + Send + Sync,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    pub internal_buffer: Vec<ParquetType>,
    pub internal_buffer_size_bytes: usize,
    pub poll_interval: Duration,
    pub table_config: TableConfig,
    pub gcs_client: Arc<GCSClient>,
    pub processor_name: String,
    pub buffer_handler: B,
    _marker: PhantomData<Input>, // Use PhantomData to indicate that Input is relevant
}

#[async_trait]
impl<Input, ParquetType, B> Processable for TimedSizeBufferStep<Input, ParquetType, B>
where
    Input: Send + Sync + 'static + Sized + ExtractResources<ParquetType>,
    B: BufferHandler<ParquetType> + Send + 'static + Sync,
    ParquetType: Allocative
        + GetTimeStamp
        + HasVersion
        + HasParquetSchema
        + 'static
        + NamedTable
        + Send
        + Sync,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    type Input = Input;
    type Output = ();
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        // grab the correct parquetType from the Input. match on the Input type = parquetType somehow?
        let mut parquet_types = item.data.extract();

        let mut metadata = item.metadata;
        let mut file_uploaded = false;

        // calculate the size of the current batch
        let curr_batch_size_bytes: usize = parquet_types
            .iter()
            .map(|parquet_type| allocative::size_of_unique(parquet_type))
            .sum();

        if curr_batch_size_bytes + self.internal_buffer_size_bytes > self.table_config.max_size {
            // if the current batch size + the internal buffer size exceeds the max size, upload the internal buffer
            let struct_buffer = std::mem::take(&mut self.internal_buffer);
            self.buffer_handler
                .handle_buffer(&self.gcs_client, struct_buffer, &mut metadata)
                .await?;
            metadata.total_size_in_bytes = self.internal_buffer_size_bytes as u64;
            self.internal_buffer_size_bytes = 0;
            file_uploaded = true;
        }

        self.internal_buffer.append(&mut parquet_types);

        Ok(match file_uploaded {
            true => Some(TransactionContext { data: (), metadata }),
            false => None,
        })
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        // Ok(None)
        let mut metadata = TransactionMetadata::default();
        let struct_buffer = std::mem::take(&mut self.internal_buffer);
        self.buffer_handler
            .handle_buffer(&self.gcs_client, struct_buffer, &mut metadata)
            .await?;
        metadata.total_size_in_bytes = self.internal_buffer_size_bytes as u64;
        self.internal_buffer_size_bytes = 0;

        Ok(Some(vec![TransactionContext { data: (), metadata }]))
    }
}

impl<Input, ParquetType, B> TimedSizeBufferStep<Input, ParquetType, B>
where
    Input: Send + 'static + Sized,
    B: BufferHandler<ParquetType> + Send + 'static + Sync,
    ParquetType: NamedTable
        + HasVersion
        + HasParquetSchema
        + 'static
        + Allocative
        + Send
        + Sync
        + GetTimeStamp,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    pub fn new(
        poll_interval: Duration,
        table_config: TableConfig,
        gcs_client: Arc<GCSClient>,
        processor_name: &str,
        buffer_handler: B,
    ) -> Self {
        Self {
            internal_buffer: Vec::new(),
            internal_buffer_size_bytes: 0,
            poll_interval,
            table_config,
            gcs_client,
            _marker: PhantomData,
            buffer_handler,
            processor_name: processor_name.to_string(),
        }
    }
}

#[async_trait]
impl<Input, ParquetType, B> PollableAsyncStep for TimedSizeBufferStep<Input, ParquetType, B>
where
    Input: Send + Sync + 'static + Sized + ExtractResources<ParquetType>,
    B: BufferHandler<ParquetType> + Send + 'static + Sync,
    ParquetType: Allocative
        + GetTimeStamp
        + HasVersion
        + HasParquetSchema
        + 'static
        + NamedTable
        + Send
        + Sync,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        let mut metadata = TransactionMetadata::default();
        let struct_buffer = std::mem::take(&mut self.internal_buffer);
        self.buffer_handler
            .handle_buffer(&self.gcs_client, struct_buffer, &mut metadata)
            .await?;
        metadata.total_size_in_bytes = self.internal_buffer_size_bytes as u64;
        self.internal_buffer_size_bytes = 0;

        Ok(Some(vec![TransactionContext { data: (), metadata }]))
    }
}

impl<Input, ParquetType, B> NamedStep for TimedSizeBufferStep<Input, ParquetType, B>
where
    Input: Send + Sync + 'static + Sized + ExtractResources<ParquetType>,
    B: BufferHandler<ParquetType> + Send + 'static + Sync,
    ParquetType: Allocative
        + GetTimeStamp
        + HasVersion
        + HasParquetSchema
        + 'static
        + NamedTable
        + Send
        + Sync,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn name(&self) -> String {
        format!("TimedSizeBuffer: {}", self.table_config.table_name)
    }
}
