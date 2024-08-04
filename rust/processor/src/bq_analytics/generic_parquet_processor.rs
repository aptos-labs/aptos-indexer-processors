use super::ParquetProcessingResult;
use crate::{
    bq_analytics::gcs_handler::upload_parquet_to_gcs,
    gap_detectors::ProcessingResult,
    utils::{
        counters::{PARQUET_HANDLER_CURRENT_BUFFER_SIZE, PARQUET_STRUCT_SIZE},
        util::naive_datetime_to_timestamp,
    },
};
use ahash::AHashMap;
use allocative::Allocative;
use anyhow::{Context, Result};
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::time::Duration;
use tracing::{debug, error, info};

#[derive(Debug, Default, Clone)]
pub struct ParquetDataGeneric<ParquetType> {
    pub data: Vec<ParquetType>,
}

pub trait NamedTable {
    const TABLE_NAME: &'static str;
}

pub trait HasVersion {
    fn version(&self) -> i64;
}

pub trait HasParquetSchema {
    fn schema() -> Arc<parquet::schema::types::Type>;
}

pub trait GetTimeStamp {
    fn get_timestamp(&self) -> chrono::NaiveDateTime;
}

/// Auto-implement this for all types that implement `Default` and `RecordWriter`
impl<ParquetType> HasParquetSchema for ParquetType
where
    ParquetType: std::fmt::Debug + Default + Sync + Send,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn schema() -> Arc<Type> {
        let example: Self = Default::default();
        [example].as_slice().schema().unwrap()
    }
}

pub struct ParquetHandler<ParquetType>
where
    ParquetType: NamedTable + NamedTable + HasVersion + HasParquetSchema + 'static + Allocative,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    pub schema: Arc<Type>,
    pub writer: SerializedFileWriter<Vec<u8>>,
    pub buffer: Vec<ParquetType>,
    pub buffer_size_bytes: usize,

    pub transaction_version_to_struct_count: AHashMap<i64, i64>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
    pub upload_interval: Duration,
    pub max_buffer_size: usize,
    pub last_upload_time: Instant,
    pub processor_name: String,
}
fn create_new_writer(schema: Arc<Type>) -> Result<SerializedFileWriter<Vec<u8>>> {
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();
    let props_arc = Arc::new(props);

    SerializedFileWriter::new(Vec::new(), schema, props_arc).context("Failed to create new writer")
}

impl<ParquetType> ParquetHandler<ParquetType>
where
    ParquetType: Allocative + GetTimeStamp + HasVersion + HasParquetSchema + 'static + NamedTable,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn create_new_writer(&self) -> Result<SerializedFileWriter<Vec<u8>>> {
        create_new_writer(self.schema.clone())
    }

    fn close_writer(&mut self) -> Result<SerializedFileWriter<Vec<u8>>> {
        let new_writer = self.create_new_writer()?;
        let old_writer = std::mem::replace(&mut self.writer, new_writer);
        Ok(old_writer)
    }

    pub fn new(
        bucket_name: String,
        bucket_root: String,
        gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
        schema: Arc<Type>,
        upload_interval: Duration,
        max_buffer_size: usize,
        processor_name: String,
    ) -> Result<Self> {
        // had to append unique id to avoid concurrent write issues
        let writer = create_new_writer(schema.clone())?;

        Ok(Self {
            writer,
            buffer: Vec::new(),
            buffer_size_bytes: 0,
            transaction_version_to_struct_count: AHashMap::new(),
            bucket_name,
            bucket_root,
            gap_detector_sender,
            schema,
            upload_interval,
            max_buffer_size,
            last_upload_time: Instant::now(),
            processor_name,
        })
    }

    pub async fn handle(
        &mut self,
        gcs_client: &GCSClient,
        changes: ParquetDataGeneric<ParquetType>,
    ) -> Result<()> {
        let parquet_structs = changes.data;
        let processor_name = self.processor_name.clone();

        if self.last_upload_time.elapsed() >= self.upload_interval {
            info!(
                "Time has elapsed more than {} since last upload for {}",
                self.upload_interval.as_secs(),
                ParquetType::TABLE_NAME
            );
            if let Err(e) = self.upload_buffer(gcs_client).await {
                error!("Failed to upload buffer: {}", e);
                return Err(e);
            }
            self.last_upload_time = Instant::now();
        }

        for parquet_struct in parquet_structs {
            let size_of_struct = allocative::size_of_unique(&parquet_struct);
            PARQUET_STRUCT_SIZE
                .with_label_values(&[&processor_name, ParquetType::TABLE_NAME])
                .set(size_of_struct as i64);
            self.buffer_size_bytes += size_of_struct;
            self.buffer.push(parquet_struct);

            if self.buffer_size_bytes >= self.max_buffer_size {
                debug!(
                    table_name = ParquetType::TABLE_NAME,
                    buffer_size = self.buffer_size_bytes,
                    max_buffer_size = self.max_buffer_size,
                    "Max buffer size reached, uploading to GCS."
                );
                if let Err(e) = self.upload_buffer(gcs_client).await {
                    error!("Failed to upload buffer: {}", e);
                    return Err(e);
                }
                self.last_upload_time = Instant::now();
            }
        }

        PARQUET_HANDLER_CURRENT_BUFFER_SIZE
            .with_label_values(&[&self.processor_name, ParquetType::TABLE_NAME])
            .set(self.buffer_size_bytes as i64);

        Ok(())
    }

    async fn upload_buffer(&mut self, gcs_client: &GCSClient) -> Result<()> {
        // This is to cover the case when interval duration has passed but buffer is empty
        if self.buffer.is_empty() {
            debug!("Buffer is empty, skipping upload.");

            let parquet_processing_result = ParquetProcessingResult {
                start_version: -1, // this is to indicate that nothing was actually uploaded
                end_version: -1,
                last_transaction_timestamp: None,
                txn_version_to_struct_count: None,
                parquet_processed_structs: None,
                table_name: ParquetType::TABLE_NAME.to_string(),
            };

            self.gap_detector_sender
                .send(ProcessingResult::ParquetProcessingResult(
                    parquet_processing_result,
                ))
                .await
                .expect("[Parser] Failed to send versions to gap detector");

            return Ok(());
        }
        let start_version = self
            .buffer
            .first()
            .context("Buffer is not empty but has no first element")?
            .version();
        let last = self
            .buffer
            .last()
            .context("Buffer is not empty but has no last element")?;
        let end_version = last.version();
        let last_transaction_timestamp = naive_datetime_to_timestamp(last.get_timestamp());

        let parquet_processed_transactions = build_parquet_processed_transactions(&self.buffer);
        let struct_buffer = std::mem::take(&mut self.buffer);

        let mut row_group_writer = self
            .writer
            .next_row_group()
            .context("Failed to get row group")?;

        struct_buffer
            .as_slice()
            .write_to_row_group(&mut row_group_writer)
            .context("Failed to write to row group")?;
        row_group_writer
            .close()
            .context("Failed to close row group")?;

        let old_writer = self.close_writer().context("Failed to close writer")?;
        let upload_buffer = old_writer
            .into_inner()
            .context("Failed to get inner buffer")?;

        let bucket_root = PathBuf::from(&self.bucket_root);

        upload_parquet_to_gcs(
            gcs_client,
            upload_buffer,
            ParquetType::TABLE_NAME,
            &self.bucket_name,
            &bucket_root,
            self.processor_name.clone(),
        )
        .await?;

        self.buffer_size_bytes = 0;

        let parquet_processing_result = ParquetProcessingResult {
            start_version,
            end_version,
            last_transaction_timestamp: Some(last_transaction_timestamp),
            txn_version_to_struct_count: None,
            parquet_processed_structs: Some(parquet_processed_transactions),
            table_name: ParquetType::TABLE_NAME.to_string(),
        };
        info!(
            table_name = ParquetType::TABLE_NAME,
            start_version = start_version,
            end_version = end_version,
            "Uploaded parquet to GCS and sending result to gap detector."
        );
        self.gap_detector_sender
            .send(ProcessingResult::ParquetProcessingResult(
                parquet_processing_result,
            ))
            .await
            .expect("[Parser] Failed to send versions to gap detector");

        Ok(())
    }
}

fn build_parquet_processed_transactions<ParquetType: NamedTable + HasVersion>(
    buffer: &[ParquetType],
) -> AHashMap<i64, i64> {
    let mut txn_version_to_struct_count_for_gap_detector = AHashMap::new();

    for item in buffer.iter() {
        let version = item.version();

        txn_version_to_struct_count_for_gap_detector
            .entry(version)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    txn_version_to_struct_count_for_gap_detector
}
