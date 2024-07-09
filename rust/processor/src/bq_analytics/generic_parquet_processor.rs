use super::ParquetProcessingResult;
use crate::{
    bq_analytics::gcs_handler::upload_parquet_to_gcs,
    gap_detectors::ProcessingResult,
    utils::{
        counters::{PARQUET_HANDLER_BUFFER_SIZE, PARQUET_STRUCT_SIZE},
        util::naive_datetime_to_timestamp,
    },
};
use ahash::AHashMap;
use allocative::Allocative;
use anyhow::{anyhow, Result};
use google_cloud_storage::client::Client as GCSClient;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
    schema::types::Type,
};
use std::{
    fs::{remove_file, rename, File},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};
use tokio::time::Duration;
use tracing::{debug, error, info};
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct ParquetDataGeneric<ParquetType> {
    pub data: Vec<ParquetType>,
    pub transaction_version_to_struct_count: AHashMap<i64, i64>,
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
    pub writer: SerializedFileWriter<File>,
    pub buffer: Vec<ParquetType>,
    pub buffer_size_bytes: usize,

    pub transaction_version_to_struct_count: AHashMap<i64, i64>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
    pub file_path: String,
    pub upload_interval: Duration,
    pub max_buffer_size: usize,
    pub last_upload_time: Instant,
}
fn create_new_writer(
    file_path: &str,
    schema: Arc<parquet::schema::types::Type>,
) -> Result<SerializedFileWriter<File>> {
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();
    let props_arc = Arc::new(props);
    let file: File = File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(file_path)?;

    Ok(SerializedFileWriter::new(
        file.try_clone()?,
        schema,
        props_arc,
    )?)
}

impl<ParquetType> ParquetHandler<ParquetType>
where
    ParquetType: Allocative + GetTimeStamp + HasVersion + HasParquetSchema + 'static + NamedTable,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    fn create_new_writer(&self) -> Result<SerializedFileWriter<File>> {
        let file_path = &self.file_path;
        create_new_writer(file_path, self.schema.clone())
    }

    fn close_writer(&mut self) -> Result<()> {
        let mut writer = self.create_new_writer()?;
        std::mem::swap(&mut self.writer, &mut writer);
        writer.close()?;
        Ok(())
    }

    pub fn new(
        bucket_name: String,
        bucket_root: String,
        gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
        schema: Arc<Type>,
        upload_interval: Duration,
        max_buffer_size: usize,
    ) -> Result<Self> {
        // had to append unique id to avoid concurrent write issues
        let file_path = format!("{}_{}.parquet", ParquetType::TABLE_NAME, Uuid::new_v4());
        let writer = create_new_writer(&file_path, schema.clone())?;

        Ok(Self {
            writer,
            buffer: Vec::new(),
            buffer_size_bytes: 0,
            transaction_version_to_struct_count: AHashMap::new(),
            bucket_name,
            bucket_root,
            gap_detector_sender,
            schema,
            file_path,
            upload_interval,
            max_buffer_size,
            last_upload_time: Instant::now(),
        })
    }

    pub async fn handle(
        &mut self,
        gcs_client: &GCSClient,
        changes: ParquetDataGeneric<ParquetType>,
    ) -> Result<()> {
        let parquet_structs = changes.data;
        self.transaction_version_to_struct_count
            .extend(changes.transaction_version_to_struct_count);

        for parquet_struct in parquet_structs {
            let size_of_struct = allocative::size_of_unique(&parquet_struct);
            PARQUET_STRUCT_SIZE
                .with_label_values(&[ParquetType::TABLE_NAME])
                .set(size_of_struct as i64);
            self.buffer_size_bytes += size_of_struct;
            self.buffer.push(parquet_struct);

            if self.buffer_size_bytes >= self.max_buffer_size {
                info!("Max buffer size reached, uploading to GCS.");
                if let Err(e) = self.upload_buffer(gcs_client).await {
                    error!("Failed to upload buffer: {}", e);
                    return Err(e);
                }
                self.last_upload_time = Instant::now();
            }

            if self.last_upload_time.elapsed() >= self.upload_interval {
                info!(
                    "Time has elapsed more than {} since last upload.",
                    self.upload_interval.as_secs()
                );
                if let Err(e) = self.upload_buffer(gcs_client).await {
                    error!("Failed to upload buffer: {}", e);
                    return Err(e);
                }
                self.last_upload_time = Instant::now();
            }
        }

        PARQUET_HANDLER_BUFFER_SIZE
            .with_label_values(&[ParquetType::TABLE_NAME])
            .set(self.buffer.len() as i64);
        Ok(())
    }

    pub async fn upload_buffer(&mut self, gcs_client: &GCSClient) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let start_version = self.buffer.first().unwrap().version();
        let last = self.buffer.last().unwrap();
        let end_version = last.version();
        let last_transaction_timestamp = naive_datetime_to_timestamp(last.get_timestamp());

        let txn_version_to_struct_count =
            process_struct_count_map(&self.buffer, &mut self.transaction_version_to_struct_count);

        let new_file_path: PathBuf = PathBuf::from(format!(
            "{}_{}.parquet",
            ParquetType::TABLE_NAME,
            Uuid::new_v4()
        ));
        rename(&self.file_path, &new_file_path)?; // this fixes an issue with concurrent file access issues

        let struct_buffer = std::mem::take(&mut self.buffer);

        let mut row_group_writer = self.writer.next_row_group()?;
        struct_buffer
            .as_slice()
            .write_to_row_group(&mut row_group_writer)
            .unwrap();
        row_group_writer.close()?;
        self.close_writer()?;

        debug!(
            table_name = ParquetType::TABLE_NAME,
            start_version = start_version,
            end_version = end_version,
            "Max buffer size reached, uploading to GCS."
        );
        let bucket_root = PathBuf::from(&self.bucket_root);
        let upload_result = upload_parquet_to_gcs(
            gcs_client,
            &new_file_path,
            ParquetType::TABLE_NAME,
            &self.bucket_name,
            &bucket_root,
        )
        .await;
        self.buffer_size_bytes = 0;
        remove_file(&new_file_path)?;

        match upload_result {
            Ok(_) => {
                let parquet_processing_result = ParquetProcessingResult {
                    start_version,
                    end_version,
                    last_transaction_timestamp: Some(last_transaction_timestamp),
                    txn_version_to_struct_count,
                };

                self.gap_detector_sender
                    .send(ProcessingResult::ParquetProcessingResult(
                        parquet_processing_result,
                    ))
                    .await
                    .expect("[Parser] Failed to send versions to gap detector");
            },
            Err(e) => {
                error!("Failed to upload file to GCS: {}", e);
                return Err(anyhow!("Failed to upload file to GCS: {}", e));
            },
        };

        Ok(())
    }
}

fn process_struct_count_map<ParquetType: NamedTable + HasVersion>(
    buffer: &[ParquetType],
    txn_version_to_struct_count: &mut AHashMap<i64, i64>,
) -> AHashMap<i64, i64> {
    let mut txn_version_to_struct_count_for_gap_detector = AHashMap::new();

    for item in buffer.iter() {
        let version = item.version();

        if let Some(count) = txn_version_to_struct_count.get(&(version)) {
            txn_version_to_struct_count_for_gap_detector.insert(version, *count);
            txn_version_to_struct_count.remove(&(version));
        }
    }
    txn_version_to_struct_count_for_gap_detector
}
