use super::ParquetProcessingResult;
use crate::{
    bq_analytics::gcs_handler::upload_parquet_to_gcs,
    gap_detectors::ProcessingResult,
    utils::counters::{PARQUET_HANDLER_BUFFER_SIZE, PARQUET_STRUCT_SIZE},
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
};
use tracing::{debug, error};
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct ParquetDataGeneric<ParquetType> {
    pub data: Vec<ParquetType>,
    pub first_txn_version: u64,
    pub last_txn_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
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
    ParquetType: NamedTable + HasVersion + HasParquetSchema + 'static + Allocative,
    for<'a> &'a [ParquetType]: RecordWriter<ParquetType>,
{
    pub schema: Arc<parquet::schema::types::Type>,
    pub writer: SerializedFileWriter<File>,
    pub buffer: Vec<ParquetType>,
    pub buffer_size_bytes: usize,

    pub transaction_version_to_struct_count: AHashMap<i64, i64>,
    pub bucket_name: String,
    pub gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
    pub file_path: String,
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
    ParquetType: NamedTable + HasVersion + HasParquetSchema + 'static + Allocative,
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
        gap_detector_sender: kanal::AsyncSender<ProcessingResult>,
        schema: Arc<parquet::schema::types::Type>,
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
            gap_detector_sender,
            schema,
            file_path,
        })
    }

    pub async fn handle(
        &mut self,
        gcs_client: &GCSClient,
        changes: ParquetDataGeneric<ParquetType>,
        max_buffer_size: usize,
    ) -> Result<()> {
        let last_transaction_timestamp = changes.last_transaction_timestamp;
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
        }

        // for now, it's okay to go little above the buffer_size, given that we will keep max size as 200 MB
        if self.buffer_size_bytes >= max_buffer_size {
            let start_version = self.buffer.first().unwrap().version();
            let end_version = self.buffer.last().unwrap().version();

            let txn_version_to_struct_count = process_struct_count_map(
                &self.buffer,
                &mut self.transaction_version_to_struct_count,
            );

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
            let upload_result = upload_parquet_to_gcs(
                gcs_client,
                &new_file_path,
                ParquetType::TABLE_NAME,
                &self.bucket_name,
            )
            .await;
            self.buffer_size_bytes = 0;
            remove_file(&new_file_path)?;

            return match upload_result {
                Ok(_) => {
                    let parquet_processing_result = ParquetProcessingResult {
                        start_version,
                        end_version,
                        last_transaction_timestamp: last_transaction_timestamp.clone(),
                        txn_version_to_struct_count,
                    };

                    self.gap_detector_sender
                        .send(ProcessingResult::ParquetProcessingResult(
                            parquet_processing_result,
                        ))
                        .await
                        .expect("[Parser] Failed to send versions to gap detector");
                    Ok(())
                },
                Err(e) => {
                    error!("Failed to upload file to GCS: {}", e);
                    Err(anyhow!("Failed to upload file to GCS: {}", e))
                },
            };
        }

        PARQUET_HANDLER_BUFFER_SIZE
            .with_label_values(&[ParquetType::TABLE_NAME])
            .set(self.buffer.len() as i64);
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
