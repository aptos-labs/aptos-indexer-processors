use crate::{
    bq_analytics::ParquetProcessingResult,
    gap_detectors::{
        gap_detector::{DefaultGapDetector, DefaultGapDetectorResult, GapDetectorTrait},
        parquet_gap_detector::{ParquetFileGapDetector, ParquetFileGapDetectorResult},
    },
    processors::{DefaultProcessingResult, Processor, ProcessorTrait},
    utils::counters::{PARQUET_PROCESSOR_DATA_GAP_COUNT, PROCESSOR_DATA_GAP_COUNT},
    worker::PROCESSOR_SERVICE_TYPE,
};
use enum_dispatch::enum_dispatch;
use kanal::AsyncReceiver;
use tracing::{error, info};

pub mod gap_detector;
pub mod parquet_gap_detector;

// Size of a gap (in txn version) before gap detected
pub const DEFAULT_GAP_DETECTION_BATCH_SIZE: u64 = 500;
// Number of seconds between each processor status update
const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

#[enum_dispatch(GapDetectorTrait)]
pub enum GapDetector {
    DefaultGapDetector,
    ParquetFileGapDetector,
}

#[enum_dispatch(GapDetectorTrait)]
pub enum GapDetectorResult {
    DefaultGapDetectorResult,
    ParquetFileGapDetectorResult,
}
pub enum ProcessingResult {
    DefaultProcessingResult(DefaultProcessingResult),
    ParquetProcessingResult(ParquetProcessingResult),
}

pub async fn create_gap_detector_status_tracker_loop(
    gap_detector_receiver: AsyncReceiver<ProcessingResult>,
    processor: Processor,
    starting_version: u64,
    gap_detection_batch_size: u64,
) {
    let processor_name = processor.name();
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        "[Parser] Starting gap detector task",
    );

    let mut default_gap_detector = DefaultGapDetector::new(starting_version);
    let mut parquet_gap_detector = ParquetFileGapDetector::new(starting_version);
    let mut last_update_time = std::time::Instant::now();
    loop {
        match gap_detector_receiver.recv().await {
            Ok(ProcessingResult::DefaultProcessingResult(result)) => {
                match default_gap_detector
                    .process_versions(ProcessingResult::DefaultProcessingResult(result))
                {
                    Ok(res) => {
                        match res {
                            GapDetectorResult::DefaultGapDetectorResult(res) => {
                                PROCESSOR_DATA_GAP_COUNT
                                    .with_label_values(&[processor_name])
                                    .set(res.num_gaps as i64);
                                if res.num_gaps >= gap_detection_batch_size {
                                    tracing::debug!(
                                    processor_name,
                                    gap_start_version = res.next_version_to_process,
                                    num_gaps = res.num_gaps,
                                    "[Parser] Processed {gap_detection_batch_size} batches with a gap",
                                );
                                    // We don't panic as everything downstream will panic if it doesn't work/receive
                                }
                                if let Some(res_last_success_batch) = res.last_success_batch {
                                    if last_update_time.elapsed().as_secs()
                                        >= UPDATE_PROCESSOR_STATUS_SECS
                                    {
                                        processor
                                            .update_last_processed_version(
                                                res_last_success_batch.end_version,
                                                res_last_success_batch
                                                    .last_transaction_timestamp
                                                    .clone(),
                                            )
                                            .await
                                            .unwrap();
                                        last_update_time = std::time::Instant::now();
                                    }
                                }
                            },
                            _ => {
                                panic!("Invalid result type");
                            },
                        }
                    },
                    Err(e) => {
                        error!(
                        processor_name,
                        service_type = PROCESSOR_SERVICE_TYPE,
                        error = ?e,
                        "[Parser] Gap detector task has panicked"
                        );
                        panic!("[Parser] Gap detector task has panicked: {:?}", e);
                    },
                }
            },
            Ok(ProcessingResult::ParquetProcessingResult(result)) => {
                info!(
                    processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    "[ParquetGapDetector] received parquet gap detector task",
                );
                match parquet_gap_detector
                    .process_versions(ProcessingResult::ParquetProcessingResult(result))
                {
                    Ok(res) => {
                        match res {
                            GapDetectorResult::ParquetFileGapDetectorResult(res) => {
                                PARQUET_PROCESSOR_DATA_GAP_COUNT
                                    .with_label_values(&[processor_name])
                                    .set(res.num_gaps as i64);
                                // we need a new gap detection batch size
                                if res.num_gaps >= gap_detection_batch_size {
                                    tracing::debug!(
                                    processor_name,
                                        gap_start_version = res.next_version_to_process,
                                        num_gaps = res.num_gaps,
                                        "[Parser] Processed {gap_detection_batch_size} batches with a gap",
                                        );
                                    // We don't panic as everything downstream will panic if it doesn't work/receive
                                }

                                if let Some(res_last_success_batch) = res.last_success_batch {
                                    if last_update_time.elapsed().as_secs()
                                        >= UPDATE_PROCESSOR_STATUS_SECS
                                    {
                                        tracing::info!("Updating last processed version");
                                        processor
                                            .update_last_processed_version(
                                                res_last_success_batch.end_version as u64,
                                                res_last_success_batch
                                                    .last_transaction_timestamp
                                                    .clone(),
                                            )
                                            .await
                                            .unwrap();
                                        last_update_time = std::time::Instant::now();
                                    } else {
                                        tracing::info!("Not Updating last processed version");
                                    }
                                }
                            },
                            _ => {
                                panic!("Invalid result type");
                            },
                        }
                    },
                    Err(e) => {
                        error!(
                            processor_name,
                            service_type = PROCESSOR_SERVICE_TYPE,
                            error = ?e,
                            "[Parser] Gap detector task has panicked"
                        );
                        panic!("[Parser] Gap detector task has panicked: {:?}", e);
                    },
                }
            },
            Err(e) => {
                info!(
                    processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?e,
                    "[Parser] Gap detector channel has been closed",
                );
                return;
            },
        };
    }
}
