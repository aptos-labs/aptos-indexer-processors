// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    parquet_manager::ParquetProcessingResult, processors::{Processor, ProcessorTrait}, utils::counters::{PARQUET_PROCESSOR_DATA_GAP_COUNT}, worker::PROCESSOR_SERVICE_TYPE
};
use ahash::AHashMap;
use kanal::AsyncReceiver;
use tracing::{error, info};

// Size of a gap (in txn version) before gap detected
pub const DEFAULT_GAP_DETECTION_BATCH_SIZE: u64 = 1;
// Number of seconds between each processor status update
const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub struct ParquetFileGapDetector {
    next_version_to_process: u64,
    seen_versions: AHashMap<u64, ParquetProcessingResult>,
    last_success_batch: Option<ParquetProcessingResult>,
    version_counters: AHashMap<u64, i64>,
}

pub struct ParquetFileGapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_success_batch: Option<ParquetProcessingResult>,
}

impl ParquetFileGapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version,
            seen_versions: AHashMap::new(),
            last_success_batch: None,
            version_counters: AHashMap::new(),
        }
    }

    pub fn process_versions(
        &mut self,
        result: ParquetProcessingResult,
    ) -> anyhow::Result<ParquetFileGapDetectorResult> {
        // Update counts of structures for each transaction version
        // 
        for (version, count) in result.txn_version_to_struct_count.iter() {
            if !self.version_counters.contains_key(version) {
                self.version_counters.insert(*version, (*count as i64).try_into().unwrap());
            } 
            
            println!("version: {}, count: {}", version, count);
            *self.version_counters.entry(*version).or_default() -= 1;

            println!("version: {}, count: {}", version, self.version_counters.get(version).unwrap());
        }
        
        // update next version to process and move forward
        let mut current_version = result.start_version;
        while let Some(&mut current_count) = self.version_counters.get_mut(&current_version) {
            if current_count == 0 {
                self.next_version_to_process = current_version + 1;
                self.version_counters.remove(&current_version);
            }
            current_version += 1;
        }
        if current_version == result.end_version {
            tracing::debug!("No gap detected");
        } else {
            self.seen_versions.insert(current_version, result);
            tracing::debug!("Gap detected");
        }

        Ok(ParquetFileGapDetectorResult {
            next_version_to_process: self.next_version_to_process,
            num_gaps: self.seen_versions.len() as u64,
            last_success_batch: self.last_success_batch.clone(),
        })
    }

}

pub async fn create_parquet_file_gap_detector_status_tracker_loop(
    parquet_gap_detector_receiver: AsyncReceiver<ParquetProcessingResult>,
    processor: Processor,
    starting_version: u64,
    gap_detection_batch_size: u64,
) {
    let processor_name: &str = processor.name();
    info!(
        processor_name = processor_name,
        service_type = PROCESSOR_SERVICE_TYPE,
        "[Parquet handler] Starting gap detector task",
    );

    let mut parquet_gap_detector: ParquetFileGapDetector = ParquetFileGapDetector::new(starting_version);
    let mut last_update_time = std::time::Instant::now();

    loop {
        let result = match parquet_gap_detector_receiver.recv().await {
            Ok(result) => result,
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

        match parquet_gap_detector.process_versions(result) {
            Ok(res) => {
                PARQUET_PROCESSOR_DATA_GAP_COUNT
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
                    if last_update_time.elapsed().as_secs() >= UPDATE_PROCESSOR_STATUS_SECS {
                        processor
                            .update_last_processed_version(
                                res_last_success_batch.end_version,
                                res_last_success_batch.last_transaction_timestamp.clone(),
                            )
                            .await
                            .unwrap();
                        last_update_time = std::time::Instant::now();
                    }
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
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn detect_gap_test() {
        let starting_version = 0;
        let mut gap_detector = ParquetFileGapDetector::new(starting_version);

        // pub start_version: u64,
        // pub end_version: u64,
        // pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
        // pub parquet_insertion_duration_in_secs: Option<f64>,
        // pub txn_version_to_struct_count: AHashMap<u64, u64>,
        let mut txn_version_to_struct_count = AHashMap::new();
        txn_version_to_struct_count.insert(0, 1);
        txn_version_to_struct_count.insert(1, 1);
        txn_version_to_struct_count.insert(2, 1);
        txn_version_to_struct_count.insert(3, 1);
        txn_version_to_struct_count.insert(103, 5);

        // Processing batches with gaps
        // for i in 0..DEFAULT_GAP_DETECTION_BATCH_SIZE {
            let result = ParquetProcessingResult {
                start_version: 0,
                end_version: 104,
                last_transaction_timestamp: None,
                parquet_insertion_duration_in_secs: 0.0,
                txn_version_to_struct_count
            };
            let gap_detector_result = gap_detector.process_versions(result).unwrap();
            // assert_eq!(gap_detector_result.num_gaps, i + 1);
            assert_eq!(gap_detector_result.next_version_to_process, 4);
            // assert_eq!(gap_detector_result.last_success_batch, None);
        // }

        // // Process a batch without a gap
        // let gap_detector_result = gap_detector
        //     .process_versions(ParquetProcessingResult {
        //         start_version: 0,
        //         end_version: 99,
        //         last_transaction_timestamp: None,
        //         processing_duration_in_secs: 0.0,
        //         db_insertion_duration_in_secs: 0.0,
        //     })
        //     .unwrap();
        // assert_eq!(gap_detector_result.num_gaps, 0);
        // assert_eq!(
        //     gap_detector_result.next_version_to_process,
        //     100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE) * 100
        // );
        // assert_eq!(
        //     gap_detector_result
        //         .last_success_batch
        //         .clone()
        //         .unwrap()
        //         .start_version,
        //     100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        // );
        // assert_eq!(
        //     gap_detector_result.last_success_batch.unwrap().end_version,
        //     199 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        // );
    }
}
