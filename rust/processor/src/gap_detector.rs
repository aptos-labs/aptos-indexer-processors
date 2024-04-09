// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::{ProcessingResult, Processor, ProcessorTrait},
    utils::counters::PROCESSOR_DATA_GAP_COUNT,
    worker::PROCESSOR_SERVICE_TYPE,
};
use ahash::AHashMap;
use kanal::AsyncReceiver;
use tracing::{error, info};

// Size of a gap (in txn version) before gap detected
pub const DEFAULT_GAP_DETECTION_BATCH_SIZE: u64 = 500;
// Number of seconds between each processor status update
const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub struct GapDetector {
    next_version_to_process: u64,
    seen_versions: AHashMap<u64, ProcessingResult>,
    last_success_batch: Option<ProcessingResult>,
}

pub struct GapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_success_batch: Option<ProcessingResult>,
}

impl GapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version,
            seen_versions: AHashMap::new(),
            last_success_batch: None,
        }
    }

    pub fn process_versions(
        &mut self,
        result: ProcessingResult,
    ) -> anyhow::Result<GapDetectorResult> {
        // Check for gaps
        if self.next_version_to_process != result.start_version {
            self.seen_versions.insert(result.start_version, result);
            tracing::debug!("Gap detected");
        } else {
            // If no gap is detected, find the latest processed batch without gaps
            self.update_prev_batch(result);
            tracing::debug!("No gap detected");
        }

        Ok(GapDetectorResult {
            next_version_to_process: self.next_version_to_process,
            num_gaps: self.seen_versions.len() as u64,
            last_success_batch: self.last_success_batch.clone(),
        })
    }

    fn update_prev_batch(&mut self, result: ProcessingResult) {
        let mut new_prev_batch = result;
        while let Some(next_version) = self.seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
        }
        self.next_version_to_process = new_prev_batch.end_version + 1;
        self.last_success_batch = Some(new_prev_batch);
    }
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

    let mut gap_detector = GapDetector::new(starting_version);
    let mut last_update_time = std::time::Instant::now();

    loop {
        let result = match gap_detector_receiver.recv().await {
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

        match gap_detector.process_versions(result) {
            Ok(res) => {
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
        let mut gap_detector = GapDetector::new(starting_version);

        // Processing batches with gaps
        for i in 0..DEFAULT_GAP_DETECTION_BATCH_SIZE {
            let result = ProcessingResult {
                start_version: 100 + i * 100,
                end_version: 199 + i * 100,
                last_transaction_timestamp: None,
                processing_duration_in_secs: 0.0,
                db_insertion_duration_in_secs: 0.0,
            };
            let gap_detector_result = gap_detector.process_versions(result).unwrap();
            assert_eq!(gap_detector_result.num_gaps, i + 1);
            assert_eq!(gap_detector_result.next_version_to_process, 0);
            assert_eq!(gap_detector_result.last_success_batch, None);
        }

        // Process a batch without a gap
        let gap_detector_result = gap_detector
            .process_versions(ProcessingResult {
                start_version: 0,
                end_version: 99,
                last_transaction_timestamp: None,
                processing_duration_in_secs: 0.0,
                db_insertion_duration_in_secs: 0.0,
            })
            .unwrap();
        assert_eq!(gap_detector_result.num_gaps, 0);
        assert_eq!(
            gap_detector_result.next_version_to_process,
            100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE) * 100
        );
        assert_eq!(
            gap_detector_result
                .last_success_batch
                .clone()
                .unwrap()
                .start_version,
            100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        );
        assert_eq!(
            gap_detector_result.last_success_batch.unwrap().end_version,
            199 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        );
    }
}
