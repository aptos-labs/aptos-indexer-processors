// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    gap_detectors::{GapDetectorResult, GapDetectorTrait, ProcessingResult},
    processors::DefaultProcessingResult,
};
use ahash::AHashMap;
use anyhow::Result;

#[derive(Clone)]
pub struct DefaultGapDetector {
    next_version_to_process: u64,
    seen_versions: AHashMap<u64, DefaultProcessingResult>,
    last_success_batch: Option<DefaultProcessingResult>,
}

pub struct DefaultGapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_success_batch: Option<DefaultProcessingResult>,
}

impl GapDetectorTrait for DefaultGapDetector {
    fn process_versions(&mut self, result: ProcessingResult) -> Result<GapDetectorResult> {
        match result {
            ProcessingResult::DefaultProcessingResult(result) => {
                // Check for gaps
                if self.next_version_to_process != result.start_version {
                    self.seen_versions.insert(result.start_version, result);
                    tracing::debug!("Gap detected");
                } else {
                    // If no gap is detected, find the latest processed batch without gaps
                    self.update_prev_batch(result);
                    tracing::debug!("No gap detected");
                }

                Ok(GapDetectorResult::DefaultGapDetectorResult(
                    DefaultGapDetectorResult {
                        next_version_to_process: self.next_version_to_process,
                        num_gaps: self.seen_versions.len() as u64,
                        last_success_batch: self.last_success_batch.clone(),
                    },
                ))
            },
            _ => {
                panic!("Invalid result type");
            },
        }
    }
}

impl DefaultGapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version,
            seen_versions: AHashMap::new(),
            last_success_batch: None,
        }
    }

    fn update_prev_batch(&mut self, result: DefaultProcessingResult) {
        let mut new_prev_batch = result;
        while let Some(next_version) = self.seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
        }
        self.next_version_to_process = new_prev_batch.end_version + 1;
        self.last_success_batch = Some(new_prev_batch);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::gap_detectors::DEFAULT_GAP_DETECTION_BATCH_SIZE;

    #[tokio::test]
    async fn detect_gap_test() {
        let starting_version = 0;
        let mut default_gap_detector = DefaultGapDetector::new(starting_version);

        // Processing batches with gaps
        for i in 0..DEFAULT_GAP_DETECTION_BATCH_SIZE {
            let result = DefaultProcessingResult {
                start_version: 100 + i * 100,
                end_version: 199 + i * 100,
                last_transaction_timestamp: None,
                processing_duration_in_secs: 0.0,
                db_insertion_duration_in_secs: 0.0,
            };
            let default_gap_detector_result = default_gap_detector
                .process_versions(ProcessingResult::DefaultProcessingResult(result))
                .unwrap();
            let default_gap_detector_result = match default_gap_detector_result {
                GapDetectorResult::DefaultGapDetectorResult(res) => res,
                _ => panic!("Invalid result type"),
            };

            assert_eq!(default_gap_detector_result.num_gaps, i + 1);
            assert_eq!(default_gap_detector_result.next_version_to_process, 0);
            assert_eq!(default_gap_detector_result.last_success_batch, None);
        }

        // Process a batch without a gap
        let default_gap_detector_result = default_gap_detector
            .process_versions(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version: 0,
                    end_version: 99,
                    last_transaction_timestamp: None,
                    processing_duration_in_secs: 0.0,
                    db_insertion_duration_in_secs: 0.0,
                },
            ))
            .unwrap();
        let default_gap_detector_result = match default_gap_detector_result {
            GapDetectorResult::DefaultGapDetectorResult(res) => res,
            _ => panic!("Invalid result type"),
        };
        assert_eq!(default_gap_detector_result.num_gaps, 0);
        assert_eq!(
            default_gap_detector_result.next_version_to_process,
            100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE) * 100
        );
        assert_eq!(
            default_gap_detector_result
                .last_success_batch
                .clone()
                .unwrap()
                .start_version,
            100 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        );
        assert_eq!(
            default_gap_detector_result
                .last_success_batch
                .unwrap()
                .end_version,
            199 + (DEFAULT_GAP_DETECTION_BATCH_SIZE - 1) * 100
        );
    }
}
