// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::processors::ProcessingResult;
use ahash::AHashMap;

// Number of batches processed before gap detected
pub const GAP_DETECTION_BATCH_COUNT: u64 = 50;

pub struct GapDetector {
    starting_version: u64,
    seen_versions: AHashMap<u64, ProcessingResult>,
    maybe_prev_batch: Option<ProcessingResult>,
}

#[derive(Debug, PartialEq)]
pub enum GapDetectorResult {
    GapDetected {
        gap_start_version: u64,
    },
    NoGapDetected {
        last_success_batch: Option<ProcessingResult>,
        num_batches_processed_without_gap: u64,
    },
}

impl GapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            starting_version,
            seen_versions: AHashMap::new(),
            maybe_prev_batch: None,
        }
    }

    pub fn process_versions(
        &mut self,
        result: ProcessingResult,
    ) -> anyhow::Result<GapDetectorResult> {
        // Check for gaps
        let gap_detected = self.detect_gap(result.clone());

        let mut num_batches_processed_without_gap = 0;
        if gap_detected {
            self.seen_versions.insert(result.start_version, result);
            tracing::debug!("Gap detected");

            // It detects a gap if it processed GAP_DETECTION_BATCH_COUNT batches without finding (maybe_prev_batch.end_version + 1)
            if self.seen_versions.len() as u64 >= GAP_DETECTION_BATCH_COUNT {
                let gap_start_version = if let Some(prev_batch) = &self.maybe_prev_batch {
                    prev_batch.end_version + 1
                } else {
                    self.starting_version
                };
                return Ok(GapDetectorResult::GapDetected { gap_start_version });
            }
        } else {
            // If no gap is detected, find the latest processed batch without gaps
            num_batches_processed_without_gap = self.update_prev_batch(result);
            tracing::debug!("No gap detected");
        }

        Ok(GapDetectorResult::NoGapDetected {
            last_success_batch: self.maybe_prev_batch.clone(),
            num_batches_processed_without_gap,
        })
    }

    fn detect_gap(&self, result: ProcessingResult) -> bool {
        if let Some(prev_batch) = &self.maybe_prev_batch {
            prev_batch.end_version + 1 != result.start_version
        } else {
            result.start_version != self.starting_version
        }
    }

    fn update_prev_batch(&mut self, result: ProcessingResult) -> u64 {
        let mut new_prev_batch = result;
        let mut num_batches_processed_without_gap = 1;
        while let Some(next_version) = self.seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
            num_batches_processed_without_gap += 1;
        }
        self.maybe_prev_batch = Some(new_prev_batch);
        num_batches_processed_without_gap
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn detect_gap_test() {
        let starting_version = 0;
        let mut gap_detector = GapDetector::new(starting_version);

        for i in 0..GAP_DETECTION_BATCH_COUNT - 1 {
            let result = ProcessingResult {
                start_version: 100 + i * 100,
                end_version: 199 + i * 100,
                last_transaction_timstamp: None,
                processing_duration_in_secs: 0.0,
                db_insertion_duration_in_secs: 0.0,
            };
            let gap_detector_result = gap_detector.process_versions(result).unwrap();
            match gap_detector_result {
                GapDetectorResult::NoGapDetected {
                    last_success_batch: _,
                    num_batches_processed_without_gap: _,
                } => {},
                _ => panic!("No gap should be detected"),
            }
        }
        let gap_detectgor_result = gap_detector.process_versions(ProcessingResult {
            start_version: 100 + (GAP_DETECTION_BATCH_COUNT - 1) * 100,
            end_version: 199 + (GAP_DETECTION_BATCH_COUNT - 1) * 100,
            last_transaction_timstamp: None,
            processing_duration_in_secs: 0.0,
            db_insertion_duration_in_secs: 0.0,
        });
        match gap_detectgor_result {
            Ok(GapDetectorResult::GapDetected { gap_start_version }) => {
                assert_eq!(gap_start_version, 0);
            },
            _ => panic!("Gap should be detected"),
        }

        let gap_detector_result = gap_detector.process_versions(ProcessingResult {
            start_version: 0,
            end_version: 99,
            last_transaction_timstamp: None,
            processing_duration_in_secs: 0.0,
            db_insertion_duration_in_secs: 0.0,
        });
        match gap_detector_result {
            Ok(GapDetectorResult::NoGapDetected {
                last_success_batch: _,
                num_batches_processed_without_gap,
            }) => {
                assert_eq!(
                    num_batches_processed_without_gap,
                    GAP_DETECTION_BATCH_COUNT + 1
                );
            },
            _ => panic!("No gap should be detected"),
        }
    }
}
