// // Copyright © Aptos Foundation
// // SPDX-License-Identifier: Apache-2.0

use crate::{
    gap_detectors::{gap_detector::GapDetectorTrait, GapDetectorResult, ProcessingResult},
    parquet_processors::ParquetProcessingResult,
};
use ahash::AHashMap;
use tracing::{debug, info};

pub struct ParquetFileGapDetector {
    next_version_to_process: i64,
    seen_versions: AHashMap<i64, ParquetProcessingResult>,
    last_success_batch: Option<ParquetProcessingResult>,
    version_counters: AHashMap<i64, i64>,
}

pub struct ParquetFileGapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_success_batch: Option<ParquetProcessingResult>,
}

impl ParquetFileGapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version as i64,
            seen_versions: AHashMap::new(),
            last_success_batch: None,
            version_counters: AHashMap::new(),
        }
    }
}
impl GapDetectorTrait for ParquetFileGapDetector {
    fn process_versions(&mut self, result: ProcessingResult) -> anyhow::Result<GapDetectorResult> {
        // Update counts of structures for each transaction version
        let result = match result {
            ProcessingResult::ParquetProcessingResult(r) => r,
            _ => panic!("Invalid result type"),
        };
        for (version, count) in result.txn_version_to_struct_count.iter() {
            if !self.version_counters.contains_key(version) {
                // info!("Inserting version {} with count {} into parquet gap detector", version, count);
                self.version_counters.insert(*version, *count);
            }

            *self.version_counters.entry(*version).or_default() -= 1;
        }

        // Update next version to process and move forward
        let mut current_version = result.start_version;
        while current_version <= result.end_version {
            match self.version_counters.get_mut(&current_version) {
                Some(count) => {
                    if *count == 0 && current_version == self.next_version_to_process {
                        while let Some(&count) =
                            self.version_counters.get(&self.next_version_to_process)
                        {
                            if count == 0 {
                                self.version_counters.remove(&self.next_version_to_process); // Remove the fully processed version
                                self.next_version_to_process += 1; // Increment to the next version
                                info!("Version {} fully processed. Next version to process updated to {}", self.next_version_to_process - 1, self.next_version_to_process);
                            } else {
                                break;
                            }
                        }
                    }
                },
                None => {
                    // TODO: validate this that we shouldn't reach this b/c we already added default count.
                    // or it could mean that we have duplicates.
                    debug!("No struct count found for version {}", current_version);
                },
            }
            current_version += 1; // Move to the next version in sequence
        }

        if current_version == result.end_version {
            debug!("No gap detected");
        } else {
            self.seen_versions.insert(current_version, result);
            debug!("Gap detected");
        }

        Ok(GapDetectorResult::ParquetFileGapDetectorResult(
            ParquetFileGapDetectorResult {
                next_version_to_process: self.next_version_to_process as u64,
                num_gaps: self.seen_versions.len() as u64,
                last_success_batch: self.last_success_batch.clone(),
            },
        ))
    }
}

// TODO: add tests
