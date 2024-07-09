// // Copyright © Aptos Foundation
// // SPDX-License-Identifier: Apache-2.0

use crate::gap_detectors::{GapDetectorResult, GapDetectorTrait, ProcessingResult};
use ahash::AHashMap;
use anyhow::Result;
use std::cmp::max;
use tracing::{debug, info};

pub struct ParquetFileGapDetector {
    next_version_to_process: i64,
    version_counters: AHashMap<i64, i64>,
    max_version: i64,
}

pub struct ParquetFileGapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub start_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

impl ParquetFileGapDetector {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version as i64,
            version_counters: AHashMap::new(),
            max_version: 0,
        }
    }
}
impl GapDetectorTrait for ParquetFileGapDetector {
    fn process_versions(&mut self, result: ProcessingResult) -> Result<GapDetectorResult> {
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
            self.max_version = max(self.max_version, *version);

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

        Ok(GapDetectorResult::ParquetFileGapDetectorResult(
            ParquetFileGapDetectorResult {
                next_version_to_process: self.next_version_to_process as u64,
                num_gaps: (self.max_version - self.next_version_to_process) as u64,
                start_version: result.start_version as u64,
                last_transaction_timestamp: result.last_transaction_timestamp,
            },
        ))
    }
}
