// // Copyright © Aptos Foundation
// // SPDX-License-Identifier: Apache-2.0

use crate::gap_detectors::{GapDetectorResult, GapDetectorTrait, ProcessingResult};
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use std::{
    cmp::max,
    sync::{Arc, Mutex},
};
use tracing::{debug, info};

impl GapDetectorTrait for Arc<Mutex<ParquetFileGapDetectorInner>> {
    fn process_versions(&mut self, result: ProcessingResult) -> Result<GapDetectorResult> {
        let mut detector = self.lock().unwrap();
        detector.process_versions(result)
    }
}

#[derive(Clone)]
pub struct ParquetFileGapDetectorInner {
    next_version_to_process: i64,
    version_counters: AHashMap<i64, i64>,
    seen_versions: AHashSet<i64>,
    max_version: i64,
}

pub struct ParquetFileGapDetectorResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

impl ParquetFileGapDetectorInner {
    pub fn new(starting_version: u64) -> Self {
        Self {
            next_version_to_process: starting_version as i64,
            version_counters: AHashMap::new(),
            seen_versions: AHashSet::new(),
            max_version: 0,
        }
    }

    pub fn update_struct_map(
        &mut self,
        txn_version_to_struct_count: AHashMap<i64, i64>,
        start_version: i64,
        end_version: i64,
    ) {
        for version in start_version..=end_version {
            if let Some(count) = txn_version_to_struct_count.get(&version) {
                if self.version_counters.contains_key(&version) {
                    *self.version_counters.get_mut(&version).unwrap() += *count;
                } else {
                    self.version_counters.insert(version, *count);
                }
            } else {
                // this is the case when file gets uploaded before it gets processed here.
                if !self.version_counters.contains_key(&version) {
                    self.version_counters.insert(version, 0);
                }
                // if there is version populated already, meaning that there are some other structs that are being processed for this version. we don't do anything
                // b/c we don't have any of the structs we are processing for this txn version.
            }
        }
        self.max_version = max(self.max_version, end_version);
    }

    /// This function updates the `next_version_to_process` based on the current version counters.
    /// It increments the `next_version_to_process` if the current version is fully processed, which means
    /// that all the structs for that version have been processed, i.e., `count = 0`.
    /// If a version is fully processed, it removes the version from the version counters and adds it to the `seen_versions`.
    /// For tables other than transactions, the latest version to process may not always be the most recent transaction version
    /// since this value is updated based on the minimum of the maximum versions of the latest table files per processor
    /// that have been uploaded to GCS. Therefore, when the processor restarts, some duplicate rows may be generated, which is acceptable.
    /// The function also ensures that the current version starts checking from the `next_version_to_process`
    /// value stored in the database. While there might be potential performance improvements,
    /// the current implementation prioritizes data integrity.
    /// The function also handles cases where a version is already processed or where no struct count
    /// is found for a version, providing appropriate logging for these scenarios.
    pub fn update_next_version_to_process(&mut self, end_version: i64, table_name: &str) {
        // this has to start checking with this value all the time, since this is the value that will be stored in the db as well.
        // maybe there could be an improvement to be more performant. but priortizing the data integrity as of now.
        let mut current_version = self.next_version_to_process;

        while current_version <= end_version {
            // If the current version has a struct count entry
            if let Some(&count) = self.version_counters.get(&current_version) {
                if count == 0 {
                    self.version_counters.remove(&current_version);
                    self.seen_versions.insert(current_version);
                    self.next_version_to_process += 1;
                } else {
                    // Stop processing if the version is not yet complete
                    break;
                }
            } else if self.seen_versions.contains(&current_version) {
                // If the version is already seen and processed
                debug!(
                    "Version {} already processed, skipping and current next_version {} ",
                    current_version, self.next_version_to_process
                );
                self.next_version_to_process =
                    max(self.next_version_to_process, current_version + 1);
            } else {
                // If the version is neither in seen_versions nor version_counters
                debug!(
                    current_version = current_version,
                    "No struct count found for version. This shouldn't happen b/c we already added default count for this version."
                );
            }

            current_version += 1;
        }

        debug!(
            next_version_to_process = self.next_version_to_process,
            table_name = table_name,
            "Updated the next_version_to_process.",
        );
    }
}

impl GapDetectorTrait for ParquetFileGapDetectorInner {
    fn process_versions(&mut self, result: ProcessingResult) -> Result<GapDetectorResult> {
        let result = match result {
            ProcessingResult::ParquetProcessingResult(r) => r,
            _ => panic!("Invalid result type"),
        };

        let parquet_processed_structs = result.parquet_processed_structs.unwrap_or_else(|| {
            info!("Interval duration has passed, but there are no structs to process.");
            AHashMap::new()
        });

        if result.start_version == -1 {
            // meaning we didn't really upload anything but we stil lwould like to update the map to reduce memory usage.
            self.update_next_version_to_process(self.max_version, &result.table_name);
            return Ok(GapDetectorResult::ParquetFileGapDetectorResult(
                ParquetFileGapDetectorResult {
                    next_version_to_process: self.next_version_to_process as u64,
                    num_gaps: (self.max_version - self.next_version_to_process) as u64,
                    last_transaction_timestamp: result.last_transaction_timestamp,
                },
            ));
        }

        info!(
            start_version = result.start_version,
            end_version = result.end_version,
            table_name = &result.table_name,
            "[Parquet Gap Detector] Processing versions after parquet file upload."
        );

        for (version, count) in parquet_processed_structs.iter() {
            if let Some(entry) = self.version_counters.get_mut(version) {
                *entry -= count;
            } else {
                //  if not hasn't been updated, we can populate this count with the negative value and pass through
                self.version_counters.insert(*version, -count);
            }
        }
        self.update_next_version_to_process(result.end_version, &result.table_name);

        Ok(GapDetectorResult::ParquetFileGapDetectorResult(
            ParquetFileGapDetectorResult {
                next_version_to_process: self.next_version_to_process as u64,
                num_gaps: (self.max_version - self.next_version_to_process) as u64,
                last_transaction_timestamp: result.last_transaction_timestamp,
            },
        ))
    }
}
