// // Copyright © Aptos Foundation
// // SPDX-License-Identifier: Apache-2.0

use crate::gap_detectors::{GapDetectorResult, GapDetectorTrait, ProcessingResult};
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use std::{
    cmp::max,
    sync::{Arc, Mutex},
};
use tracing::info;

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
        for (version, count) in txn_version_to_struct_count.iter() {
            if !self.version_counters.contains_key(version) {
                self.version_counters.insert(*version, *count);
            } else {
                // there is an edge case where file gets uploaded before it gets processed here, so we need to add the count to the existing count.
                *self.version_counters.get_mut(version).unwrap() += *count;
            }

            self.max_version = max(self.max_version, *version);
        }
        self.update_next_version_to_process(start_version, end_version);
    }

    /// This function updates the next version to process based on the current version counters.
    /// It will increment the next version to process if the current version is fully processed.
    /// It will also remove the version from the version counters if it is fully processed.
    /// what it means to be fully processed is that all the structs for that version processed, i.e. count = 0.
    pub fn update_next_version_to_process(&mut self, start_version: i64, end_version: i64) {
        let mut current_version = start_version;

        while current_version <= end_version {
            match self.version_counters.get_mut(&current_version) {
                Some(count) => {
                    if *count == 0 && current_version == self.next_version_to_process {
                        while let Some(&count) =
                            self.version_counters.get(&self.next_version_to_process)
                        {
                            if count == 0 {
                                info!("Version {} fully processed. Next version to process updated to {}", self.next_version_to_process, self.next_version_to_process + 1);
                                self.version_counters.remove(&self.next_version_to_process); // Remove the fully processed version
                                self.seen_versions.insert(self.next_version_to_process);
                                self.next_version_to_process += 1;
                            } else {
                                break;
                            }
                        }
                    }
                },
                None => {
                    // TODO: validate this that we shouldn't reach this b/c we already added default count.
                    if self.seen_versions.contains(&current_version) {
                        info!(
                            "Version {} already processed, skipping and current next_version{} ",
                            current_version, self.next_version_to_process
                        );
                        self.next_version_to_process =
                            max(self.next_version_to_process, current_version + 1);
                    } else {
                        // this is the case where we haven't updated the map yet, while the file gets uploaded first. the bigger file size we will have,
                        // the less chance we will see this as upload takes longer time. And map population is done before the upload.
                        info!(
                            current_version = current_version,
                            "No struct count found for version, it will be processed later"
                        );
                    }
                },
            }
            current_version += 1; // Move to the next version in sequence
        }
    }
}

impl GapDetectorTrait for ParquetFileGapDetectorInner {
    fn process_versions(&mut self, result: ProcessingResult) -> Result<GapDetectorResult> {
        let result = match result {
            ProcessingResult::ParquetProcessingResult(r) => r,
            _ => panic!("Invalid result type"),
        };
        let parquet_processed_transactions = result
            .parquet_processed_structs
            .context("Missing parquet processed transactions")?;

        for (version, count) in parquet_processed_transactions.iter() {
            if let Some(entry) = self.version_counters.get_mut(version) {
                *entry -= count;
            } else {
                // if not hasn't been updated, we can populate this count with the negative value and pass through
                self.version_counters.insert(*version, -count);
            }
        }

        self.update_next_version_to_process(result.start_version, result.end_version);

        Ok(GapDetectorResult::ParquetFileGapDetectorResult(
            ParquetFileGapDetectorResult {
                next_version_to_process: self.next_version_to_process as u64,
                num_gaps: (self.max_version - self.next_version_to_process) as u64,
                last_transaction_timestamp: result.last_transaction_timestamp,
            },
        ))
    }
}
