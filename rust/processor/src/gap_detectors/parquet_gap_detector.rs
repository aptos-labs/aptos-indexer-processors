// // Copyright © Aptos Foundation
// // SPDX-License-Identifier: Apache-2.0

use crate::gap_detectors::{GapDetectorResult, GapDetectorTrait, ProcessingResult};
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use std::{
    cmp::{max, min},
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

        // b/c of the case where file gets uploaded first, we should check if we have to update last_success_version for this processor
        self.update_next_version_to_process(
            min(self.next_version_to_process, end_version),
            "all_table",
        );
    }

    /// This function updates the next version to process based on the current version counters.
    /// It will increment the next version to process if the current version is fully processed.
    /// It will also remove the version from the version counters if it is fully processed.
    /// what it means to be fully processed is that all the structs for that version processed, i.e. count = 0.
    /// Note that for tables other than transactions, it won't be always the latest txn version since we update this value with
    /// Thus we will keep the latest version_to_process in the db with the min(max version of latest table files per processor)
    /// that has been uploaded to GCS. so whenever we restart the processor, it may generate some duplicates rows, and we are okay with that.
    pub fn update_next_version_to_process(&mut self, end_version: i64, table_name: &str) {
        // this has to start checking with this value all the time, since this is the value that will be stored in the db as well.
        // maybe there could be an improvement to be more performant. but priortizing the data integrity as of now.
        let mut current_version = self.next_version_to_process;

        while current_version <= end_version {
            #[allow(clippy::collapsible_else_if)]
            if self.version_counters.contains_key(&current_version) {
                while let Some(&count) = self.version_counters.get(&current_version) {
                    if current_version > end_version {
                        // we shouldn't update further b/c we haven't uploaded the files containing versions after end_version.
                        break;
                    }
                    if count == 0 {
                        self.version_counters.remove(&current_version);
                        self.seen_versions.insert(current_version); // seen_version holds the txns version that we have processed already
                        current_version += 1;
                        self.next_version_to_process += 1;
                    } else {
                        break;
                    }
                }
            } else {
                if self.seen_versions.contains(&current_version) {
                    debug!(
                        "Version {} already processed, skipping and current next_version {} ",
                        current_version, self.next_version_to_process
                    );
                    self.next_version_to_process =
                        max(self.next_version_to_process, current_version + 1);
                } else {
                    // this is the case where we haven't updated the map yet, while the file gets uploaded first. the bigger file size we will have,
                    // the less chance we will see this as upload takes longer time. And map population is done before the upload.
                    debug!(
                        current_version = current_version,
                        "No struct count found for version. This shouldn't happen b/c we already added default count for this version."
                    );
                }
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
        let parquet_processed_structs = result
            .parquet_processed_structs
            .context("Missing parquet processed transactions")?;
        info!(
            start_version = result.start_version,
            end_version = result.end_version,
            "Parquet file has been uploaded."
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
