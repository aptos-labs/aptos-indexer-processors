// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    processors::{Processor, ProcessorTrait},
    utils::counters::PROCESSOR_DATA_GAP_COUNT,
    worker::PROCESSOR_SERVICE_TYPE,
};
use ahash::AHashMap;
use kanal::AsyncReceiver;
use tracing::{error, info};

// Number of seconds between each processor status update
const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub struct VersionTracker {
    next_version_to_process: u64,
    processed_versions_map: AHashMap<u64, InternalVersionTrackerItem>,
    last_success_version: Option<u64>,
    last_success_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    num_tables: u64,
}

#[derive(Clone)]
pub enum VersionTrackerItem {
    // VersionTracker can mark this batch of txn's as completely processed
    CompleteBatch(CompleteBatch),
    /// Internally, VersionTracker counts the number of times it receives a PartialBatch. Once the count of partial batches reaches the
    /// number of tables we expect to be written, VersionTracker can mark the versions as completely processed. The config.yaml will
    /// need to set number_db_tables.
    PartialBatch(PartialBatch),
}

#[derive(Clone)]
pub struct CompleteBatch {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

#[derive(Clone)]
pub struct PartialBatch {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

struct InternalVersionTrackerItem {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub processed_count: u64,
}

pub struct VersionTrackerResult {
    pub next_version_to_process: u64,
    pub num_gaps: u64,
    pub last_success_version: Option<u64>,
    pub last_success_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
}

impl VersionTracker {
    pub fn new(starting_version: u64, num_tables: u64) -> Self {
        Self {
            next_version_to_process: starting_version,
            processed_versions_map: AHashMap::new(),
            last_success_timestamp: None,
            last_success_version: None,
            num_tables,
        }
    }

    pub fn process_versions(
        &mut self,
        item: VersionTrackerItem,
    ) -> anyhow::Result<VersionTrackerResult> {
        match item {
            VersionTrackerItem::CompleteBatch(batch) => {
                if batch.start_version < self.next_version_to_process {
                    return Err(anyhow::anyhow!(
                        "Received a CompleteBatch with start_version {} that is less than the next version to process {}",
                        batch.start_version,
                        self.next_version_to_process,
                    ));
                }

                self.processed_versions_map.insert(
                    batch.start_version,
                    InternalVersionTrackerItem {
                        start_version: batch.start_version,
                        end_version: batch.end_version,
                        last_transaction_timestamp: batch.last_transaction_timestamp,
                        processed_count: self.num_tables, // Use the max num_tables
                    },
                );
            },
            VersionTrackerItem::PartialBatch(batch) => {
                if batch.start_version < self.next_version_to_process {
                    return Err(anyhow::anyhow!(
                        "Received a PartialBatch with start_version {} that is less than the next version to process {}",
                        batch.start_version,
                        self.next_version_to_process,
                    ));
                }

                if let Some(tracker_item) =
                    self.processed_versions_map.get_mut(&batch.start_version)
                {
                    // We assume that all batches with the same start_version have the same end_version
                    tracker_item.processed_count += 1;

                    if tracker_item.processed_count > self.num_tables {
                        return Err(anyhow::anyhow!(
                            "Batch with start_version {} has been processed {} times, which is greater than the number of tables {}",
                            tracker_item.start_version,
                            tracker_item.processed_count,
                            self.num_tables,
                        ));
                    }
                } else {
                    self.processed_versions_map.insert(
                        batch.start_version,
                        InternalVersionTrackerItem {
                            start_version: batch.start_version,
                            end_version: batch.end_version,
                            last_transaction_timestamp: batch.last_transaction_timestamp,
                            processed_count: 1,
                        },
                    );
                }
            },
        }

        self.update_latest_batch_processed();

        Ok(VersionTrackerResult {
            next_version_to_process: self.next_version_to_process,
            num_gaps: self.processed_versions_map.len() as u64,
            last_success_version: self.last_success_version,
            last_success_timestamp: self.last_success_timestamp.clone(),
        })
    }

    fn update_latest_batch_processed(&mut self) {
        let mut current_starting_version = self.next_version_to_process;
        while let Some(current_item) = self.processed_versions_map.get(&current_starting_version) {
            if current_item.processed_count == self.num_tables {
                // This batch is fully processed. Update the latest version tracked
                self.next_version_to_process = current_item.end_version + 1;
                self.last_success_version = Some(current_item.end_version);
                self.last_success_timestamp
                    .clone_from(&current_item.last_transaction_timestamp);
            } else {
                // The next batch to process has only been partially processed, so there's a gap
                break;
            }

            // There's no gap yet, so update the map and pointer
            self.processed_versions_map
                .remove(&current_starting_version);
            current_starting_version = self.next_version_to_process;
        }
    }
}

pub async fn create_version_tracker_loop(
    version_tracker_receiver: AsyncReceiver<VersionTrackerItem>,
    processor: Processor,
    starting_version: u64,
    num_db_tables: u64,
) {
    let processor_name = processor.name();
    info!(
        processor_name = processor_name,
        start_version = starting_version,
        num_db_tables,
        service_type = PROCESSOR_SERVICE_TYPE,
        "[Parser] Starting version tracker task",
    );

    let mut version_tracker = VersionTracker::new(starting_version, num_db_tables);
    let mut last_update_time = std::time::Instant::now();

    loop {
        let result = match version_tracker_receiver.recv().await {
            Ok(result) => result,
            Err(e) => {
                info!(
                    processor_name,
                    service_type = PROCESSOR_SERVICE_TYPE,
                    error = ?e,
                    "[Parser] Version tracker channel has been closed",
                );
                return;
            },
        };

        match version_tracker.process_versions(result) {
            Ok(res) => {
                PROCESSOR_DATA_GAP_COUNT
                    .with_label_values(&[processor_name])
                    .set(res.num_gaps as i64);

                // Update the processor status
                if let Some(res_last_success_version) = res.last_success_version {
                    if last_update_time.elapsed().as_secs() >= UPDATE_PROCESSOR_STATUS_SECS {
                        processor
                            .update_last_processed_version(
                                res_last_success_version,
                                res.last_success_timestamp,
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
                    "[Parser] Version tracker task has panicked"
                );
                panic!("[Parser] Version tracker task has panicked: {:?}", e);
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn track_versions_complete_batch_test() {
        let starting_version = 0;
        let mut version_tracker = VersionTracker::new(starting_version, 1);
        let num_batches = 10;
        // Processing batches with a gap
        for i in 1..=num_batches {
            let item = VersionTrackerItem::CompleteBatch(CompleteBatch {
                start_version: i * 100,
                end_version: i * 100 + 99,
                last_transaction_timestamp: None,
            });
            let result = version_tracker.process_versions(item).unwrap();

            tracing::info!(i);
            // Assert that latest success version is unchanged
            assert_eq!(result.num_gaps, i);
            assert_eq!(result.next_version_to_process, 0);
            assert_eq!(result.last_success_version, None);
        }

        // Process 1 batch without a gap
        let result = version_tracker
            .process_versions(VersionTrackerItem::CompleteBatch(CompleteBatch {
                start_version: 0,
                end_version: 99,
                last_transaction_timestamp: None,
            }))
            .unwrap();

        assert_eq!(result.num_gaps, 0);
        assert_eq!(result.next_version_to_process, (num_batches + 1) * 100);
        assert_eq!(
            result.last_success_version.unwrap(),
            (num_batches) * 100 + 99
        );
    }

    #[test]
    fn track_versions_partial_batch_test() {
        let starting_version = 0;
        let num_tables = 5;
        let mut version_tracker = VersionTracker::new(starting_version, num_tables);

        // Processing (num_tables - 1) partial batches per version batch
        for i in 0..10 {
            for _ in 0..(num_tables - 1) {
                let item = VersionTrackerItem::PartialBatch(PartialBatch {
                    start_version: i * 100,
                    end_version: i * 100 + 99,
                    last_transaction_timestamp: None,
                });
                let result = version_tracker.process_versions(item).unwrap();

                // Assert that latest success version is unchanged
                assert_eq!(result.num_gaps, i + 1);
                assert_eq!(result.next_version_to_process, 0);
                assert_eq!(result.last_success_version, None);
            }
        }

        // Process each version batch one more time
        for i in 0..10 {
            let item = VersionTrackerItem::PartialBatch(PartialBatch {
                start_version: i * 100,
                end_version: i * 100 + 99,
                last_transaction_timestamp: None,
            });

            let result = version_tracker.process_versions(item).unwrap();

            // Assert that latest success version is updated every time
            assert_eq!(result.num_gaps, 10 - i - 1);
            assert_eq!(result.next_version_to_process, (i + 1) * 100);
            assert_eq!(result.last_success_version.unwrap(), i * 100 + 99);
        }
    }
}
