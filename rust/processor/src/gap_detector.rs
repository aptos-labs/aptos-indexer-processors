// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    models::processor_status::ProcessorStatus,
    processors::ProcessedVersions,
    schema::processor_status,
    utils::{
        database::{execute_with_better_error, get_conn, new_db_pool, PgDbPool},
        util::parse_timestamp,
    },
};
use ahash::AHashMap;
use diesel::{upsert::excluded, ExpressionMethods};
use futures::StreamExt;
use tracing::{error, info};

// Number of batches processed before gap detected
const GAP_DETECTION_BATCH_COUNT: u64 = 50;
// Number of batches to process before updating processor status
const PROCESSOR_STATUS_UPDATE_BATCH_COUNT: u64 = 10;

pub struct GapDetector {
    receiver: tokio::sync::mpsc::Receiver<ProcessedVersions>,
    db_pool: PgDbPool,
    processor_name: String,
    starting_version: u64,
}

pub struct GapDetectorProcessedVersions {
    pub start_version: u64,
    pub end_version: u64,
    pub last_transaction_timstamp: Option<String>,
    // The number of processed txn batches that are represented by this struct
    pub num_batches: u64,
}

impl GapDetector {
    pub fn new(
        receiver: tokio::sync::mpsc::Receiver<ProcessedVersions>,
        db_pool: PgDbPool,
        processor_name: String,
        starting_version: u64,
    ) -> Self {
        Self {
            receiver,
            db_pool,
            processor_name,
            starting_version,
        }
    }

    pub async fn run(&mut self) {
        // Keep track of the start versions we've seen
        let mut seen_versions = AHashMap::new();
        // Keep track of the latest batch processed without gaps
        let mut maybe_prev_batch: Option<ProcessedVersions> = None;
        // Counter of how many batches have been processed with no gaps
        let mut num_batches_processed_without_gap = 0;
        // Counter of how many batches have been processed with a gap from prev_end
        let mut num_batches_processed_with_gap = 0;

        loop {
            let result = match self.receiver.recv().await {
                Some(result) => result,
                None => {
                    error!(
                        processor_name = self.processor_name,
                        "[Parser] Gap detector channel has been closed"
                    );
                    panic!("[Parser] Gap detector channel has been closed");
                },
            };

            // Check for gaps
            let gap_detected = Self::detect_gap(
                maybe_prev_batch.clone(),
                self.starting_version,
                result.clone(),
            );

            if gap_detected {
                seen_versions.insert(result.start_version, result);
                num_batches_processed_with_gap += 1;
            } else {
                let (new_prev_batch, batches_processed) =
                    Self::get_next_prev_batch(&mut seen_versions, result);
                maybe_prev_batch = Some(new_prev_batch);
                num_batches_processed_without_gap += batches_processed;
                num_batches_processed_with_gap = 0;
            }

            // // If maybe_prev_batch is None, then the first batch should be the one with starting_version

            // // Check for gaps
            // if let Some(prev_batch) = maybe_prev_batch.clone() {
            //     // If result isn't the first version processed, check for gaps
            //     if prev_batch.end_version + 1 != result.start_version {
            //         seen_versions.insert(result.start_version, result);
            //         num_batches_processed_with_gap += 1;
            //         info!(
            //             processor_name = processor_name,
            //             gap_start_version = prev_batch.end_version + 1,
            //             num_batches_processed_with_gap,
            //             "[Parser] Detected a gap"
            //         );
            //     } else {
            //         let mut new_prev_batch = result;
            //         num_batches_processed_without_gap += 1;
            //         while let Some(next_batch_processed) =
            //             seen_versions.remove(&(new_prev_batch.end_version + 1))
            //         {
            //             new_prev_batch = next_batch_processed;
            //             num_batches_processed_without_gap += 1;
            //         }
            //         maybe_prev_batch = Some(new_prev_batch);
            //         num_batches_processed_with_gap = 0;
            //         info!(
            //             processor_name = processor_name,
            //             num_batches_processed_without_gap, "[Parser] No gaps detected"
            //         );
            //     }
            // } else {
            //     // The first version processed should be equal to starting_version
            //     if result.start_version == starting_version {
            //         maybe_prev_batch = Some(result);
            //         num_batches_processed_without_gap += 1;
            //         num_batches_processed_with_gap = 0;

            //         info!(
            //             processor_name = processor_name,
            //             num_batches_processed_without_gap, "[Parser] No gaps detected"
            //         )
            //     } else {
            //         // If previous or next batch is in seen_version, condense the batches
            //         seen_versions.insert(result.start_version, result);
            //         num_batches_processed_with_gap += 1;
            //         info!(
            //             processor_name = processor_name,
            //             gap_start_version = starting_version,
            //             num_batches_processed_with_gap,
            //             "[Parser] Detected a gap"
            //         );
            //     }
            // }

            // If there's a gap detected, panic.
            // A gap is detected if we've processed GAP_DETECTION_BATCH_COUNT batches without seeing the batch that comes after prev_batch
            if num_batches_processed_with_gap >= GAP_DETECTION_BATCH_COUNT {
                let gap_start_version = if let Some(prev_batch) = maybe_prev_batch {
                    prev_batch.end_version + 1
                } else {
                    self.starting_version
                };
                error!(
                    processor_name = self.processor_name,
                    gap_start_version,
                    "[Parser] Processed {GAP_DETECTION_BATCH_COUNT} batches with a gap. Panicking."
                );
                panic!(
                    "[Parser] Processed {GAP_DETECTION_BATCH_COUNT} batches with a gap. Panicking."
                );
            }

            // Check if need to update processor status
            if num_batches_processed_without_gap >= PROCESSOR_STATUS_UPDATE_BATCH_COUNT {
                let prev_batch = maybe_prev_batch.clone().unwrap();
                self.update_processor_status(
                    self.processor_name.clone(),
                    prev_batch.end_version,
                    prev_batch.last_transaction_timstamp,
                )
                .await;
                num_batches_processed_without_gap = 0;
            }
        }
    }

    fn detect_gap(
        maybe_prev_batch: Option<ProcessedVersions>,
        starting_version: u64,
        result: ProcessedVersions,
    ) -> bool {
        let mut gap_detected = false;
        if let Some(prev_batch) = maybe_prev_batch.clone() {
            if prev_batch.end_version + 1 == result.start_version {
                gap_detected = false;
            } else {
                gap_detected = true;
            }
        } else if result.start_version == starting_version {
            gap_detected = false;
        } else {
            gap_detected = true;
        }
        gap_detected
    }

    fn get_next_prev_batch(
        seen_versions: &mut AHashMap<u64, ProcessedVersions>,
        result: ProcessedVersions,
    ) -> (ProcessedVersions, u64) {
        let mut new_prev_batch = result;
        let mut num_batches_processed_without_gap = 1;
        while let Some(next_batch_processed) =
            seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_batch_processed;
            num_batches_processed_without_gap += 1;
        }
        (new_prev_batch, num_batches_processed_without_gap)
    }

    async fn update_processor_status(
        &self,
        processor_name: String,
        last_success_version: u64,
        last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    ) {
        let timestamp =
            last_transaction_timestamp.map(|t| parse_timestamp(&t, last_success_version as i64));
        let status = ProcessorStatus {
            processor: processor_name.clone(),
            last_success_version: last_success_version as i64,
            last_transaction_timestamp: timestamp,
        };
        execute_with_better_error(
            self.db_pool.clone(),
            diesel::insert_into(processor_status::table)
                .values(&status)
                .on_conflict(processor_status::processor)
                .do_update()
                .set((
                    processor_status::last_success_version
                        .eq(excluded(processor_status::last_success_version)),
                    processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                    processor_status::last_transaction_timestamp
                        .eq(excluded(processor_status::last_transaction_timestamp)),
                )),
            Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
        )
        .await
        .expect("[Parser] Error updating processor status");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // #[tokio::test]
    // async fn test_create_gap_detector_no_gap() {
    //     let (tx, rx) = tokio::sync::mpsc::channel(100);
    //     let db_pool = new_db_pool("postgres://test", 1)
    //         .await
    //         .expect("Failed to create connection pool");
    //     let processor_name = "test_processor".to_string();
    //     let starting_version = 0;
    //     let gap_detector_task =
    //         create_gap_detector_task(rx, db_pool, processor_name, starting_version);
    //     for i in 0..GAP_DETECTION_BATCH_COUNT {
    //         let result = ProcessedVersions {
    //             start_version: i * 100,
    //             end_version: i * 100 + 99,
    //             last_transaction_timstamp: None,
    //         };
    //         tx.send(result).await.unwrap();
    //     }
    //     gap_detector_task.await;
    // }

    // #[tokio::test]
    // #[should_panic(expected = "batches with a gap. Panicking.")]
    // async fn test_create_gap_detector_with_gap() {
    //     let (tx, rx) = tokio::sync::mpsc::channel(100);
    //     let db_pool = new_db_pool("postgres://test", 1)
    //         .await
    //         .expect("Failed to create connection pool");
    //     let processor_name = "test_processor".to_string();
    //     let starting_version = 0;
    //     let gap_detector_task =
    //         create_gap_detector_task(rx, db_pool, processor_name, starting_version);
    //     for i in 0..GAP_DETECTION_BATCH_COUNT {
    //         let result = ProcessedVersions {
    //             start_version: 100 + i * 100,
    //             end_version: 100 + i * 100 + 99,
    //             last_transaction_timstamp: None,
    //         };
    //         tx.send(result).await.unwrap();
    //     }
    //     gap_detector_task.await;
    // }
}
