// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    models::processor_status::ProcessorStatus,
    processors::ProcessedVersions,
    schema::processor_status,
    utils::{
        database::{execute_with_better_error, PgDbPool},
        util::parse_timestamp,
    },
};
use ahash::AHashMap;
use diesel::{upsert::excluded, ExpressionMethods};

// Number of batches processed before gap detected
pub const GAP_DETECTION_BATCH_COUNT: u64 = 50;
// Number of batches to process before updating processor status
const PROCESSOR_STATUS_UPDATE_BATCH_COUNT: u64 = 10;

pub struct GapDetector {
    processor_name: String,
    db_pool: PgDbPool,
    starting_version: u64,
    seen_versions: AHashMap<u64, ProcessedVersions>,
    maybe_prev_batch: Option<ProcessedVersions>,
    num_batches_processed_without_gap: u64,
    num_batches_processed_with_gap: u64,
}

#[derive(Debug, PartialEq)]
pub enum GapDetectorResult {
    GapDetected { gap_start_version: u64 },
    NoGapDetected(),
}

impl GapDetector {
    pub fn new(processor_name: String, db_pool: PgDbPool, starting_version: u64) -> Self {
        Self {
            processor_name,
            db_pool,
            starting_version,
            seen_versions: AHashMap::new(),
            maybe_prev_batch: None,
            num_batches_processed_without_gap: 0,
            num_batches_processed_with_gap: 0,
        }
    }

    pub async fn process_versions(
        &mut self,
        result: ProcessedVersions,
    ) -> anyhow::Result<GapDetectorResult> {
        // Check for gaps
        let gap_detected = self.detect_gap(result.clone());

        if gap_detected {
            self.seen_versions.insert(result.start_version, result);
            self.num_batches_processed_with_gap += 1;
            tracing::debug!("Gap detected");
        } else {
            // If no gap is detected, find the latest processed batch without gaps
            self.update_prev_batch(result);
            self.num_batches_processed_with_gap = 0;
            tracing::debug!("No gap detected");
        }

        // If there's a gap detected, panic.
        // A gap is detected if we've processed GAP_DETECTION_BATCH_COUNT batches without seeing the batch that comes after prev_batch
        if self.num_batches_processed_with_gap >= GAP_DETECTION_BATCH_COUNT {
            let gap_start_version = if let Some(prev_batch) = &self.maybe_prev_batch {
                prev_batch.end_version + 1
            } else {
                self.starting_version
            };
            return Ok(GapDetectorResult::GapDetected { gap_start_version });
        }

        // Check if need to update processor status
        if self.num_batches_processed_without_gap >= PROCESSOR_STATUS_UPDATE_BATCH_COUNT {
            let prev_batch = self.maybe_prev_batch.clone().unwrap();
            self.update_processor_status(
                prev_batch.end_version,
                prev_batch.last_transaction_timstamp,
            )
            .await;
            self.num_batches_processed_without_gap = 0;
        }

        Ok(GapDetectorResult::NoGapDetected())
    }

    fn detect_gap(&self, result: ProcessedVersions) -> bool {
        if let Some(prev_batch) = &self.maybe_prev_batch {
            prev_batch.end_version + 1 != result.start_version
        } else {
            result.start_version != self.starting_version
        }
    }

    fn update_prev_batch(&mut self, result: ProcessedVersions) {
        let mut new_prev_batch = result;
        self.num_batches_processed_without_gap += 1;
        while let Some(next_version) = self.seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
            self.num_batches_processed_without_gap += 1;
        }
        self.maybe_prev_batch = Some(new_prev_batch);
    }

    async fn update_processor_status(
        &self,
        last_success_version: u64,
        last_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    ) {
        let timestamp =
            last_transaction_timestamp.map(|t| parse_timestamp(&t, last_success_version as i64));
        let status = ProcessorStatus {
            processor: self.processor_name.clone(),
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
    use crate::utils::database::new_db_pool;

    #[tokio::test]
    async fn detect_gap_test() {
        let starting_version = 0;
        let db_pool = new_db_pool("postgres://test", None)
            .await
            .expect("Failed to create connection pool");
        let mut gap_detector = GapDetector::new("test".to_string(), db_pool, starting_version);

        for i in 0..GAP_DETECTION_BATCH_COUNT - 1 {
            let result = ProcessedVersions {
                start_version: 100 + i * 100,
                end_version: 199 + i * 100,
                last_transaction_timstamp: None,
            };
            let gap_detector_result = gap_detector.process_versions(result).await.unwrap();
            match gap_detector_result {
                GapDetectorResult::NoGapDetected() => {},
                _ => panic!("No gap should be detected"),
            }
        }
        let gap_detectgor_result = gap_detector
            .process_versions(ProcessedVersions {
                start_version: 100 + (GAP_DETECTION_BATCH_COUNT - 1) * 100,
                end_version: 199 + (GAP_DETECTION_BATCH_COUNT - 1) * 100,
                last_transaction_timstamp: None,
            })
            .await;
        match gap_detectgor_result {
            Ok(GapDetectorResult::GapDetected { gap_start_version }) => {
                assert_eq!(gap_start_version, 0);
            },
            _ => panic!("Gap should be detected"),
        }
    }
}
