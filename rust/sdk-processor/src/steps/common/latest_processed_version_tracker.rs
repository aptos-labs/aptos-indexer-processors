use crate::utils::database::{execute_with_better_error, ArcDbPool};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::{errors::ProcessorError, time::parse_timestamp},
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};
use processor::{db::common::models::processor_status::ProcessorStatus, schema::processor_status};
use tracing::info;

const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub struct LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    conn_pool: ArcDbPool,
    tracker_name: String,
    // Next version to process that we expect.
    next_version: u64,
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<T>>,
    // Tracks all the versions that have been processed out of order.
    seen_versions: AHashMap<u64, TransactionContext<T>>,
}

impl<T> LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    pub fn new(conn_pool: ArcDbPool, starting_version: u64, tracker_name: String) -> Self {
        Self {
            conn_pool,
            tracker_name,
            next_version: starting_version,
            last_success_batch: None,
            seen_versions: AHashMap::new(),
        }
    }

    fn update_last_success_batch(&mut self, current_batch: TransactionContext<T>) {
        let mut new_prev_batch = current_batch;
        // While there are batches in seen_versions that are in order, update the new_prev_batch to the next batch.
        while let Some(next_version) = self.seen_versions.remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
        }
        self.next_version = new_prev_batch.end_version + 1;
        self.last_success_batch = Some(new_prev_batch);
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        // Update the processor status
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            let end_timestamp = last_success_batch
                .end_transaction_timestamp
                .as_ref()
                .map(|t| parse_timestamp(t, last_success_batch.end_version as i64))
                .map(|t| t.naive_utc());
            let status = ProcessorStatus {
                processor: self.tracker_name.clone(),
                last_success_version: last_success_batch.end_version as i64,
                last_transaction_timestamp: end_timestamp,
            };
            execute_with_better_error(
                self.conn_pool.clone(),
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
            ).await.map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to update processor status: {}", e),
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl<T> Processable for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    type Input = T;
    type Output = T;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<T>>, ProcessorError> {
        // info!(
        //     start_version = current_batch.start_version,
        //     end_version = current_batch.end_version,
        //     step_name = self.name(),
        //     "Processing versions"
        // );
        // If there's a gap in the next_version and current_version, save the current_version to seen_versions for
        // later processing.
        if self.next_version != current_batch.start_version {
            info!(
                expected_next_version = self.next_version,
                step = self.name(),
                batch_version = current_batch.start_version,
                "Gap detected",
            );
            self.seen_versions
                .insert(current_batch.start_version, TransactionContext {
                    data: vec![], // No data is needed for tracking. This is to avoid clone.
                    start_version: current_batch.start_version,
                    end_version: current_batch.end_version,
                    start_transaction_timestamp: current_batch.start_transaction_timestamp.clone(),
                    end_transaction_timestamp: current_batch.end_transaction_timestamp.clone(),
                    total_size_in_bytes: current_batch.total_size_in_bytes,
                });
        } else {
            // info!("No gap detected");
            // If the current_batch is the next expected version, update the last success batch
            self.update_last_success_batch(TransactionContext {
                data: vec![], // No data is needed for tracking. This is to avoid clone.
                start_version: current_batch.start_version,
                end_version: current_batch.end_version,
                start_transaction_timestamp: current_batch.start_transaction_timestamp.clone(),
                end_transaction_timestamp: current_batch.end_transaction_timestamp.clone(),
                total_size_in_bytes: current_batch.total_size_in_bytes,
            });
        }
        // Pass through
        Ok(Some(current_batch))
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        // If processing or polling ends, save the last successful batch to the database.
        self.save_processor_status().await?;
        Ok(None)
    }
}

#[async_trait]
impl<T: Send + 'static> PollableAsyncStep for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + 'static,
{
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(UPDATE_PROCESSOR_STATUS_SECS)
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<T>>>, ProcessorError> {
        // TODO: Add metrics for gap count
        self.save_processor_status().await?;
        // Nothing should be returned
        Ok(None)
    }
}

impl<T> NamedStep for LatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    fn name(&self) -> String {
        format!(
            "LatestVersionProcessedTracker: {}",
            std::any::type_name::<T>()
        )
    }
}
