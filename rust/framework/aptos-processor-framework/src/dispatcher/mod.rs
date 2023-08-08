// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "counters")]
use crate::counters::{
    LATEST_PROCESSED_VERSION, PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS,
    PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS, PROCESSOR_ERRORS_COUNT, PROCESSOR_INVOCATIONS_COUNT,
    PROCESSOR_SUCCESSES_COUNT,
};
#[cfg(feature = "counters")]
use crate::utils::time_diff_since_pb_timestamp_in_secs;
use crate::{
    processor::{ProcessingResult, ProcessorTrait},
    storage::StorageTrait,
};
use anyhow::Context as AnyhowContext;
use aptos_indexer_protos::transaction::v1::Transaction;
use aptos_moving_average::MovingAverage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{error::TryRecvError, Receiver};
use tracing::{error, info};

/// Configuration often used by Dispatchers.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DispatcherConfig {
    #[serde(default = "DispatcherConfig::default_num_concurrent_processing_tasks")]
    pub num_concurrent_processing_tasks: usize,
}

impl DispatcherConfig {
    const fn default_num_concurrent_processing_tasks() -> usize {
        10
    }
}

#[derive(Debug)]
pub struct Dispatcher {
    pub config: DispatcherConfig,
    pub storage: Arc<dyn StorageTrait>,
    pub processor: Arc<dyn ProcessorTrait>,
    pub receiver: Receiver<(u8, Vec<Transaction>)>,
    pub starting_version: u64,
}

/// TODO document how the dispatcher works, what it does when it encounters errors, etc.
/// TODO: Stop panicking in these tokio workers, exit with an error instead.
/// TODO: Use the ! return type when it is stable.

impl Dispatcher {
    /// This is the main logic of the processor. We will do a few large parts:
    /// 1. Connect to GRPC and handling all the stuff before starting the stream such as diesel migration
    /// 2. Start a thread specifically to fetch data from GRPC. We will keep a buffer of X batches of transactions
    /// 3. Start a loop to consume from the buffer. We will have Y threads to process the transactions in parallel. (Y should be less than X for obvious reasons)
    ///   * Note that the batches will be sequential so we won't have problems with gaps
    /// 4. We will keep track of the last processed version and monitoring things like TPS
    pub async fn dispatch(&mut self) {
        let processor_name = self.processor.name();
        let concurrent_tasks = self.config.num_concurrent_processing_tasks;

        let mut moving_average = MovingAverage::new(10);

        // This is the consumer side of the channel. These are the major states:
        // 1. We're backfilling so we should expect many concurrent threads to process transactions
        // 2. We're caught up so we should expect a single thread to process transactions
        // 3. We have received either an empty batch or a batch with a gap. We should panic.
        // 4. We have not received anything in X seconds, we should panic.
        // 5. If it's the wrong chain, panic.
        let mut db_chain_id = None;
        let mut batch_start_version = self.starting_version;

        loop {
            let mut transactions_batches = vec![];
            // TODO: All this stuff with casting back and forth u64 and i64 is not how
            // it used to work. Figure out how this next line didn't underflow before.
            let mut last_fetched_version = (batch_start_version as i64) - 1;
            for task_index in 0..concurrent_tasks {
                let receive_status = match task_index {
                    0 => {
                        // If we're the first task, we should wait until we get data. If `None`, it means the channel is closed.
                        self.receiver.recv().await.ok_or(TryRecvError::Disconnected)
                    },
                    _ => {
                        // If we're not the first task, we should poll to see if we get any data.
                        self.receiver.try_recv()
                    },
                };
                match receive_status {
                    Ok((chain_id, transactions)) => {
                        if let Some(existing_id) = db_chain_id {
                            if chain_id != existing_id {
                                error!(
                                    processor_name = processor_name,
                                    chain_id = chain_id,
                                    existing_id = existing_id,
                                    "[Parser] Stream somehow changed chain id!",
                                );
                                panic!("[Parser] Stream somehow changed chain id!");
                            }
                        } else {
                            db_chain_id = Some(
                                self.check_or_update_chain_id(chain_id)
                                    .await
                                    .expect("Failed to update chain ID"),
                            );
                        }
                        let current_fetched_version =
                            transactions.as_slice().first().unwrap().version;
                        if (last_fetched_version + 1) as u64 != current_fetched_version {
                            error!(
                                batch_start_version = batch_start_version,
                                last_fetched_version = last_fetched_version,
                                current_fetched_version = current_fetched_version,
                                "[Parser] Received batch with gap from GRPC stream"
                            );
                            panic!("[Parser] Received batch with gap from GRPC stream");
                        }
                        last_fetched_version =
                            transactions.as_slice().last().unwrap().version as i64;
                        transactions_batches.push(transactions);
                    },
                    // Channel is empty and send is not drpped which we definitely expect. Wait for a bit and continue polling.
                    Err(TryRecvError::Empty) => {
                        break;
                    },
                    // This happens when the channel is closed. We should panic.
                    Err(TryRecvError::Disconnected) => {
                        error!(
                            processor_name = processor_name,
                            "[Parser] Channel closed; stream ended."
                        );
                        panic!("[Parser] Channel closed");
                    },
                }
            }

            // Process the transactions in parallel
            let mut tasks = vec![];
            for transactions in transactions_batches {
                let processor_clone = self.processor.clone();
                let task = tokio::spawn(async move {
                    let start_version = transactions.as_slice().first().unwrap().version;
                    let end_version = transactions.as_slice().last().unwrap().version;
                    #[cfg(feature = "counters")]
                    let txn_time = {
                        let txn_time = transactions.as_slice().first().unwrap().timestamp.clone();
                        if let Some(ref t) = txn_time {
                            PROCESSOR_DATA_RECEIVED_LATENCY_IN_SECS
                                .with_label_values(&[processor_name])
                                .set(time_diff_since_pb_timestamp_in_secs(t));
                        }
                        PROCESSOR_INVOCATIONS_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        txn_time
                    };
                    #[cfg(feature = "counters")]
                    if let Some(ref t) = txn_time {
                        PROCESSOR_DATA_PROCESSED_LATENCY_IN_SECS
                            .with_label_values(&[processor_name])
                            .set(time_diff_since_pb_timestamp_in_secs(t));
                    }
                    processor_clone
                        .process_transactions(transactions, start_version, end_version)
                        .await
                });
                tasks.push(task);
            }
            let processing_time = std::time::Instant::now();
            let task_count = tasks.len();
            let batches = match futures::future::try_join_all(tasks).await {
                Ok(res) => res,
                Err(err) => panic!("[Parser] Error processing transaction batches: {:?}", err),
            };
            info!(
                processing_duration = processing_time.elapsed().as_secs_f64(),
                task_count = task_count,
                processor_name = processor_name,
                "[Parser] Finished processing transaction batches"
            );
            // Update states depending on results of the batch processing
            let mut processed_versions = vec![];
            for res in batches {
                let processed: ProcessingResult = match res {
                    Ok(versions) => {
                        #[cfg(feature = "counters")]
                        PROCESSOR_SUCCESSES_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        versions
                    },
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            error = ?e,
                            "[Parser] Error processing transactions"
                        );
                        #[cfg(feature = "counters")]
                        PROCESSOR_ERRORS_COUNT
                            .with_label_values(&[processor_name])
                            .inc();
                        panic!();
                    },
                };
                processed_versions.push(processed);
            }

            // Make sure there are no gaps and advance states
            processed_versions.sort();
            let mut prev_start = None;
            let mut prev_end = None;
            let processed_versions_sorted = processed_versions.clone();
            for (start, end) in processed_versions {
                if prev_start.is_none() {
                    prev_start = Some(start);
                    prev_end = Some(end);
                } else {
                    if prev_end.unwrap() + 1 != start {
                        error!(
                            processor_name = processor_name,
                            processed_versions = processed_versions_sorted
                                .iter()
                                .map(|(s, e)| format!("{}-{}", s, e))
                                .collect::<Vec<_>>()
                                .join(", "),
                            "[Parser] Gaps in processing stream"
                        );
                        panic!();
                    }
                    prev_start = Some(start);
                    prev_end = Some(end);
                }
            }
            let batch_start = processed_versions_sorted.first().unwrap().0;
            let batch_end = processed_versions_sorted.last().unwrap().1;
            batch_start_version = batch_end + 1;

            #[cfg(feature = "counters")]
            LATEST_PROCESSED_VERSION
                .with_label_values(&[processor_name])
                .set(batch_end as i64);
            self.storage
                .write_last_processed_version(processor_name, batch_end)
                .await
                .expect("Failed to write last processed version");

            moving_average.tick_now(batch_end - batch_start + 1);
            info!(
                processor_name = processor_name,
                start_version = batch_start,
                end_version = batch_end,
                batch_size = batch_end - batch_start + 1,
                tps = (moving_average.avg() * 1000.0) as u64,
                "[Parser] Processed transactions.",
            );
        }
    }

    /// Verify the chain id from GRPC against the database.
    pub async fn check_or_update_chain_id(&self, grpc_chain_id: u8) -> anyhow::Result<u8> {
        let processor_name = self.processor.name();

        info!(
            processor_name = processor_name,
            "[Parser] Checking if chain ID is correct"
        );

        let maybe_existing_chain_id = self.storage.read_chain_id().await?;

        match maybe_existing_chain_id {
            Some(chain_id) => {
                anyhow::ensure!(chain_id == grpc_chain_id, "[Parser] Wrong chain detected! Trying to index chain {} now but existing data is for chain {}", grpc_chain_id, chain_id);
                info!(
                    processor_name = processor_name,
                    chain_id = chain_id,
                    "[Parser] Chain ID matches! Continue to index...",
                );
                Ok(chain_id)
            },
            None => {
                info!(
                    processor_name = processor_name,
                    chain_id = grpc_chain_id,
                    "[Parser] Adding chain ID to db, continue to index.."
                );
                self.storage
                    .write_chain_id(grpc_chain_id)
                    .await
                    .context(r#"[Parser] Error updating chain_id!"#)
                    .map(|_| grpc_chain_id)
            },
        }
    }
}
