use crate::parquet_processors::ParquetTypeEnum;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::{TransactionContext, TransactionMetadata},
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use std::collections::HashMap;

// pub const DEFAULT_UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;
/// The `ParquetProcessorStatusSaver` trait object should be implemented in order to save the latest successfully
///
/// processed transaction versino to storage. I.e., persisting the `processor_status` to storage.
#[async_trait]
pub trait ParquetProcessorStatusSaver {
    // T represents the transaction type that the processor is tracking.
    async fn save_parquet_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
        table_name: &str,
    ) -> Result<(), ProcessorError>;
}

/// Tracks the versioned processing of sequential transactions, ensuring no gaps
/// occur between them.
///
/// Important: this step assumes ordered transactions. Please use the `OrederByVersionStep` before this step
/// if the transactions are not ordered.
pub struct ParquetVersionTrackerStep<S>
where
    Self: Sized + Send + 'static,
    S: ParquetProcessorStatusSaver + Send + 'static,
{
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: HashMap<ParquetTypeEnum, TransactionContext<()>>,
    polling_interval_secs: u64,
    processor_status_saver: S,
}

impl<S> ParquetVersionTrackerStep<S>
where
    Self: Sized + Send + 'static,
    S: ParquetProcessorStatusSaver + Send + 'static,
{
    pub fn new(processor_status_saver: S, polling_interval_secs: u64) -> Self {
        Self {
            last_success_batch: HashMap::new(),
            processor_status_saver,
            polling_interval_secs,
        }
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        // this shouldn't be over 5.
        println!(
            "Len of last_success_batch: {}",
            self.last_success_batch.len()
        );
        for (parquet_type, last_success_batch) in &self.last_success_batch {
            let table_name = parquet_type.to_string();
            println!("Saving processor status for table: {}", table_name);
            self.processor_status_saver
                .save_parquet_processor_status(last_success_batch, &table_name)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<S> Processable for ParquetVersionTrackerStep<S>
where
    Self: Sized + Send + 'static,
    S: ParquetProcessorStatusSaver + Send + 'static,
{
    type Input = HashMap<ParquetTypeEnum, TransactionMetadata>;
    type Output = HashMap<ParquetTypeEnum, TransactionMetadata>;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        // Initialize a new map to store the processed metadata
        let mut processed_data = HashMap::new();

        // Check for version gap before processing each key-value pair
        let upload_result = current_batch.data;
        println!("Upload result len {}", upload_result.len());
        for (parquet_type, current_metadata) in &upload_result {
            // we need to have a map of last_sucess_bath for parquet-Type as well.
            // if there is a last_success_batch for the current parquet-Type then we need to check the version gap
            println!(
                "checking for parquet_type: {:?} with start version {}, end_version {}",
                parquet_type.to_string(),
                current_metadata.start_version,
                current_metadata.end_version
            );
            if let Some(last_success) = self.last_success_batch.get(parquet_type) {
                if last_success.metadata.end_version + 1 != current_metadata.start_version {
                    println!("Gap detected for {:?} starting from version: {} when the stored end_version is {}", &parquet_type.to_string(), current_metadata.start_version, last_success.metadata.end_version);
                    return Err(ProcessorError::ProcessError {
                        message: format!(
                            "Gap detected for {:?} starting from version: {}",
                            &parquet_type.to_string(),
                            current_metadata.start_version
                        ),
                    });
                }
            }

            processed_data.insert(*parquet_type, current_metadata.clone());

            // Update last_success_batch for the current key
            self.last_success_batch
                .entry(*parquet_type)
                .and_modify(|e| {
                    e.data = ();
                    e.metadata = current_metadata.clone();
                })
                .or_insert(TransactionContext {
                    data: (),
                    metadata: current_metadata.clone(),
                });
        }

        // Pass through the current batch with updated metadata
        Ok(Some(TransactionContext {
            data: processed_data,
            metadata: current_batch.metadata.clone(),
        }))
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        // Save the last successful batch to the database
        self.save_processor_status().await?;
        Ok(None)
    }
}

#[async_trait]
impl<S> PollableAsyncStep for ParquetVersionTrackerStep<S>
where
    Self: Sized + Send + Sync + 'static,
    S: ParquetProcessorStatusSaver + Send + Sync + 'static,
{
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.polling_interval_secs)
    }

    async fn poll(
        &mut self,
    ) -> Result<
        Option<Vec<TransactionContext<HashMap<ParquetTypeEnum, TransactionMetadata>>>>,
        ProcessorError,
    > {
        // TODO: Add metrics for gap count
        self.save_processor_status().await?;
        // Nothing should be returned
        Ok(None)
    }
}

impl<S> NamedStep for ParquetVersionTrackerStep<S>
where
    Self: Sized + Send + 'static,
    S: ParquetProcessorStatusSaver + Send + 'static,
{
    fn name(&self) -> String {
        "ParquetVersionTrackerStep".to_string()
    }
}
