// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::{
        move_tables::{CurrentTableItem, TableMetadata}, parquet_transactions::{TransactionModel}, parquet_write_set_changes::{WriteSetChangeDetail}
    }, parquet_manager::{ParquetData, ParquetStruct}, utils::database::PgDbPool
    // schemas::default_schemas::new_transactions::TRANSACTION_SCHEMA,
};
use ahash::AHashMap;

use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use kanal::AsyncSender;

use std::{fmt::Debug, fmt::{Formatter, Result}};
use tracing::{info};
use serde::{Deserialize, Serialize};
use anyhow::{anyhow};



const RESOURCE_TYPES: [&str; 2] = ["transactions", "move_resources"];//, "write_set_changes"];
            // if we are sending move_resources -> transaction_version -> # of structs per transaction_version -> if we can send Map<transaction_version, int>
            // then we need a map to look at key of transaction_version and value of # of structs per transaction_version


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetProcessorConfig {
    pub bucket: String,
    pub google_application_credentials: Option<String>,
    pub is_parquet_backfill_enabled: bool,
}

pub struct ParquetProcessor {
    connection_pool: PgDbPool,
    parquet_senders_map: AHashMap<String, AsyncSender<ParquetData>>,
}

impl ParquetProcessor {
    pub fn new(
        connection_pool: PgDbPool,
        config: ParquetProcessorConfig,
        parquet_senders: Vec<AsyncSender<ParquetData>>,
    ) -> Self {
        
        tracing::info!("init ParquetProcessor");

        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }
        
        assert_eq!(RESOURCE_TYPES.len(), parquet_senders.len(), "The number of senders must match the number of resource types.");

        let parquet_senders_map: AHashMap<String, AsyncSender<ParquetData>> = RESOURCE_TYPES
            .iter()
            .map(|&resource_type| resource_type.to_string())
            .zip(parquet_senders)
            .collect();


        // assert!(parquet_senders.len() == RESOURCE_TYPES.len());

        Self {
            connection_pool,
            parquet_senders_map,
        }
    }
}

impl Debug for ParquetProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(
            f,
            "ParquetProcessor {{ capacity of channel: {:?}  }}",
            &self.parquet_senders_map.capacity()
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>, 
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start: std::time::Instant = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        // maybe return a map and iterate use the key to get the sender
        let (processed_models, transaction_version_to_struct_count) = tokio::task::spawn_blocking(move || process_transactions(transactions)) 
            .await
            .expect("Failed to spawn_blocking for TransactionModel::from_transactions");

        for (transaction_version, struct_count) in transaction_version_to_struct_count.clone() {
            info!("Transaction version: {}, struct count: {}", transaction_version, struct_count);
        }

        for (resource_type, processed_structs) in &processed_models {
            let sender = self.parquet_senders_map.get(resource_type).unwrap();
            if processed_structs.is_empty() {
                continue;
            }
            
            let parquet_data = ParquetData {
                data: processed_structs.clone(),
                last_transaction_timestamp: last_transaction_timestamp.clone(),
                transaction_version_to_struct_count: transaction_version_to_struct_count.clone(),
            };


            sender
                .send(parquet_data)
                .await
                .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_duration_in_secs: f64 = 0.0;
        
        // TODO: revisit: not sure if this is needd here, since we won't use this for now
        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            last_transaction_timestamp,
            db_insertion_duration_in_secs,
        })
    }

    
    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

/**
 * Phase 1 is to migrate table that we know for sure we will deprecate and is huge in size.
 */
pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> 
    (
        AHashMap<String, Vec<ParquetStruct>>,
        AHashMap<i64, i64>,
    )
{
    let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();
    let (txns, _block_metadata_txns, write_set_changes, wsc_details) =
        TransactionModel::from_transactions(&transactions, &mut transaction_version_to_struct_count);
    
    let mut move_modules = vec![];
    let mut move_resources: Vec<ParquetStruct> = vec![];
    let mut table_items = vec![];
    let mut current_table_items = AHashMap::new();
    let mut table_metadata = AHashMap::new();

    for detail in wsc_details {
        match detail {
            WriteSetChangeDetail::Module(module) => {
                move_modules.push(module.clone());
                // transaction_version_to_struct_count.entry(module.transaction_version).and_modify(|e| *e += 1);
            },
            WriteSetChangeDetail::Resource(resource) => {
                move_resources.push(ParquetStruct::MoveResource(resource.clone()));
                transaction_version_to_struct_count.entry(resource.transaction_version).and_modify(|e| *e += 1);
            },
            WriteSetChangeDetail::Table(item, current_item, metadata) => {
                table_items.push(item.clone());
                // transaction_version_to_struct_count.entry(item.transaction_version).and_modify(|e| *e += 1);

                current_table_items.insert(
                    (
                        current_item.table_handle.clone(),
                        current_item.key_hash.clone(),
                    ),
                    current_item.clone(),
                );
                // transaction_version_to_struct_count.entry(current_item.last_transaction_version).and_modify(|e| *e += 1);

                if let Some(meta) = metadata {
                    table_metadata.insert(meta.handle.clone(), meta.clone());
                    // transaction_version_to_struct_count.entry(current_item.last_transaction_version).and_modify(|e| *e += 1);
                }
            },
        }
    }


    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<CurrentTableItem>>();
    let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));


    let parquet_transactions = txns
        .into_iter()
        .map(ParquetStruct::Transaction)
        .collect::<Vec<ParquetStruct>>();

    let _parquet_write_set_changes = write_set_changes
        .into_iter()
        .map(ParquetStruct::WriteSetChange)
        .collect::<Vec<ParquetStruct>>();

    let mut processed_resources_map = AHashMap::new();

    for resource in RESOURCE_TYPES {
        let processed_resources = match resource {
            "move_resources" => move_resources.clone(),
            // "write_set_changes" => parquet_write_set_changes.clone(),
            "transactions" => parquet_transactions.clone(),
            _ => vec![],
        };
        processed_resources_map.insert(resource.to_owned(), processed_resources);
    }

    (
        processed_resources_map,
        transaction_version_to_struct_count,
    )
    
}