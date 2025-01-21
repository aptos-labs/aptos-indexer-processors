// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::ParquetProcessorTrait;
use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::{
        common::models::{
            fungible_asset_models::{
                raw_v2_fungible_asset_activities::{
                    EventToCoinType, FungibleAssetActivityConvertible, RawFungibleAssetActivity,
                },
                raw_v2_fungible_asset_balances::RawFungibleAssetBalance,
            },
            object_models::v2_object_utils::{
                ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
                Untransferable,
            },
        },
        parquet::models::fungible_asset_models::parquet_v2_fungible_asset_activities::FungibleAssetActivity,
        postgres::models::{
            fungible_asset_models::v2_fungible_asset_utils::FeeStatement,
            resources::{FromWriteResource, V2FungibleAssetResource},
        },
    },
    gap_detectors::ProcessingResult,
    processors::{ProcessorName, ProcessorTrait},
    utils::{
        database::ArcDbPool,
        util::{get_entry_function_from_user_request, standardize_address},
    },
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetFungibleAssetActivitiesProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetFungibleAssetActivitiesProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetFungibleAssetActivitiesProcessor {
    connection_pool: ArcDbPool,
    fungible_asset_activities_sender: AsyncSender<ParquetDataGeneric<FungibleAssetActivity>>,
}

impl ParquetFungibleAssetActivitiesProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetFungibleAssetActivitiesProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let fungible_asset_activities_sender: AsyncSender<
            ParquetDataGeneric<FungibleAssetActivity>,
        > = create_parquet_handler_loop::<FungibleAssetActivity>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetFungibleAssetActivitiesProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            fungible_asset_activities_sender,
        }
    }
}

impl Debug for ParquetFungibleAssetActivitiesProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetFungibleAssetActivitiesProcessor {{ capacity of fungible_asset_activites channel: {:?}}}",
            &self.fungible_asset_activities_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetFungibleAssetActivitiesProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetFungibleAssetActivitiesProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp;
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

        let raw_fungible_asset_activities =
            parse_activities(&transactions, &mut transaction_version_to_struct_count).await;
        let parquet_fungible_asset_activities: Vec<FungibleAssetActivity> =
            raw_fungible_asset_activities
                .into_iter()
                .map(FungibleAssetActivity::from_raw)
                .collect();

        let parquet_fungible_asset_activities = ParquetDataGeneric {
            data: parquet_fungible_asset_activities,
        };

        self.fungible_asset_activities_sender
            .send(parquet_fungible_asset_activities)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        Ok(ProcessingResult::ParquetProcessingResult(
            ParquetProcessingResult {
                start_version: start_version as i64,
                end_version: end_version as i64,
                last_transaction_timestamp,
                txn_version_to_struct_count: Some(transaction_version_to_struct_count),
                parquet_processed_structs: None,
                table_name: "".to_string(),
            },
        ))
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

async fn parse_activities(
    transactions: &[Transaction],
    transaction_version_to_struct_count: &mut AHashMap<i64, i64>,
) -> Vec<RawFungibleAssetActivity> {
    let mut fungible_asset_activities = vec![];

    let data: Vec<_> = transactions
        .iter()
        .map(|txn| {
            let mut fungible_asset_activities = vec![];

            // Get Metadata for fungible assets by object
            let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            if txn.txn_data.is_none() {
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                return vec![];
            }
            let txn_data = txn.txn_data.as_ref().unwrap();
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
            let txn_timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!")
                .seconds;
            #[allow(deprecated)]
            let txn_timestamp = NaiveDateTime::from_timestamp_opt(txn_timestamp, 0)
                .expect("Txn Timestamp is invalid!");

            let default = vec![];
            let (events, user_request, entry_function_id_str) = match txn_data {
                TxnData::BlockMetadata(tx_inner) => (&tx_inner.events, None, None),
                TxnData::Validator(tx_inner) => (&tx_inner.events, None, None),
                TxnData::Genesis(tx_inner) => (&tx_inner.events, None, None),
                TxnData::User(tx_inner) => {
                    let user_request = tx_inner
                        .request
                        .as_ref()
                        .expect("Sends is not present in user txn");
                    let entry_function_id_str = get_entry_function_from_user_request(user_request);
                    (&tx_inner.events, Some(user_request), entry_function_id_str)
                },
                _ => (&default, None, None),
            };

            // This is because v1 events (deposit/withdraw) don't have coin type so the only way is to match
            // the event to the resource using the event guid
            let mut event_to_v1_coin_type: EventToCoinType = AHashMap::new();

            // First loop to get all objects
            // Need to do a first pass to get all the objects
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) = ObjectWithMetadata::from_write_resource(wr).unwrap() {
                        fungible_asset_object_helper.insert(
                            standardize_address(&wr.address.to_string()),
                            ObjectAggregatedData {
                                object,
                                ..ObjectAggregatedData::default()
                            },
                        );
                    }
                }
            }
            // Loop to get the metadata relevant to parse v1 and v2.
            // As an optimization, we also handle v1 balances in the process
            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                    if let Some((_, _, event_to_coin)) =
                        RawFungibleAssetBalance::get_v1_from_write_resource(
                            write_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        event_to_v1_coin_type.extend(event_to_coin);
                    }
                    // Fill the v2 object metadata
                    let address = standardize_address(&write_resource.address.to_string());
                    if let Some(aggregated_data) = fungible_asset_object_helper.get_mut(&address) {
                        if let Some(v2_fungible_asset_resource) =
                            V2FungibleAssetResource::from_write_resource(write_resource).unwrap()
                        {
                            match v2_fungible_asset_resource {
                                V2FungibleAssetResource::FungibleAssetMetadata(
                                    fungible_asset_metadata,
                                ) => {
                                    aggregated_data.fungible_asset_metadata =
                                        Some(fungible_asset_metadata);
                                },
                                V2FungibleAssetResource::FungibleAssetStore(
                                    fungible_asset_store,
                                ) => {
                                    aggregated_data.fungible_asset_store =
                                        Some(fungible_asset_store);
                                },
                                V2FungibleAssetResource::FungibleAssetSupply(
                                    fungible_asset_supply,
                                ) => {
                                    aggregated_data.fungible_asset_supply =
                                        Some(fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetSupply(
                                    concurrent_fungible_asset_supply,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_supply =
                                        Some(concurrent_fungible_asset_supply);
                                },
                                V2FungibleAssetResource::ConcurrentFungibleAssetBalance(
                                    concurrent_fungible_asset_balance,
                                ) => {
                                    aggregated_data.concurrent_fungible_asset_balance =
                                        Some(concurrent_fungible_asset_balance);
                                },
                            }
                        }
                        if let Some(untransferable) =
                            Untransferable::from_write_resource(write_resource).unwrap()
                        {
                            aggregated_data.untransferable = Some(untransferable);
                        }
                    }
                } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap()
                {
                    if let Some((_, _, event_to_coin)) =
                        RawFungibleAssetBalance::get_v1_from_delete_resource(
                            delete_resource,
                            index as i64,
                            txn_version,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        event_to_v1_coin_type.extend(event_to_coin);
                    }
                }
            }

            // The artificial gas event, only need for v1
            if let Some(req) = user_request {
                let fee_statement = events.iter().find_map(|event| {
                    let event_type = event.type_str.as_str();
                    FeeStatement::from_event(event_type, &event.data, txn_version)
                });
                let gas_event = RawFungibleAssetActivity::get_gas_event(
                    transaction_info,
                    req,
                    &entry_function_id_str,
                    txn_version,
                    txn_timestamp,
                    block_height,
                    fee_statement,
                );
                fungible_asset_activities.push(gas_event);
                transaction_version_to_struct_count
                    .entry(txn_version)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }

            // Loop to handle events and collect additional metadata from events for v2
            for (index, event) in events.iter().enumerate() {
                if let Some(v1_activity) = RawFungibleAssetActivity::get_v1_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    &entry_function_id_str,
                    &event_to_v1_coin_type,
                    index as i64,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                        error = ?e,
                        "[Parser] error parsing fungible asset activity v1");
                    panic!("[Parser] error parsing fungible asset activity v1");
                }) {
                    fungible_asset_activities.push(v1_activity);
                    transaction_version_to_struct_count
                        .entry(txn_version)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                }
                if let Some(v2_activity) = RawFungibleAssetActivity::get_v2_from_event(
                    event,
                    txn_version,
                    block_height,
                    txn_timestamp,
                    index as i64,
                    &entry_function_id_str,
                    &fungible_asset_object_helper,
                )
                .unwrap_or_else(|e| {
                    tracing::error!(
                        transaction_version = txn_version,
                        index = index,
                        error = ?e,
                        "[Parser] error parsing fungible asset activity v2");
                    panic!("[Parser] error parsing fungible asset activity v2");
                }) {
                    fungible_asset_activities.push(v2_activity);
                    transaction_version_to_struct_count
                        .entry(txn_version)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                }
            }

            fungible_asset_activities
        })
        .collect();

    for faa in data {
        fungible_asset_activities.extend(faa);
    }
    fungible_asset_activities
}
