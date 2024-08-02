// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::ParquetProcessorTrait;
use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::{
        fungible_asset_models::{
            parquet_coin_supply::CoinSupply,
            parquet_v2_fungible_asset_balances::FungibleAssetBalance,
        },
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
    },
    gap_detectors::ProcessingResult,
    processors::{ProcessorName, ProcessorTrait},
    utils::{database::ArcDbPool, util::standardize_address},
};
use ahash::AHashMap;
use anyhow::anyhow;
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetFungibleAssetProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}

impl ParquetProcessorTrait for ParquetFungibleAssetProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetFungibleAssetProcessor {
    connection_pool: ArcDbPool,
    coin_supply_sender: AsyncSender<ParquetDataGeneric<CoinSupply>>,
    fungible_asset_balances_sender: AsyncSender<ParquetDataGeneric<FungibleAssetBalance>>,
}

impl ParquetFungibleAssetProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetFungibleAssetProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let coin_supply_sender = create_parquet_handler_loop::<CoinSupply>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetFungibleAssetProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        let fungible_asset_balances_sender = create_parquet_handler_loop::<FungibleAssetBalance>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetFungibleAssetProcessor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            coin_supply_sender,
            fungible_asset_balances_sender,
        }
    }
}

impl Debug for ParquetFungibleAssetProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetFungibleAssetProcessor {{ capacity of coin_supply channel: {:?}, capacity of fungible_asset_balances channel: {:?}}}",
            &self.coin_supply_sender.capacity(),
            &self.fungible_asset_balances_sender.capacity(),
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetFungibleAssetProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetFungibleAssetProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
        let mut transaction_version_to_struct_count: AHashMap<i64, i64> = AHashMap::new();

        let (fungible_asset_balances, coin_supply) =
            parse_v2_coin(&transactions, &mut transaction_version_to_struct_count).await;

        let parquet_coin_supply = ParquetDataGeneric { data: coin_supply };

        self.coin_supply_sender
            .send(parquet_coin_supply)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        let parquet_fungible_asset_balances = ParquetDataGeneric {
            data: fungible_asset_balances,
        };

        self.fungible_asset_balances_sender
            .send(parquet_fungible_asset_balances)
            .await
            .map_err(|e| anyhow!("Failed to send to parquet manager: {}", e))?;

        Ok(ProcessingResult::ParquetProcessingResult(
            ParquetProcessingResult {
                start_version: start_version as i64,
                end_version: end_version as i64,
                last_transaction_timestamp: last_transaction_timestamp.clone(),
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

async fn parse_v2_coin(
    transactions: &[Transaction],
    transaction_version_to_struct_count: &mut AHashMap<i64, i64>,
) -> (Vec<FungibleAssetBalance>, Vec<CoinSupply>) {
    let mut fungible_asset_balances = vec![];
    let mut all_coin_supply = vec![];

    // Get Metadata for fungible assets by object
    let mut fungible_asset_object_helper: ObjectAggregatedDataMapping = AHashMap::new();

    for txn in transactions {
        let txn_version = txn.version as i64;
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");
        let txn_timestamp = txn
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!")
            .seconds;
        #[allow(deprecated)]
        let txn_timestamp =
            NaiveDateTime::from_timestamp_opt(txn_timestamp, 0).expect("Txn Timestamp is invalid!");

        // First loop to get all objects
        // Need to do a first pass to get all the objects
        for wsc in transaction_info.changes.iter() {
            if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                if let Some(object) =
                    ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                {
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

        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            if let Change::WriteResource(write_resource) = wsc.change.as_ref().unwrap() {
                if let Some((balance, _, _)) = FungibleAssetBalance::get_v1_from_write_resource(
                    write_resource,
                    index as i64,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap()
                {
                    fungible_asset_balances.push(balance);
                    transaction_version_to_struct_count
                        .entry(txn_version)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                }
            } else if let Change::DeleteResource(delete_resource) = wsc.change.as_ref().unwrap() {
                if let Some((balance, _, _)) = FungibleAssetBalance::get_v1_from_delete_resource(
                    delete_resource,
                    index as i64,
                    txn_version,
                    txn_timestamp,
                )
                .unwrap()
                {
                    fungible_asset_balances.push(balance);
                    transaction_version_to_struct_count
                        .entry(txn_version)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                }
            }
        }

        // Loop to handle all the other changes
        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            match wsc.change.as_ref().unwrap() {
                Change::WriteResource(write_resource) => {
                    if let Some((balance, _)) = FungibleAssetBalance::get_v2_from_write_resource(
                        write_resource,
                        index as i64,
                        txn_version,
                        txn_timestamp,
                        &fungible_asset_object_helper,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        tracing::error!(
                            transaction_version = txn_version,
                            index = index,
                                error = ?e,
                            "[Parser] error parsing fungible balance v2");
                        panic!("[Parser] error parsing fungible balance v2");
                    }) {
                        fungible_asset_balances.push(balance);
                        transaction_version_to_struct_count
                            .entry(txn_version)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                },
                Change::WriteTableItem(table_item) => {
                    if let Some(coin_supply) =
                        CoinSupply::from_write_table_item(table_item, txn_version, txn_timestamp)
                            .unwrap()
                    {
                        all_coin_supply.push(coin_supply);
                        transaction_version_to_struct_count
                            .entry(txn_version)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                },
                _ => {},
            }
        }
    }

    (fungible_asset_balances, all_coin_supply)
}
