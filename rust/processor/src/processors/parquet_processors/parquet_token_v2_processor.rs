// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bq_analytics::{
        create_parquet_handler_loop, generic_parquet_processor::ParquetDataGeneric,
        ParquetProcessingResult,
    },
    db::common::models::{
        fungible_asset_models::v2_fungible_asset_utils::FungibleAssetMetadata,
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata, Untransferable,
        },
        token_models::tokens::{TableHandleToOwner, TableMetadataForToken},
        token_v2_models::{
            parquet_v2_token_datas::TokenDataV2,
            parquet_v2_token_ownerships::TokenOwnershipV2,
            v2_token_ownerships::NFTOwnershipV2,
            v2_token_utils::{
                AptosCollection, Burn, BurnEvent, ConcurrentSupply, FixedSupply, MintEvent,
                PropertyMapModel, TokenIdentifiers, TokenV2, TokenV2Burned, TokenV2Minted,
                TransferEvent, UnlimitedSupply,
            },
        },
    },
    gap_detectors::ProcessingResult,
    processors::{parquet_processors::ParquetProcessorTrait, ProcessorName, ProcessorTrait},
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::ArcDbPool,
        util::{parse_timestamp, standardize_address},
    },
};
use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use aptos_protos::transaction::v1::{transaction::TxnData, write_set_change::Change, Transaction};
use async_trait::async_trait;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetTokenV2ProcessorConfig {
    pub google_application_credentials: Option<String>,
    pub bucket_name: String,
    pub bucket_root: String,
    pub parquet_handler_response_channel_size: usize,
    pub max_buffer_size: usize,
    pub parquet_upload_interval: u64,
}
impl ParquetProcessorTrait for ParquetTokenV2ProcessorConfig {
    fn parquet_upload_interval_in_secs(&self) -> Duration {
        Duration::from_secs(self.parquet_upload_interval)
    }
}

pub struct ParquetTokenV2Processor {
    connection_pool: ArcDbPool,
    v2_token_datas_sender: AsyncSender<ParquetDataGeneric<TokenDataV2>>,
    v2_token_ownerships_sender: AsyncSender<ParquetDataGeneric<TokenOwnershipV2>>,
}

impl ParquetTokenV2Processor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: ParquetTokenV2ProcessorConfig,
        new_gap_detector_sender: AsyncSender<ProcessingResult>,
    ) -> Self {
        config.set_google_credentials(config.google_application_credentials.clone());

        let v2_token_datas_sender = create_parquet_handler_loop::<TokenDataV2>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTokenV2Processor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        let v2_token_ownerships_sender = create_parquet_handler_loop::<TokenOwnershipV2>(
            new_gap_detector_sender.clone(),
            ProcessorName::ParquetTokenV2Processor.into(),
            config.bucket_name.clone(),
            config.bucket_root.clone(),
            config.parquet_handler_response_channel_size,
            config.max_buffer_size,
            config.parquet_upload_interval_in_secs(),
        );

        Self {
            connection_pool,
            v2_token_datas_sender,
            v2_token_ownerships_sender,
        }
    }
}

impl Debug for ParquetTokenV2Processor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "TokenV2TransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for ParquetTokenV2Processor {
    fn name(&self) -> &'static str {
        ProcessorName::ParquetTokenV2Processor.into()
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

        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        let (token_datas_v2, token_ownerships_v2) = parse_v2_token(
            &transactions,
            &table_handle_to_owner,
            &mut transaction_version_to_struct_count,
        )
        .await;

        let token_data_v2_parquet_data = ParquetDataGeneric {
            data: token_datas_v2,
        };

        self.v2_token_datas_sender
            .send(token_data_v2_parquet_data)
            .await
            .context("Failed to send token data v2 parquet data")?;

        let token_ownerships_v2_parquet_data = ParquetDataGeneric {
            data: token_ownerships_v2,
        };

        self.v2_token_ownerships_sender
            .send(token_ownerships_v2_parquet_data)
            .await
            .context("Failed to send token ownerships v2 parquet data")?;

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

async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: &TableHandleToOwner,
    transaction_version_to_struct_count: &mut AHashMap<i64, i64>,
) -> (Vec<TokenDataV2>, Vec<TokenOwnershipV2>) {
    // Token V2 and V1 combined
    let mut token_datas_v2 = vec![];
    let mut token_ownerships_v2 = vec![];

    // Tracks prior ownership in case a token gets burned
    let mut prior_nft_ownership: AHashMap<String, NFTOwnershipV2> = AHashMap::new();
    // Get Metadata for token v2 by object
    // We want to persist this through the entire batch so that even if a token is burned,
    // we can still get the object core metadata for it
    let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = AHashMap::new();

    // Code above is inefficient (multiple passthroughs) so I'm approaching TokenV2 with a cleaner code structure
    for txn in transactions {
        let txn_version = txn.version;
        let txn_data = match txn.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["TokenV2Processor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist"
                );
                continue;
            },
        };
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        if let TxnData::User(user_txn) = txn_data {
            // Get burn events for token v2 by object
            let mut tokens_burned: TokenV2Burned = AHashMap::new();
            // Get mint events for token v2 by object
            let mut tokens_minted: TokenV2Minted = AHashSet::new();
            // Need to do a first pass to get all the objects
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    if let Some(object) =
                        ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                    {
                        token_v2_metadata_helper.insert(
                            standardize_address(&wr.address.to_string()),
                            ObjectAggregatedData {
                                object,
                                ..ObjectAggregatedData::default()
                            },
                        );
                    }
                }
            }

            // Need to do a second pass to get all the structs related to the object
            for wsc in transaction_info.changes.iter() {
                if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                    let address = standardize_address(&wr.address.to_string());
                    if let Some(aggregated_data) = token_v2_metadata_helper.get_mut(&address) {
                        if let Some(fixed_supply) =
                            FixedSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fixed_supply = Some(fixed_supply);
                        }
                        if let Some(unlimited_supply) =
                            UnlimitedSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.unlimited_supply = Some(unlimited_supply);
                        }
                        if let Some(aptos_collection) =
                            AptosCollection::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.aptos_collection = Some(aptos_collection);
                        }
                        if let Some(property_map) =
                            PropertyMapModel::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.property_map = Some(property_map);
                        }
                        if let Some(concurrent_supply) =
                            ConcurrentSupply::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.concurrent_supply = Some(concurrent_supply);
                        }
                        if let Some(token) = TokenV2::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.token = Some(token);
                        }
                        if let Some(fungible_asset_metadata) =
                            FungibleAssetMetadata::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.fungible_asset_metadata = Some(fungible_asset_metadata);
                        }
                        if let Some(token_identifier) =
                            TokenIdentifiers::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.token_identifier = Some(token_identifier);
                        }
                        if let Some(untransferable) =
                            Untransferable::from_write_resource(wr, txn_version).unwrap()
                        {
                            aggregated_data.untransferable = Some(untransferable);
                        }
                    }
                }
            }

            // Pass through events to get the burn events and token activities v2
            // This needs to be here because we need the metadata above for token activities
            // and burn / transfer events need to come before the next section
            for (index, event) in user_txn.events.iter().enumerate() {
                if let Some(burn_event) = Burn::from_event(event, txn_version).unwrap() {
                    tokens_burned.insert(burn_event.get_token_address(), burn_event);
                }
                if let Some(old_burn_event) = BurnEvent::from_event(event, txn_version).unwrap() {
                    let burn_event = Burn::new(
                        standardize_address(event.key.as_ref().unwrap().account_address.as_str()),
                        old_burn_event.get_token_address(),
                        "".to_string(),
                    );
                    tokens_burned.insert(burn_event.get_token_address(), burn_event);
                }
                if let Some(mint_event) = MintEvent::from_event(event, txn_version).unwrap() {
                    tokens_minted.insert(mint_event.get_token_address());
                }
                if let Some(transfer_events) =
                    TransferEvent::from_event(event, txn_version).unwrap()
                {
                    if let Some(aggregated_data) =
                        token_v2_metadata_helper.get_mut(&transfer_events.get_object_address())
                    {
                        // we don't want index to be 0 otherwise we might have collision with write set change index
                        // note that these will be multiplied by -1 so that it doesn't conflict with wsc index
                        let index = if index == 0 {
                            user_txn.events.len()
                        } else {
                            index
                        };
                        aggregated_data
                            .transfer_events
                            .push((index as i64, transfer_events));
                    }
                }
            }

            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                let wsc_index = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteTableItem(table_item) => {
                        if let Some(token_data) = TokenDataV2::get_v1_from_write_table_item(
                            table_item,
                            txn_version,
                            wsc_index,
                            txn_timestamp,
                        )
                        .unwrap()
                        {
                            token_datas_v2.push(token_data);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_write_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                            }
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                    },
                    Change::DeleteTableItem(table_item) => {
                        if let Some((token_ownership, current_token_ownership)) =
                            TokenOwnershipV2::get_v1_from_delete_table_item(
                                table_item,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                table_handle_to_owner,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(token_ownership);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            if let Some(cto) = current_token_ownership {
                                prior_nft_ownership.insert(
                                    cto.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: cto.token_data_id.clone(),
                                        owner_address: cto.owner_address.clone(),
                                        is_soulbound: cto.is_soulbound_v2,
                                    },
                                );
                            }
                        }
                    },
                    Change::WriteResource(resource) => {
                        if let Some(token_data) = TokenDataV2::get_v2_from_write_resource(
                            resource,
                            txn_version,
                            wsc_index,
                            txn_timestamp,
                            &token_v2_metadata_helper,
                        )
                        .unwrap()
                        {
                            // Add NFT ownership
                            let mut ownerships = TokenOwnershipV2::get_nft_v2_from_token_data(
                                &token_data,
                                &token_v2_metadata_helper,
                            )
                            .unwrap();
                            if let Some(current_nft_ownership) = ownerships.first() {
                                // Note that the first element in ownerships is the current ownership. We need to cache
                                // it in prior_nft_ownership so that moving forward if we see a burn we'll know
                                // where it came from.
                                prior_nft_ownership.insert(
                                    current_nft_ownership.token_data_id.clone(),
                                    NFTOwnershipV2 {
                                        token_data_id: current_nft_ownership.token_data_id.clone(),
                                        owner_address: current_nft_ownership
                                            .owner_address
                                            .as_ref()
                                            .unwrap()
                                            .clone(),
                                        is_soulbound: current_nft_ownership.is_soulbound_v2,
                                    },
                                );
                            }
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += ownerships.len() as i64 + 1)
                                .or_insert(ownerships.len() as i64 + 1);
                            token_ownerships_v2.append(&mut ownerships);
                            token_datas_v2.push(token_data);
                        }
                        // Add burned NFT handling
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_write_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                                &token_v2_metadata_helper,
                            )
                            .await
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                        }
                    },
                    Change::DeleteResource(resource) => {
                        if let Some((nft_ownership, current_nft_ownership)) =
                            TokenOwnershipV2::get_burned_nft_v2_from_delete_resource(
                                resource,
                                txn_version,
                                wsc_index,
                                txn_timestamp,
                                &prior_nft_ownership,
                                &tokens_burned,
                            )
                            .unwrap()
                        {
                            token_ownerships_v2.push(nft_ownership);
                            transaction_version_to_struct_count
                                .entry(txn_version)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            prior_nft_ownership.insert(
                                current_nft_ownership.token_data_id.clone(),
                                NFTOwnershipV2 {
                                    token_data_id: current_nft_ownership.token_data_id.clone(),
                                    owner_address: current_nft_ownership.owner_address.clone(),
                                    is_soulbound: current_nft_ownership.is_soulbound_v2,
                                },
                            );
                        }
                    },
                    _ => {},
                }
            }
        }
    }

    (token_datas_v2, token_ownerships_v2)
}
