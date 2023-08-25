// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::token_v2_models::{
        v2_token_datas::{CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2},
        v2_token_utils::{ObjectWithMetadata, TokenV2AggregatedData, TokenV2AggregatedDataMapping},
    },
    utils::{
        database::PgDbPool,
        util::{parse_timestamp, standardize_address},
    },
};
use aptos_indexer_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use std::{
    collections::HashMap,
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::info;

pub const NAME: &str = "nft_metadata_processor";
pub struct NFTMetadataProcessor {
    connection_pool: PgDbPool,
    pubsub_topic_name: String,
    chain_id: u8,
}

impl NFTMetadataProcessor {
    pub fn new(connection_pool: PgDbPool, pubsub_topic_name: String) -> Self {
        tracing::info!("init NFTMetadataProcessor");
        Self {
            connection_pool,
            pubsub_topic_name,
            chain_id: 0,
        }
    }

    pub fn set_chain_id(&mut self, chain_id: u8) {
        self.chain_id = chain_id;
    }
}

impl Debug for NFTMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "NFTMetadataProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for NFTMetadataProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        // Initialize pubsub client
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        let topic = client.topic(&self.pubsub_topic_name.clone());
        let publisher = topic.new_publisher(None);

        // Publish all parsed CurrentTokenDataV2 to Pubsub
        let pubsub_messages = parse_v2_token(&transactions)
            .iter()
            .map(|token_data| PubsubMessage {
                data: format!(
                    "{},{},{},{},{},false",
                    token_data.token_data_id,
                    token_data.token_uri,
                    token_data.last_transaction_version,
                    token_data.last_transaction_timestamp,
                    db_chain_id.expect("db_chain_id must not be null"),
                )
                .into(),
                ordering_key: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .to_string(),
                ..Default::default()
            })
            .collect();

        info!(
            start_version = start_version,
            end_version = end_version,
            "[NFT Metadata Crawler] Publishing to queue"
        );
        for awaiter in publisher.publish_bulk(pubsub_messages).await {
            awaiter.get().await?;
        }

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

/// Copied from token_processor;
fn parse_v2_token(transactions: &[Transaction]) -> Vec<CurrentTokenDataV2> {
    let mut current_token_datas_v2: HashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        HashMap::new();

    for txn in transactions {
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        let mut token_v2_metadata_helper: TokenV2AggregatedDataMapping = HashMap::new();
        for (_, wsc) in transaction_info.changes.iter().enumerate() {
            if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                if let Some(object) =
                    ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                {
                    token_v2_metadata_helper.insert(
                        standardize_address(&wr.address.to_string()),
                        TokenV2AggregatedData {
                            aptos_collection: None,
                            fixed_supply: None,
                            object,
                            unlimited_supply: None,
                            property_map: None,
                            transfer_event: None,
                            token: None,
                            fungible_asset_metadata: None,
                            fungible_asset_supply: None,
                            fungible_asset_store: None,
                        },
                    );
                }
            }
        }

        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            let wsc_index = index as i64;
            match wsc.change.as_ref().unwrap() {
                Change::WriteTableItem(table_item) => {
                    if let Some((_, current_token_data)) =
                        TokenDataV2::get_v1_from_write_table_item(
                            table_item,
                            txn_version,
                            wsc_index,
                            txn_timestamp,
                        )
                        .unwrap()
                    {
                        current_token_datas_v2
                            .insert(current_token_data.token_data_id.clone(), current_token_data);
                    }
                },
                Change::WriteResource(resource) => {
                    if let Some((_, current_token_data)) = TokenDataV2::get_v2_from_write_resource(
                        resource,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        &token_v2_metadata_helper,
                    )
                    .unwrap()
                    {
                        current_token_datas_v2
                            .insert(current_token_data.token_data_id.clone(), current_token_data);
                    }
                },

                _ => {},
            }
        }
    }

    let mut current_token_datas_v2 = current_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    current_token_datas_v2.sort_by(|a, b| a.token_data_id.cmp(&b.token_data_id));

    current_token_datas_v2
}
