// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::{
        object_models::v2_object_utils::{
            ObjectAggregatedData, ObjectAggregatedDataMapping, ObjectWithMetadata,
        },
        token_models::tokens::{TableHandleToOwner, TableMetadataForToken},
        token_v2_models::{
            v2_collections::{CollectionV2, CurrentCollectionV2, CurrentCollectionV2PK},
            v2_token_datas::{CurrentTokenDataV2, CurrentTokenDataV2PK, TokenDataV2},
        },
    },
    utils::{
        database::{PgDbPool, PgPoolConnection},
        util::{parse_timestamp, standardize_address},
    },
};
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use futures_util::future::try_join_all;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::info;

pub const CHUNK_SIZE: usize = 1000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NftMetadataProcessorConfig {
    pub pubsub_topic_name: String,
    pub google_application_credentials: Option<String>,
}

pub struct NftMetadataProcessor {
    connection_pool: PgDbPool,
    chain_id: u8,
    config: NftMetadataProcessorConfig,
}

impl NftMetadataProcessor {
    pub fn new(connection_pool: PgDbPool, config: NftMetadataProcessorConfig) -> Self {
        tracing::info!("init NftMetadataProcessor");

        // Crate reads from authentication from file specified in
        // GOOGLE_APPLICATION_CREDENTIALS env var.
        if let Some(credentials) = config.google_application_credentials.clone() {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials);
        }

        Self {
            connection_pool,
            chain_id: 0,
            config,
        }
    }

    pub fn set_chain_id(&mut self, chain_id: u8) {
        self.chain_id = chain_id;
    }
}

impl Debug for NftMetadataProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "NftMetadataProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for NftMetadataProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::NftMetadataProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let mut conn = self.get_conn().await;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        // Initialize pubsub client
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        let topic = client.topic(&self.config.pubsub_topic_name.clone());
        let publisher = topic.new_publisher(None);
        let ordering_key = get_current_timestamp();

        // Publish CurrentTokenDataV2 and CurrentCollectionV2 from transactions
        let (token_datas, collections) =
            parse_v2_token(&transactions, &table_handle_to_owner, &mut conn).await;
        let mut pubsub_messages: Vec<PubsubMessage> =
            Vec::with_capacity(token_datas.len() + collections.len());

        // Publish all parsed token and collection data to Pubsub
        for token_data in token_datas {
            pubsub_messages.push(PubsubMessage {
                data: format!(
                    "{},{},{},{},{},false",
                    token_data.token_data_id,
                    token_data.token_uri,
                    token_data.last_transaction_version,
                    token_data.last_transaction_timestamp,
                    db_chain_id.expect("db_chain_id must not be null"),
                )
                .into(),
                ordering_key: ordering_key.clone(),
                ..Default::default()
            })
        }

        for collection in collections {
            pubsub_messages.push(PubsubMessage {
                data: format!(
                    "{},{},{},{},{},false",
                    collection.collection_id,
                    collection.uri,
                    collection.last_transaction_version,
                    collection.last_transaction_timestamp,
                    db_chain_id.expect("db_chain_id must not be null"),
                )
                .into(),
                ordering_key: ordering_key.clone(),
                ..Default::default()
            })
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        info!(
            start_version = start_version,
            end_version = end_version,
            "[NFT Metadata Crawler] Publishing to queue"
        );

        let chunks: Vec<Vec<PubsubMessage>> = pubsub_messages
            .chunks(CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        for chunk in chunks {
            try_join_all(
                publisher
                    .publish_bulk(chunk)
                    .await
                    .into_iter()
                    .map(|awaiter| awaiter.get()),
            )
            .await?;
        }

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs,
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

/// Copied from token_processor;
async fn parse_v2_token(
    transactions: &[Transaction],
    table_handle_to_owner: &TableHandleToOwner,
    conn: &mut PgPoolConnection<'_>,
) -> (Vec<CurrentTokenDataV2>, Vec<CurrentCollectionV2>) {
    let mut current_token_datas_v2: HashMap<CurrentTokenDataV2PK, CurrentTokenDataV2> =
        HashMap::new();
    let mut current_collections_v2: HashMap<CurrentCollectionV2PK, CurrentCollectionV2> =
        HashMap::new();

    for txn in transactions {
        let txn_version = txn.version as i64;
        let txn_timestamp = parse_timestamp(txn.timestamp.as_ref().unwrap(), txn_version);
        let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

        let mut token_v2_metadata_helper: ObjectAggregatedDataMapping = HashMap::new();
        for wsc in transaction_info.changes.iter() {
            if let Change::WriteResource(wr) = wsc.change.as_ref().unwrap() {
                if let Some(object) =
                    ObjectWithMetadata::from_write_resource(wr, txn_version).unwrap()
                {
                    token_v2_metadata_helper.insert(
                        standardize_address(&wr.address.to_string()),
                        ObjectAggregatedData {
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
                    if let Some((_, current_collection)) =
                        CollectionV2::get_v1_from_write_table_item(
                            table_item,
                            txn_version,
                            wsc_index,
                            txn_timestamp,
                            table_handle_to_owner,
                            conn,
                        )
                        .await
                        .unwrap()
                    {
                        current_collections_v2
                            .insert(current_collection.collection_id.clone(), current_collection);
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
                    if let Some((_, current_collection)) = CollectionV2::get_v2_from_write_resource(
                        resource,
                        txn_version,
                        wsc_index,
                        txn_timestamp,
                        &token_v2_metadata_helper,
                    )
                    .unwrap()
                    {
                        current_collections_v2
                            .insert(current_collection.collection_id.clone(), current_collection);
                    }
                },

                _ => {},
            }
        }
    }

    let current_token_datas_v2 = current_token_datas_v2
        .into_values()
        .collect::<Vec<CurrentTokenDataV2>>();
    let current_collections_v2 = current_collections_v2
        .into_values()
        .collect::<Vec<CurrentCollectionV2>>();

    (current_token_datas_v2, current_collections_v2)
}

/// Get current system timestamp in milliseconds for ordering key
fn get_current_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}
