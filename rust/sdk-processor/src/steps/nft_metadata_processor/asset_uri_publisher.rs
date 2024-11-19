use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{convert::remove_null_bytes, errors::ProcessorError},
};
use async_trait::async_trait;
use futures::future::try_join_all;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::publisher::Publisher;
use processor::{
    self,
    db::postgres::models::{
        token_models::token_claims::CurrentTokenPendingClaim,
        token_v2_models::{
            v1_token_royalty::CurrentTokenRoyaltyV1,
            v2_collections::{CollectionV2, CurrentCollectionV2},
            v2_token_activities::TokenActivityV2,
            v2_token_datas::{CurrentTokenDataV2, TokenDataV2},
            v2_token_metadata::CurrentTokenV2Metadata,
            v2_token_ownerships::{CurrentTokenOwnershipV2, TokenOwnershipV2},
        },
    },
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub const CHUNK_SIZE: usize = 1000;

pub struct AssetUriPublisher
where
    Self: Sized + Send + 'static,
{
    chain_id: u64,
    publisher: Arc<Publisher>,
}

impl AssetUriPublisher {
    pub fn new(publisher: Arc<Publisher>, chain_id: u64) -> Self {
        Self {
            publisher,
            chain_id,
        }
    }
}

#[async_trait]
impl Processable for AssetUriPublisher {
    type Input = (
        Vec<CollectionV2>,
        Vec<TokenDataV2>,
        Vec<TokenOwnershipV2>,
        Vec<CurrentCollectionV2>,
        Vec<CurrentTokenDataV2>,
        Vec<CurrentTokenDataV2>,
        Vec<CurrentTokenOwnershipV2>,
        Vec<CurrentTokenOwnershipV2>,
        Vec<TokenActivityV2>,
        Vec<CurrentTokenV2Metadata>,
        Vec<CurrentTokenRoyaltyV1>,
        Vec<CurrentTokenPendingClaim>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<CollectionV2>,
            Vec<TokenDataV2>,
            Vec<TokenOwnershipV2>,
            Vec<CurrentCollectionV2>,
            Vec<CurrentTokenDataV2>,
            Vec<CurrentTokenDataV2>,
            Vec<CurrentTokenOwnershipV2>,
            Vec<CurrentTokenOwnershipV2>,
            Vec<TokenActivityV2>,
            Vec<CurrentTokenV2Metadata>,
            Vec<CurrentTokenRoyaltyV1>,
            Vec<CurrentTokenPendingClaim>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (_, _, _, cc, ctd, _, _, _, _, _, _, _) = input.data;

        let ordering_key = get_current_timestamp();
        let mut pubsub_messages: Vec<PubsubMessage> = Vec::with_capacity(cc.len() + ctd.len());

        // Publish all parsed token and collection asset data to PubSub
        for token_data in ctd {
            pubsub_messages.push(PubsubMessage {
                data: clean_token_pubsub_message(token_data, self.chain_id).into(),
                ordering_key: ordering_key.clone(),
                ..Default::default()
            })
        }

        for collection in cc {
            pubsub_messages.push(PubsubMessage {
                data: clean_collection_pubsub_message(collection, self.chain_id).into(),
                ordering_key: ordering_key.clone(),
                ..Default::default()
            })
        }

        // PubSub can only accept 1000 messages at a time
        let chunks: Vec<Vec<PubsubMessage>> = pubsub_messages
            .chunks(CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        for chunk in chunks {
            try_join_all(
                self.publisher
                    .publish_bulk(chunk)
                    .await
                    .into_iter()
                    .map(|awaiter| awaiter.get()),
            )
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Failed to publish to PubSub: {}", e),
            })?;
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AssetUriPublisher {}

impl NamedStep for AssetUriPublisher {
    fn name(&self) -> String {
        "AssetUriPublisher".to_string()
    }
}

fn clean_token_pubsub_message(ctd: CurrentTokenDataV2, db_chain_id: u64) -> String {
    remove_null_bytes(&format!(
        "{},{},{},{},{},false",
        ctd.token_data_id,
        ctd.token_uri,
        ctd.last_transaction_version,
        ctd.last_transaction_timestamp,
        db_chain_id,
    ))
}

fn clean_collection_pubsub_message(cc: CurrentCollectionV2, db_chain_id: u64) -> String {
    remove_null_bytes(&format!(
        "{},{},{},{},{},false",
        cc.collection_id,
        cc.uri,
        cc.last_transaction_version,
        cc.last_transaction_timestamp,
        db_chain_id,
    ))
}

fn get_current_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}
