use crate::utils::database::ArcDbPool;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::token_v2_models::{
            raw_token_claims::CurrentTokenPendingClaimConvertible,
            raw_v1_token_royalty::CurrentTokenRoyaltyV1Convertible,
            raw_v2_token_activities::TokenActivityV2Convertible,
            raw_v2_token_datas::{CurrentTokenDataV2Convertible, TokenDataV2Convertible},
            raw_v2_token_metadata::CurrentTokenV2MetadataConvertible,
            raw_v2_token_ownerships::{
                CurrentTokenOwnershipV2Convertible, TokenOwnershipV2Convertible,
            },
        },
        postgres::models::{
            token_models::{token_claims::CurrentTokenPendingClaim, tokens::TableMetadataForToken},
            token_v2_models::{
                v1_token_royalty::CurrentTokenRoyaltyV1,
                v2_collections::{CollectionV2, CurrentCollectionV2},
                v2_token_activities::TokenActivityV2,
                v2_token_datas::{CurrentTokenDataV2, TokenDataV2},
                v2_token_metadata::CurrentTokenV2Metadata,
                v2_token_ownerships::{CurrentTokenOwnershipV2, TokenOwnershipV2},
            },
        },
    },
    processors::token_v2_processor::parse_v2_token,
    utils::database::DbContext,
};

/// Extracts fungible asset events, metadata, balances, and v1 supply from transactions
pub struct TokenV2Extractor
where
    Self: Sized + Send + 'static,
{
    query_retries: u32,
    query_retry_delay_ms: u64,
    conn_pool: ArcDbPool,
}

impl TokenV2Extractor {
    pub fn new(query_retries: u32, query_retry_delay_ms: u64, conn_pool: ArcDbPool) -> Self {
        Self {
            query_retries,
            query_retry_delay_ms,
            conn_pool,
        }
    }
}

#[async_trait]
impl Processable for TokenV2Extractor {
    type Input = Vec<Transaction>;
    type Output = (
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
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<CollectionV2>,     // TODO: Deprecate this
                Vec<TokenDataV2>,      // TODO: Deprecate this
                Vec<TokenOwnershipV2>, // TODO: Deprecate this
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
        >,
        ProcessorError,
    > {
        let conn = self
            .conn_pool
            .get()
            .await
            .map_err(|e| ProcessorError::DBStoreError {
                message: format!("Failed to get connection from pool: {:?}", e),
                query: None,
            })?;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner: ahash::AHashMap<String, TableMetadataForToken> =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions.data);
        let db_connection = DbContext {
            conn,
            query_retries: self.query_retries,
            query_retry_delay_ms: self.query_retry_delay_ms,
        };

        let (
            collections_v2,
            raw_token_datas_v2,
            raw_token_ownerships_v2,
            current_collections_v2,
            raw_current_token_datas_v2,
            raw_current_deleted_token_datas_v2,
            raw_current_token_ownerships_v2,
            raw_current_deleted_token_ownerships_v2,
            raw_token_activities_v2,
            raw_current_token_v2_metadata,
            raw_current_token_royalties_v1,
            raw_current_token_claims,
        ) = parse_v2_token(
            &transactions.data,
            &table_handle_to_owner,
            &mut Some(db_connection),
        )
        .await;

        let postgres_current_token_claims: Vec<CurrentTokenPendingClaim> = raw_current_token_claims
            .into_iter()
            .map(CurrentTokenPendingClaim::from_raw)
            .collect();

        let postgres_current_token_royalties_v1: Vec<CurrentTokenRoyaltyV1> =
            raw_current_token_royalties_v1
                .into_iter()
                .map(CurrentTokenRoyaltyV1::from_raw)
                .collect();

        let postgres_current_token_v2_metadata: Vec<CurrentTokenV2Metadata> =
            raw_current_token_v2_metadata
                .into_iter()
                .map(CurrentTokenV2Metadata::from_raw)
                .collect();

        let postgres_token_activities_v2: Vec<TokenActivityV2> = raw_token_activities_v2
            .into_iter()
            .map(TokenActivityV2::from_raw)
            .collect();

        let postgres_token_datas_v2: Vec<TokenDataV2> = raw_token_datas_v2
            .into_iter()
            .map(TokenDataV2::from_raw)
            .collect();

        let postgres_current_token_datas_v2: Vec<CurrentTokenDataV2> = raw_current_token_datas_v2
            .into_iter()
            .map(CurrentTokenDataV2::from_raw)
            .collect();

        let postgress_current_deleted_token_datas_v2: Vec<CurrentTokenDataV2> =
            raw_current_deleted_token_datas_v2
                .into_iter()
                .map(CurrentTokenDataV2::from_raw)
                .collect();

        let postgres_token_ownerships_v2: Vec<TokenOwnershipV2> = raw_token_ownerships_v2
            .into_iter()
            .map(TokenOwnershipV2::from_raw)
            .collect();

        let postgres_current_token_ownerships_v2: Vec<CurrentTokenOwnershipV2> =
            raw_current_token_ownerships_v2
                .into_iter()
                .map(CurrentTokenOwnershipV2::from_raw)
                .collect();

        let postgres_current_deleted_token_ownerships_v2: Vec<CurrentTokenOwnershipV2> =
            raw_current_deleted_token_ownerships_v2
                .into_iter()
                .map(CurrentTokenOwnershipV2::from_raw)
                .collect();

        Ok(Some(TransactionContext {
            data: (
                collections_v2,
                postgres_token_datas_v2,
                postgres_token_ownerships_v2,
                current_collections_v2,
                postgres_current_token_datas_v2,
                postgress_current_deleted_token_datas_v2,
                postgres_current_token_ownerships_v2,
                postgres_current_deleted_token_ownerships_v2,
                postgres_token_activities_v2,
                postgres_current_token_v2_metadata,
                postgres_current_token_royalties_v1,
                postgres_current_token_claims,
            ),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for TokenV2Extractor {}

impl NamedStep for TokenV2Extractor {
    fn name(&self) -> String {
        "TokenV2Extractor".to_string()
    }
}
