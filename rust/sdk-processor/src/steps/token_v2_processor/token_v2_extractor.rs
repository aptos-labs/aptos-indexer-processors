use crate::utils::database::ArcDbPool;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db,
    db::common::models::{
        token_models::{
            token_claims::PostgresCurrentTokenPendingClaim,
            token_royalty::PostgresCurrentTokenRoyaltyV1, tokens::TableMetadataForToken,
        },
        token_v2_models::{
            v2_collections::CurrentCollectionV2, v2_token_activities::PostgresTokenActivityV2,
            v2_token_datas::PostgresCurrentTokenDataV2,
            v2_token_ownerships::PostgresCurrentTokenOwnershipV2,
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
        Vec<CurrentCollectionV2>,
        Vec<PostgresCurrentTokenDataV2>,
        Vec<PostgresCurrentTokenDataV2>,
        Vec<PostgresCurrentTokenOwnershipV2>,
        Vec<PostgresCurrentTokenOwnershipV2>,
        Vec<PostgresTokenActivityV2>,
        Vec<PostgresCurrentTokenRoyaltyV1>,
        Vec<PostgresCurrentTokenPendingClaim>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<CurrentCollectionV2>,
                Vec<PostgresCurrentTokenDataV2>,
                Vec<PostgresCurrentTokenDataV2>,
                Vec<PostgresCurrentTokenOwnershipV2>,
                Vec<PostgresCurrentTokenOwnershipV2>,
                Vec<PostgresTokenActivityV2>,
                Vec<PostgresCurrentTokenRoyaltyV1>,
                Vec<PostgresCurrentTokenPendingClaim>,
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
            _,
            _,
            _,
            current_collections_v2,
            raw_current_token_datas_v2,
            raw_current_deleted_token_datas_v2,
            raw_current_token_ownerships_v2,
            raw_current_deleted_token_ownerships_v2,
            raw_token_activities_v2,
            _,
            raw_current_token_royalties_v1,
            raw_current_token_claims,
        ) = parse_v2_token(
            &transactions.data,
            &table_handle_to_owner,
            &mut Some(db_connection),
        )
        .await;

        let postgres_current_token_claims: Vec<PostgresCurrentTokenPendingClaim> =
            raw_current_token_claims
                .into_iter()
                .map(PostgresCurrentTokenPendingClaim::from)
                .collect();

        let postgres_current_token_royalties_v1: Vec<PostgresCurrentTokenRoyaltyV1> =
            raw_current_token_royalties_v1
                .into_iter()
                .map(PostgresCurrentTokenRoyaltyV1::from)
                .collect();

        let postgres_token_activities_v2: Vec<PostgresTokenActivityV2> = raw_token_activities_v2
            .into_iter()
            .map(PostgresTokenActivityV2::from)
            .collect();

        let postgres_current_token_datas_v2: Vec<PostgresCurrentTokenDataV2> =
            raw_current_token_datas_v2
                .into_iter()
                .map(PostgresCurrentTokenDataV2::from)
                .collect();

        let postgress_current_deleted_token_datas_v2: Vec<PostgresCurrentTokenDataV2> =
            raw_current_deleted_token_datas_v2
                .into_iter()
                .map(PostgresCurrentTokenDataV2::from)
                .collect();

        let postgres_current_token_ownerships_v2: Vec<PostgresCurrentTokenOwnershipV2> =
            raw_current_token_ownerships_v2
                .into_iter()
                .map(PostgresCurrentTokenOwnershipV2::from)
                .collect();

        let postgres_current_deleted_token_ownerships_v2: Vec<PostgresCurrentTokenOwnershipV2> =
            raw_current_deleted_token_ownerships_v2
                .into_iter()
                .map(PostgresCurrentTokenOwnershipV2::from)
                .collect();

        Ok(Some(TransactionContext {
            data: (
                current_collections_v2,
                postgres_current_token_datas_v2,
                postgress_current_deleted_token_datas_v2,
                postgres_current_token_ownerships_v2,
                postgres_current_deleted_token_ownerships_v2,
                postgres_token_activities_v2,
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
