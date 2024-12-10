use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::{database::ArcDbPool, parquet_extractor_helper::add_to_map_if_opted_in_for_backfill},
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::{
        common::models::token_v2_models::raw_token_claims::CurrentTokenPendingClaimConvertible,
        parquet::models::token_v2_models::token_claims::CurrentTokenPendingClaim,
        postgres::models::token_models::tokens::TableMetadataForToken,
    },
    processors::token_v2_processor::parse_v2_token,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;
use tracing::debug;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetTokenV2Extractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
    // TODO: Revisit and remove
    query_retries: u32,
    query_retry_delay_ms: u64,
    conn_pool: ArcDbPool,
}

impl ParquetTokenV2Extractor {
    pub fn new(
        opt_in_tables: TableFlags,
        query_retries: u32,
        query_retry_delay_ms: u64,
        conn_pool: ArcDbPool,
    ) -> Self {
        Self {
            opt_in_tables,
            query_retries,
            query_retry_delay_ms,
            conn_pool,
        }
    }
}
type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetTokenV2Extractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let mut conn = self
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

        let (
            _collections_v2,
            _token_datas_v2,
            _token_ownerships_v2,
            _current_collections_v2,
            _current_token_datas_v2,
            _current_deleted_token_datas_v2,
            _current_token_ownerships_v2,
            _current_deleted_token_ownerships_v2,
            _token_activities_v2,
            _current_token_v2_metadata,
            _current_token_royalties_v1,
            raw_current_token_claims,
        ) = parse_v2_token(
            &transactions.data,
            &table_handle_to_owner,
            &mut conn,
            self.query_retries,
            self.query_retry_delay_ms,
        )
        .await;

        let parquet_current_token_claims: Vec<CurrentTokenPendingClaim> = raw_current_token_claims
            .into_iter()
            .map(CurrentTokenPendingClaim::from_raw)
            .collect();

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(
            " - CurrentTokenPendingClaim: {}",
            parquet_current_token_claims.len()
        );

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [(
            TableFlags::CURRENT_TOKEN_PENDING_CLAIMS,
            ParquetTypeEnum::CurrentTokenPendingClaims,
            ParquetTypeStructs::CurrentTokenPendingClaim(parquet_current_token_claims),
        )];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for ParquetTokenV2Extractor {}

impl NamedStep for ParquetTokenV2Extractor {
    fn name(&self) -> String {
        "ParquetTokenV2Extractor".to_string()
    }
}
