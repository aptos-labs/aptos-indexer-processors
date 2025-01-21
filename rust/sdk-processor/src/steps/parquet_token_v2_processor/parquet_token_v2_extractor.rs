use crate::{
    parquet_processors::{ParquetTypeEnum, ParquetTypeStructs},
    utils::parquet_extractor_helper::add_to_map_if_opted_in_for_backfill,
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
        parquet::models::token_v2_models::{
            token_claims::CurrentTokenPendingClaim,
            v1_token_royalty::CurrentTokenRoyaltyV1,
            v2_token_activities::TokenActivityV2,
            v2_token_datas::{CurrentTokenDataV2, TokenDataV2},
            v2_token_metadata::CurrentTokenV2Metadata,
            v2_token_ownerships::{CurrentTokenOwnershipV2, TokenOwnershipV2},
        },
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
        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner: ahash::AHashMap<String, TableMetadataForToken> =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions.data);

        let (
            _collections_v2,
            raw_token_datas_v2,
            raw_token_ownerships_v2,
            _current_collections_v2,
            raw_current_token_datas_v2,
            raw_current_deleted_token_datas_v2,
            raw_current_token_ownerships_v2,
            raw_current_deleted_token_ownerships_v2,
            raw_token_activities_v2,
            raw_current_token_v2_metadata,
            raw_current_token_royalties_v1,
            raw_current_token_claims,
        ) = parse_v2_token(&transactions.data, &table_handle_to_owner, &mut None).await;

        let parquet_current_token_claims: Vec<CurrentTokenPendingClaim> = raw_current_token_claims
            .into_iter()
            .map(CurrentTokenPendingClaim::from_raw)
            .collect();

        let parquet_current_token_royalties_v1: Vec<CurrentTokenRoyaltyV1> =
            raw_current_token_royalties_v1
                .into_iter()
                .map(CurrentTokenRoyaltyV1::from_raw)
                .collect();

        let parquet_current_token_v2_metadata: Vec<CurrentTokenV2Metadata> =
            raw_current_token_v2_metadata
                .into_iter()
                .map(CurrentTokenV2Metadata::from_raw)
                .collect();

        let parquet_token_activities_v2: Vec<TokenActivityV2> = raw_token_activities_v2
            .into_iter()
            .map(TokenActivityV2::from_raw)
            .collect();

        let parquet_token_datas_v2: Vec<TokenDataV2> = raw_token_datas_v2
            .into_iter()
            .map(TokenDataV2::from_raw)
            .collect();

        let parquet_current_token_datas_v2: Vec<CurrentTokenDataV2> = raw_current_token_datas_v2
            .into_iter()
            .map(CurrentTokenDataV2::from_raw)
            .collect();

        let parquet_deleted_current_token_datss_v2: Vec<CurrentTokenDataV2> =
            raw_current_deleted_token_datas_v2
                .into_iter()
                .map(CurrentTokenDataV2::from_raw)
                .collect();

        let parquet_token_ownerships_v2: Vec<TokenOwnershipV2> = raw_token_ownerships_v2
            .into_iter()
            .map(TokenOwnershipV2::from_raw)
            .collect();

        let parquet_current_token_ownerships_v2: Vec<CurrentTokenOwnershipV2> =
            raw_current_token_ownerships_v2
                .into_iter()
                .map(CurrentTokenOwnershipV2::from_raw)
                .collect();

        let parquet_deleted_current_token_ownerships_v2: Vec<CurrentTokenOwnershipV2> =
            raw_current_deleted_token_ownerships_v2
                .into_iter()
                .map(CurrentTokenOwnershipV2::from_raw)
                .collect();

        // Print the size of each extracted data type
        debug!("Processed data sizes:");
        debug!(
            " - CurrentTokenPendingClaim: {}",
            parquet_current_token_claims.len()
        );
        debug!(
            " - CurrentTokenRoyaltyV1: {}",
            parquet_current_token_royalties_v1.len()
        );
        debug!(
            " - CurrentTokenV2Metadata: {}",
            parquet_current_token_v2_metadata.len()
        );
        debug!(" - TokenActivityV2: {}", parquet_token_activities_v2.len());
        debug!(" - TokenDataV2: {}", parquet_token_datas_v2.len());
        debug!(
            " - CurrentTokenDataV2: {}",
            parquet_current_token_datas_v2.len()
        );
        debug!(
            " - CurrentDeletedTokenDataV2: {}",
            parquet_deleted_current_token_datss_v2.len()
        );
        debug!(" - TokenOwnershipV2: {}", parquet_token_ownerships_v2.len());
        debug!(
            " - CurrentTokenOwnershipV2: {}",
            parquet_current_token_ownerships_v2.len()
        );
        debug!(
            " - CurrentDeletedTokenOwnershipV2: {}",
            parquet_deleted_current_token_ownerships_v2.len()
        );

        // We are merging these two tables, b/c they are essentially the same table
        let mut combined_current_token_datas_v2: Vec<CurrentTokenDataV2> = Vec::new();
        parquet_current_token_datas_v2
            .iter()
            .for_each(|x| combined_current_token_datas_v2.push(x.clone()));
        parquet_deleted_current_token_datss_v2
            .iter()
            .for_each(|x| combined_current_token_datas_v2.push(x.clone()));

        let mut merged_current_token_ownerships_v2: Vec<CurrentTokenOwnershipV2> = Vec::new();
        parquet_current_token_ownerships_v2
            .iter()
            .for_each(|x| merged_current_token_ownerships_v2.push(x.clone()));
        parquet_deleted_current_token_ownerships_v2
            .iter()
            .for_each(|x| merged_current_token_ownerships_v2.push(x.clone()));

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [
            (
                TableFlags::CURRENT_TOKEN_PENDING_CLAIMS,
                ParquetTypeEnum::CurrentTokenPendingClaims,
                ParquetTypeStructs::CurrentTokenPendingClaim(parquet_current_token_claims),
            ),
            (
                TableFlags::CURRENT_TOKEN_ROYALTY_V1,
                ParquetTypeEnum::CurrentTokenRoyaltiesV1,
                ParquetTypeStructs::CurrentTokenRoyaltyV1(parquet_current_token_royalties_v1),
            ),
            (
                TableFlags::CURRENT_TOKEN_V2_METADATA,
                ParquetTypeEnum::CurrentTokenV2Metadata,
                ParquetTypeStructs::CurrentTokenV2Metadata(parquet_current_token_v2_metadata),
            ),
            (
                TableFlags::TOKEN_ACTIVITIES_V2,
                ParquetTypeEnum::TokenActivitiesV2,
                ParquetTypeStructs::TokenActivityV2(parquet_token_activities_v2),
            ),
            (
                TableFlags::TOKEN_DATAS_V2,
                ParquetTypeEnum::TokenDatasV2,
                ParquetTypeStructs::TokenDataV2(parquet_token_datas_v2),
            ),
            (
                TableFlags::CURRENT_TOKEN_DATAS_V2,
                ParquetTypeEnum::CurrentTokenDatasV2,
                ParquetTypeStructs::CurrentTokenDataV2(combined_current_token_datas_v2),
            ),
            (
                TableFlags::TOKEN_OWNERSHIPS_V2,
                ParquetTypeEnum::TokenOwnershipsV2,
                ParquetTypeStructs::TokenOwnershipV2(parquet_token_ownerships_v2),
            ),
            (
                TableFlags::CURRENT_TOKEN_OWNERSHIPS_V2,
                ParquetTypeEnum::CurrentTokenOwnershipsV2,
                ParquetTypeStructs::CurrentTokenOwnershipV2(merged_current_token_ownerships_v2),
            ),
        ];

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
