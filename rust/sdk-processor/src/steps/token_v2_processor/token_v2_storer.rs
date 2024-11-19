use crate::{
    processors::token_v2_processor::TokenV2ProcessorConfig,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
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
    processors::token_v2_processor::{
        insert_collections_v2_query, insert_current_collections_v2_query,
        insert_current_deleted_token_datas_v2_query,
        insert_current_deleted_token_ownerships_v2_query, insert_current_token_claims_query,
        insert_current_token_datas_v2_query, insert_current_token_ownerships_v2_query,
        insert_current_token_royalties_v1_query, insert_current_token_v2_metadatas_query,
        insert_token_activities_v2_query, insert_token_datas_v2_query,
        insert_token_ownerships_v2_query,
    },
};

pub struct TokenV2Storer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: TokenV2ProcessorConfig,
}

impl TokenV2Storer {
    pub fn new(conn_pool: ArcDbPool, processor_config: TokenV2ProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for TokenV2Storer {
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
        let (
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_datas_v2,
            current_deleted_token_datas_v2,
            current_token_ownerships_v2,
            current_deleted_token_ownerships_v2,
            token_activities_v2,
            current_token_v2_metadata,
            current_token_royalties_v1,
            current_token_claims,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> = self
            .processor_config
            .default_config
            .per_table_chunk_sizes
            .clone();

        let coll_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_collections_v2_query,
            &collections_v2,
            get_config_table_chunk_size::<CollectionV2>("collections_v2", &per_table_chunk_sizes),
        );
        let td_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_token_datas_v2_query,
            &token_datas_v2,
            get_config_table_chunk_size::<TokenDataV2>("token_datas_v2", &per_table_chunk_sizes),
        );
        let to_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_token_ownerships_v2_query,
            &token_ownerships_v2,
            get_config_table_chunk_size::<TokenOwnershipV2>(
                "token_ownerships_v2",
                &per_table_chunk_sizes,
            ),
        );
        let cc_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_collections_v2_query,
            &current_collections_v2,
            get_config_table_chunk_size::<CurrentCollectionV2>(
                "current_collections_v2",
                &per_table_chunk_sizes,
            ),
        );
        let ctd_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_token_datas_v2_query,
            &current_token_datas_v2,
            get_config_table_chunk_size::<CurrentTokenDataV2>(
                "current_token_datas_v2",
                &per_table_chunk_sizes,
            ),
        );
        let cdtd_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_deleted_token_datas_v2_query,
            &current_deleted_token_datas_v2,
            get_config_table_chunk_size::<CurrentTokenDataV2>(
                "current_token_datas_v2",
                &per_table_chunk_sizes,
            ),
        );
        let cto_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_token_ownerships_v2_query,
            &current_token_ownerships_v2,
            get_config_table_chunk_size::<CurrentTokenOwnershipV2>(
                "current_token_ownerships_v2",
                &per_table_chunk_sizes,
            ),
        );
        let cdto_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_deleted_token_ownerships_v2_query,
            &current_deleted_token_ownerships_v2,
            get_config_table_chunk_size::<CurrentTokenOwnershipV2>(
                "current_token_ownerships_v2",
                &per_table_chunk_sizes,
            ),
        );
        let ta_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_token_activities_v2_query,
            &token_activities_v2,
            get_config_table_chunk_size::<TokenActivityV2>(
                "token_activities_v2",
                &per_table_chunk_sizes,
            ),
        );
        let ct_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_token_v2_metadatas_query,
            &current_token_v2_metadata,
            get_config_table_chunk_size::<CurrentTokenV2Metadata>(
                "current_token_v2_metadata",
                &per_table_chunk_sizes,
            ),
        );
        let ctr_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_token_royalties_v1_query,
            &current_token_royalties_v1,
            get_config_table_chunk_size::<CurrentTokenRoyaltyV1>(
                "current_token_royalty_v1",
                &per_table_chunk_sizes,
            ),
        );
        let ctc_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_token_claims_query,
            &current_token_claims,
            get_config_table_chunk_size::<CurrentTokenPendingClaim>(
                "current_token_pending_claims",
                &per_table_chunk_sizes,
            ),
        );

        let (
            coll_v2_res,
            td_v2_res,
            to_v2_res,
            cc_v2_res,
            ctd_v2_res,
            cdtd_v2_res,
            cto_v2_res,
            cdto_v2_res,
            ta_v2_res,
            ct_v2_res,
            ctr_v1_res,
            ctc_v1_res,
        ) = tokio::join!(
            coll_v2, td_v2, to_v2, cc_v2, ctd_v2, cdtd_v2, cto_v2, cdto_v2, ta_v2, ct_v2, ctr_v1,
            ctc_v1
        );

        for res in [
            coll_v2_res,
            td_v2_res,
            to_v2_res,
            cc_v2_res,
            ctd_v2_res,
            cdtd_v2_res,
            cto_v2_res,
            cdto_v2_res,
            ta_v2_res,
            ct_v2_res,
            ctr_v1_res,
            ctc_v1_res,
        ] {
            match res {
                Ok(_) => {},
                Err(e) => {
                    return Err(ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })
                },
            }
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for TokenV2Storer {}

impl NamedStep for TokenV2Storer {
    fn name(&self) -> String {
        "TokenV2Storer".to_string()
    }
}
