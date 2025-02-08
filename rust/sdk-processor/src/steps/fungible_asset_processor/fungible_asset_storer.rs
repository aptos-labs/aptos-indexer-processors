use crate::{
    config::processor_config::DefaultProcessorConfig,
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
    db::postgres::models::{
        coin_models::coin_supply::CoinSupply,
        fungible_asset_models::{
            v2_fungible_asset_activities::FungibleAssetActivity,
            v2_fungible_asset_balances::{
                CurrentUnifiedFungibleAssetBalance, FungibleAssetBalance,
            },
            v2_fungible_asset_to_coin_mappings::FungibleAssetToCoinMapping,
            v2_fungible_metadata::FungibleAssetMetadataModel,
        },
    },
    processors::fungible_asset_processor::{
        insert_coin_supply_query, insert_current_unified_fungible_asset_balances_v1_query,
        insert_current_unified_fungible_asset_balances_v2_query,
        insert_fungible_asset_activities_query, insert_fungible_asset_metadata_query,
        insert_fungible_asset_to_coin_mappings_query,
    },
    utils::table_flags::TableFlags,
};

pub struct FungibleAssetStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: DefaultProcessorConfig,
    deprecated_tables: TableFlags,
}

impl FungibleAssetStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: DefaultProcessorConfig,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
            deprecated_tables,
        }
    }
}

#[async_trait]
impl Processable for FungibleAssetStorer {
    type Input = (
        Vec<FungibleAssetActivity>,
        Vec<FungibleAssetMetadataModel>,
        Vec<FungibleAssetBalance>,
        (
            Vec<CurrentUnifiedFungibleAssetBalance>,
            Vec<CurrentUnifiedFungibleAssetBalance>,
        ),
        Vec<CoinSupply>,
        Vec<FungibleAssetToCoinMapping>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<FungibleAssetActivity>,
            Vec<FungibleAssetMetadataModel>,
            Vec<FungibleAssetBalance>,
            (
                Vec<CurrentUnifiedFungibleAssetBalance>,
                Vec<CurrentUnifiedFungibleAssetBalance>,
            ),
            Vec<CoinSupply>,
            Vec<FungibleAssetToCoinMapping>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (
            mut fungible_asset_activities,
            mut fungible_asset_metadata,
            mut fungible_asset_balances,
            (mut current_unified_fab_v1, mut current_unified_fab_v2),
            mut coin_supply,
            fa_to_coin_mappings,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();
        // if flag turned on we need to not include any value in the table
        if self
            .deprecated_tables
            .contains(TableFlags::FUNGIBLE_ASSET_BALANCES)
        {
            fungible_asset_balances.clear();
        }
        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES)
        {
            current_unified_fab_v1.clear();
            current_unified_fab_v2.clear();
        }
        if self.deprecated_tables.contains(TableFlags::COIN_SUPPLY) {
            coin_supply.clear();
        }

        if self
            .deprecated_tables
            .contains(TableFlags::FUNGIBLE_ASSET_ACTIVITIES)
        {
            fungible_asset_activities.clear();
        }

        if self
            .deprecated_tables
            .contains(TableFlags::FUNGIBLE_ASSET_METADATA)
        {
            fungible_asset_metadata.clear();
        }

        let faa = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_activities_query,
            &fungible_asset_activities,
            get_config_table_chunk_size::<FungibleAssetActivity>(
                "fungible_asset_activities",
                &per_table_chunk_sizes,
            ),
        );
        let fam = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_metadata_query,
            &fungible_asset_metadata,
            get_config_table_chunk_size::<FungibleAssetMetadataModel>(
                "fungible_asset_metadata",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v1_query,
            &current_unified_fab_v1,
            get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v2_query,
            &current_unified_fab_v2,
            get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cs = execute_in_chunks(
            self.conn_pool.clone(),
            insert_coin_supply_query,
            &coin_supply,
            get_config_table_chunk_size::<CoinSupply>("coin_supply", &per_table_chunk_sizes),
        );
        let fatcm = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_to_coin_mappings_query,
            &fa_to_coin_mappings,
            get_config_table_chunk_size::<FungibleAssetToCoinMapping>(
                "fungible_asset_to_coin_mappings",
                &per_table_chunk_sizes,
            ),
        );
        let (faa_res, fam_res, cufab1_res, cufab2_res, cs_res, fatcm_res) =
            tokio::join!(faa, fam, cufab_v1, cufab_v2, cs, fatcm);
        for res in [faa_res, fam_res, cufab1_res, cufab2_res, cs_res, fatcm_res] {
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

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}
