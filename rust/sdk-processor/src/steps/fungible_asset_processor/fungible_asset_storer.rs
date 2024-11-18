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
                CurrentFungibleAssetBalance, CurrentUnifiedFungibleAssetBalance,
                FungibleAssetBalance,
            },
            v2_fungible_metadata::FungibleAssetMetadataModel,
        },
    },
    processors::fungible_asset_processor::{
        insert_coin_supply_query, insert_current_fungible_asset_balances_query,
        insert_current_unified_fungible_asset_balances_v1_query,
        insert_current_unified_fungible_asset_balances_v2_query,
        insert_fungible_asset_activities_query, insert_fungible_asset_balances_query,
        insert_fungible_asset_metadata_query,
    },
    worker::TableFlags,
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
        Vec<CurrentFungibleAssetBalance>,
        Vec<CurrentUnifiedFungibleAssetBalance>,
        Vec<CoinSupply>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<FungibleAssetActivity>,
            Vec<FungibleAssetMetadataModel>,
            Vec<FungibleAssetBalance>,
            Vec<CurrentFungibleAssetBalance>,
            Vec<CurrentUnifiedFungibleAssetBalance>,
            Vec<CoinSupply>,
        )>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (
            fungible_asset_activities,
            fungible_asset_metadata,
            mut fungible_asset_balances,
            mut current_fungible_asset_balances,
            current_unified_fungible_asset_balances,
            mut coin_supply,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();
        // if flag turned on we need to not include any value in the table
        let (unified_coin_balances, unified_fa_balances): (Vec<_>, Vec<_>) = if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES)
        {
            (vec![], vec![])
        } else {
            // Basically we need to split the current unified balances into v1 and v2
            // by looking at whether asset_type_v2 is null (must be v1 if it's null)
            // Note, we can't check asset_type_v1 is none because we're now filling asset_type_v1
            // for certain assets
            current_unified_fungible_asset_balances
                .into_iter()
                .partition(|x| x.asset_type_v2.is_none())
        };

        if self
            .deprecated_tables
            .contains(TableFlags::FUNGIBLE_ASSET_BALANCES)
        {
            fungible_asset_balances.clear();
        }

        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_FUNGIBLE_ASSET_BALANCES)
        {
            current_fungible_asset_balances.clear();
        }

        if self.deprecated_tables.contains(TableFlags::COIN_SUPPLY) {
            coin_supply.clear();
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
        let fab = execute_in_chunks(
            self.conn_pool.clone(),
            insert_fungible_asset_balances_query,
            &fungible_asset_balances,
            get_config_table_chunk_size::<FungibleAssetBalance>(
                "fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cfab = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_fungible_asset_balances_query,
            &current_fungible_asset_balances,
            get_config_table_chunk_size::<CurrentFungibleAssetBalance>(
                "current_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v1 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v1_query,
            &unified_coin_balances,
            get_config_table_chunk_size::<CurrentUnifiedFungibleAssetBalance>(
                "current_unified_fungible_asset_balances",
                &per_table_chunk_sizes,
            ),
        );
        let cufab_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_unified_fungible_asset_balances_v2_query,
            &unified_fa_balances,
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
        let (faa_res, fam_res, fab_res, cfab_res, cufab1_res, cufab2_res, cs_res) =
            tokio::join!(faa, fam, fab, cfab, cufab_v1, cufab_v2, cs);
        for res in [
            faa_res, fam_res, fab_res, cfab_res, cufab1_res, cufab2_res, cs_res,
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

impl AsyncStep for FungibleAssetStorer {}

impl NamedStep for FungibleAssetStorer {
    fn name(&self) -> String {
        "FungibleAssetStorer".to_string()
    }
}
