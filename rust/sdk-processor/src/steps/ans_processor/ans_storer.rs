use crate::{
    processors::ans_processor::AnsProcessorConfig,
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
    db::postgres::models::ans_models::{
        ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
        ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2, CurrentAnsPrimaryNameV2},
    },
    processors::ans_processor::{
        insert_ans_lookups_query, insert_ans_lookups_v2_query, insert_ans_primary_names_query,
        insert_current_ans_lookups_query, insert_current_ans_lookups_v2_query,
        insert_current_ans_primary_names_query, insert_current_ans_primary_names_v2_query,
    },
};

pub struct AnsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: AnsProcessorConfig,
}

impl AnsStorer {
    pub fn new(conn_pool: ArcDbPool, processor_config: AnsProcessorConfig) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for AnsStorer {
    type Input = (
        Vec<CurrentAnsLookup>,
        Vec<AnsLookup>,
        Vec<CurrentAnsPrimaryName>,
        Vec<AnsPrimaryName>,
        Vec<CurrentAnsLookupV2>,
        Vec<AnsLookupV2>,
        Vec<CurrentAnsPrimaryNameV2>,
    );
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(
            Vec<CurrentAnsLookup>,
            Vec<AnsLookup>,
            Vec<CurrentAnsPrimaryName>,
            Vec<AnsPrimaryName>,
            Vec<CurrentAnsLookupV2>,
            Vec<AnsLookupV2>,
            Vec<CurrentAnsPrimaryNameV2>,
        )>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (
            current_ans_lookups,
            ans_lookups,
            current_ans_primary_names,
            ans_primary_names,
            current_ans_lookups_v2,
            ans_lookups_v2,
            current_ans_primary_names_v2,
        ) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.default.per_table_chunk_sizes.clone();

        let cal = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_lookups_query,
            &current_ans_lookups,
            get_config_table_chunk_size::<CurrentAnsLookup>(
                "current_ans_lookup",
                &per_table_chunk_sizes,
            ),
        );
        let al = execute_in_chunks(
            self.conn_pool.clone(),
            insert_ans_lookups_query,
            &ans_lookups,
            get_config_table_chunk_size::<AnsLookup>("ans_lookup", &per_table_chunk_sizes),
        );
        let capn = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_primary_names_query,
            &current_ans_primary_names,
            get_config_table_chunk_size::<CurrentAnsPrimaryName>(
                "current_ans_primary_name",
                &per_table_chunk_sizes,
            ),
        );
        let apn = execute_in_chunks(
            self.conn_pool.clone(),
            insert_ans_primary_names_query,
            &ans_primary_names,
            get_config_table_chunk_size::<AnsPrimaryName>(
                "ans_primary_name",
                &per_table_chunk_sizes,
            ),
        );
        let cal_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_lookups_v2_query,
            &current_ans_lookups_v2,
            get_config_table_chunk_size::<CurrentAnsLookupV2>(
                "current_ans_lookup_v2",
                &per_table_chunk_sizes,
            ),
        );
        let al_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_ans_lookups_v2_query,
            &ans_lookups_v2,
            get_config_table_chunk_size::<AnsLookupV2>("ans_lookup_v2", &per_table_chunk_sizes),
        );
        let capn_v2 = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_ans_primary_names_v2_query,
            &current_ans_primary_names_v2,
            get_config_table_chunk_size::<CurrentAnsPrimaryNameV2>(
                "current_ans_primary_name_v2",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(cal, al, capn, apn, cal_v2, al_v2, capn_v2)?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsStorer {}

impl NamedStep for AnsStorer {
    fn name(&self) -> String {
        "AnsStorer".to_string()
    }
}
