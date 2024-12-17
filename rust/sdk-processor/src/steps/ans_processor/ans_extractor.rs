use crate::{
    config::processor_config::ProcessorConfig, processors::ans_processor::AnsProcessorConfig,
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
        common::models::ans_models::{
            raw_ans_lookup_v2::{AnsLookupV2Convertible, CurrentAnsLookupV2Convertible},
            raw_ans_primary_name_v2::CurrentAnsPrimaryNameV2Convertible,
        },
        postgres::models::ans_models::{
            ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
            ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2},
            ans_primary_name_v2::CurrentAnsPrimaryNameV2,
        },
    },
    processors::ans_processor::parse_ans,
    utils::table_flags::TableFlags,
};

pub struct AnsExtractor
where
    Self: Sized + Send + 'static,
{
    deprecated_table_flags: TableFlags,
    config: AnsProcessorConfig,
}

impl AnsExtractor {
    pub fn new(
        deprecated_table_flags: TableFlags,
        config: ProcessorConfig,
    ) -> Result<Self, anyhow::Error> {
        let processor_config = match config {
            ProcessorConfig::AnsProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for ANS Processor: {:?}",
                    config
                ))
            },
        };

        Ok(Self {
            deprecated_table_flags,
            config: processor_config,
        })
    }
}

#[async_trait]
impl Processable for AnsExtractor {
    type Input = Vec<Transaction>;
    type Output = (
        Vec<CurrentAnsLookup>,
        Vec<AnsLookup>,
        Vec<CurrentAnsPrimaryName>,
        Vec<AnsPrimaryName>,
        Vec<CurrentAnsLookupV2>,
        Vec<AnsLookupV2>,
        Vec<CurrentAnsPrimaryNameV2>,
    );
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<
            TransactionContext<(
                Vec<CurrentAnsLookup>,
                Vec<AnsLookup>,
                Vec<CurrentAnsPrimaryName>,
                Vec<AnsPrimaryName>,
                Vec<CurrentAnsLookupV2>,
                Vec<AnsLookupV2>,
                Vec<CurrentAnsPrimaryNameV2>,
            )>,
        >,
        ProcessorError,
    > {
        let (
            mut all_current_ans_lookups,
            mut all_ans_lookups,
            mut all_current_ans_primary_names,
            mut all_ans_primary_names,
            raw_current_ans_lookups_v2,
            raw_ans_lookups_v2,
            raw_current_ans_primary_names_v2,
            _, // AnsPrimaryNameV2 is deprecated.
        ) = parse_ans(
            &input.data,
            self.config.ans_v1_primary_names_table_handle.clone(),
            self.config.ans_v1_name_records_table_handle.clone(),
            self.config.ans_v2_contract_address.clone(),
        );

        if self
            .deprecated_table_flags
            .contains(TableFlags::ANS_PRIMARY_NAME)
        {
            all_ans_primary_names.clear();
        }
        if self.deprecated_table_flags.contains(TableFlags::ANS_LOOKUP) {
            all_ans_lookups.clear();
        }
        if self
            .deprecated_table_flags
            .contains(TableFlags::CURRENT_ANS_LOOKUP)
        {
            all_current_ans_lookups.clear();
        }
        if self
            .deprecated_table_flags
            .contains(TableFlags::CURRENT_ANS_PRIMARY_NAME)
        {
            all_current_ans_primary_names.clear();
        }

        let postgres_current_ans_lookups_v2: Vec<CurrentAnsLookupV2> = raw_current_ans_lookups_v2
            .into_iter()
            .map(CurrentAnsLookupV2::from_raw)
            .collect();

        let postgres_ans_lookups_v2: Vec<AnsLookupV2> = raw_ans_lookups_v2
            .into_iter()
            .map(AnsLookupV2::from_raw)
            .collect();

        let postgres_current_ans_primary_names_v2: Vec<CurrentAnsPrimaryNameV2> =
            raw_current_ans_primary_names_v2
                .into_iter()
                .map(CurrentAnsPrimaryNameV2::from_raw)
                .collect();

        Ok(Some(TransactionContext {
            data: (
                all_current_ans_lookups,
                all_ans_lookups,
                all_current_ans_primary_names,
                all_ans_primary_names,
                postgres_current_ans_lookups_v2,
                postgres_ans_lookups_v2,
                postgres_current_ans_primary_names_v2,
            ),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for AnsExtractor {}

impl NamedStep for AnsExtractor {
    fn name(&self) -> String {
        "AnsExtractor".to_string()
    }
}
