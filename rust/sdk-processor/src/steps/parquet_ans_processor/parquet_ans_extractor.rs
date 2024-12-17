use crate::{
    parquet_processors::{
        parquet_ans_processor::ParquetAnsProcessorConfig, ParquetTypeEnum, ParquetTypeStructs,
    },
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
        common::models::ans_models::{
            raw_ans_lookup_v2::{AnsLookupV2Convertible, CurrentAnsLookupV2Convertible},
            raw_ans_primary_name_v2::{
                AnsPrimaryNameV2Convertible, CurrentAnsPrimaryNameV2Convertible,
            },
        },
        parquet::models::ans_models::{
            ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2},
            ans_primary_name_v2::{AnsPrimaryNameV2, CurrentAnsPrimaryNameV2},
        },
    },
    processors::ans_processor::parse_ans,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetAnsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub ans_config: ParquetAnsProcessorConfig,
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetAnsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (
            _,
            _,
            _,
            _,
            raw_current_ans_lookups_v2,
            raw_ans_lookups_v2,
            raw_current_ans_primary_names_v2,
            raw_ans_primary_name_v2,
        ) = parse_ans(
            &input.data,
            self.ans_config.ans_v1_primary_names_table_handle.clone(),
            self.ans_config.ans_v1_name_records_table_handle.clone(),
            self.ans_config.ans_v2_contract_address.clone(),
        );

        let parquet_ans_lookup_v2: Vec<AnsLookupV2> = raw_ans_lookups_v2
            .into_iter()
            .map(AnsLookupV2::from_raw)
            .collect();

        let parquet_current_ans_lookup_v2: Vec<CurrentAnsLookupV2> = raw_current_ans_lookups_v2
            .into_iter()
            .map(CurrentAnsLookupV2::from_raw)
            .collect();

        let parquet_current_ans_primary_name_v2: Vec<CurrentAnsPrimaryNameV2> =
            raw_current_ans_primary_names_v2
                .into_iter()
                .map(CurrentAnsPrimaryNameV2::from_raw)
                .collect();

        let parquet_ans_primary_name_v2: Vec<AnsPrimaryNameV2> = raw_ans_primary_name_v2
            .into_iter()
            .map(AnsPrimaryNameV2::from_raw)
            .collect();

        let mut map = HashMap::new();

        // Array of tuples for each data type and its corresponding enum variant and flag
        let data_types = [
            (
                TableFlags::ANS_LOOKUP_V2,
                ParquetTypeEnum::AnsLookupV2,
                ParquetTypeStructs::AnsLookupV2(parquet_ans_lookup_v2),
            ),
            (
                TableFlags::CURRENT_ANS_LOOKUP_V2,
                ParquetTypeEnum::CurrentAnsLookupV2,
                ParquetTypeStructs::CurrentAnsLookupV2(parquet_current_ans_lookup_v2),
            ),
            (
                TableFlags::CURRENT_ANS_PRIMARY_NAME_V2,
                ParquetTypeEnum::CurrentAnsPrimaryNameV2,
                ParquetTypeStructs::CurrentAnsPrimaryNameV2(parquet_current_ans_primary_name_v2),
            ),
            (
                TableFlags::ANS_PRIMARY_NAME_V2,
                ParquetTypeEnum::AnsPrimaryNameV2,
                ParquetTypeStructs::AnsPrimaryNameV2(parquet_ans_primary_name_v2),
            ),
        ];

        // Populate the map based on opt-in tables
        add_to_map_if_opted_in_for_backfill(self.opt_in_tables, &mut map, data_types.to_vec());

        Ok(Some(TransactionContext {
            data: map,
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for ParquetAnsExtractor {}

impl NamedStep for ParquetAnsExtractor {
    fn name(&self) -> String {
        "ParquetAnsExtractor".to_string()
    }
}
