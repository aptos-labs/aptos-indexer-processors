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
        common::models::object_models::raw_v2_objects::{
            CurrentObjectConvertible, ObjectConvertible,
        },
        parquet::models::object_models::v2_objects::{CurrentObject, Object},
    },
    processors::objects_processor::process_objects,
    utils::table_flags::TableFlags,
};
use std::collections::HashMap;

/// Extracts parquet data from transactions, allowing optional selection of specific tables.
pub struct ParquetObjectsExtractor
where
    Self: Processable + Send + Sized + 'static,
{
    pub opt_in_tables: TableFlags,
}

type ParquetTypeMap = HashMap<ParquetTypeEnum, ParquetTypeStructs>;

#[async_trait]
impl Processable for ParquetObjectsExtractor {
    type Input = Vec<Transaction>;
    type Output = ParquetTypeMap;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> anyhow::Result<Option<TransactionContext<ParquetTypeMap>>, ProcessorError> {
        let (raw_all_objects, raw_all_current_objects) =
            process_objects(transactions.data, &mut None).await;
        let parquet_objects: Vec<Object> =
            raw_all_objects.into_iter().map(Object::from_raw).collect();

        let parquet_current_objects: Vec<CurrentObject> = raw_all_current_objects
            .into_iter()
            .map(CurrentObject::from_raw)
            .collect();

        let mut map: HashMap<ParquetTypeEnum, ParquetTypeStructs> = HashMap::new();

        let data_types = [
            (
                TableFlags::OBJECTS,
                ParquetTypeEnum::Objects,
                ParquetTypeStructs::Object(parquet_objects),
            ),
            (
                TableFlags::CURRENT_OBJECTS,
                ParquetTypeEnum::CurrentObjects,
                ParquetTypeStructs::CurrentObject(parquet_current_objects),
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

impl AsyncStep for ParquetObjectsExtractor {}

impl NamedStep for ParquetObjectsExtractor {
    fn name(&self) -> String {
        "ParquetObjectsExtractor".to_string()
    }
}
