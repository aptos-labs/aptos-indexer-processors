use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use processor::db::{
    parquet::models::default_models::{
        parquet_move_modules::MoveModule, parquet_move_resources::MoveResource,
        parquet_move_tables::TableItem, parquet_transactions::Transaction as ParquetTransaction,
        parquet_write_set_changes::WriteSetChangeModel,
    },
    postgres::models::events_models::parquet_events::Event,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

pub mod parquet_default_processor;
pub mod parquet_events_processor;

/// Enum representing the different types of Parquet files that can be processed.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Display, EnumIter)]
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(
        derive(
            strum::EnumVariantNames,
            Deserialize,
            Serialize,
            strum::IntoStaticStr,
            strum::Display,
            clap::ValueEnum
        ),
        name(ParquetTypeName),
        strum(serialize_all = "snake_case")
    )
)]
pub enum ParquetTypeEnum {
    MoveResource,
    WriteSetChange,
    Transaction,
    TableItem,
    MoveModule,
    Event,
}

#[derive(Clone, Debug, strum::EnumDiscriminants)]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    derive(
        Deserialize,
        Serialize,
        strum::EnumVariantNames,
        strum::IntoStaticStr,
        strum::Display,
        clap::ValueEnum
    ),
    name(ParquetTypeStructName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum ParquetTypeStructs {
    MoveResource(Vec<MoveResource>),
    WriteSetChange(Vec<WriteSetChangeModel>),
    Transaction(Vec<ParquetTransaction>),
    TableItem(Vec<TableItem>),
    MoveModule(Vec<MoveModule>),
    Event(Vec<Event>),
}

impl ParquetTypeStructs {
    pub fn default_for_type(parquet_type: &ParquetTypeEnum) -> Self {
        match parquet_type {
            ParquetTypeEnum::MoveResource => ParquetTypeStructs::MoveResource(Vec::new()),
            ParquetTypeEnum::WriteSetChange => ParquetTypeStructs::WriteSetChange(Vec::new()),
            ParquetTypeEnum::Transaction => ParquetTypeStructs::Transaction(Vec::new()),
            ParquetTypeEnum::TableItem => ParquetTypeStructs::TableItem(Vec::new()),
            ParquetTypeEnum::MoveModule => ParquetTypeStructs::MoveModule(Vec::new()),
            ParquetTypeEnum::Event => ParquetTypeStructs::Event(Vec::new()),
        }
    }

    pub fn get_table_name(&self) -> &'static str {
        match self {
            ParquetTypeStructs::MoveResource(_) => "move_resources",
            ParquetTypeStructs::WriteSetChange(_) => "write_set_changes",
            ParquetTypeStructs::Transaction(_) => "transactions",
            ParquetTypeStructs::TableItem(_) => "table_items",
            ParquetTypeStructs::MoveModule(_) => "move_modules",
            ParquetTypeStructs::Event(_) => "events",
        }
    }

    pub fn calculate_size(&self) -> usize {
        match self {
            ParquetTypeStructs::MoveResource(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::WriteSetChange(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::Transaction(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::TableItem(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::MoveModule(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::Event(data) => allocative::size_of_unique(data),
        }
    }

    /// Appends data to the current buffer within each ParquetTypeStructs variant.
    pub fn append(&mut self, other: ParquetTypeStructs) -> Result<(), ProcessorError> {
        match (self, other) {
            (ParquetTypeStructs::MoveResource(buf), ParquetTypeStructs::MoveResource(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (
                ParquetTypeStructs::WriteSetChange(buf),
                ParquetTypeStructs::WriteSetChange(mut data),
            ) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::Transaction(buf), ParquetTypeStructs::Transaction(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::TableItem(buf), ParquetTypeStructs::TableItem(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::MoveModule(buf), ParquetTypeStructs::MoveModule(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            _ => Err(ProcessorError::ProcessError {
                message: "Mismatched buffer types in append operation".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::VariantNames;

    /// This test exists to make sure that when a new processor is added, it is added
    /// to both Processor and ProcessorConfig.
    ///
    /// To make sure this passes, make sure the variants are in the same order
    /// (lexicographical) and the names match.
    #[test]
    fn test_parquet_type_names_complete() {
        assert_eq!(ParquetTypeStructName::VARIANTS, ParquetTypeName::VARIANTS);
    }
}
