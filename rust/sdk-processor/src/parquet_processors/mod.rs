use processor::db::common::models::default_models::{
    parquet_move_modules::MoveModule, parquet_move_resources::MoveResource,
    parquet_move_tables::TableItem, parquet_transactions::Transaction as ParquetTransaction,
    parquet_write_set_changes::WriteSetChangeModel,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

pub mod parquet_default_processor;

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
}

impl ParquetTypeStructs {
    pub fn default_for_type(parquet_type: &ParquetTypeEnum) -> Self {
        match parquet_type {
            ParquetTypeEnum::MoveResource => ParquetTypeStructs::MoveResource(Vec::new()),
            ParquetTypeEnum::WriteSetChange => ParquetTypeStructs::WriteSetChange(Vec::new()),
            ParquetTypeEnum::Transaction => ParquetTypeStructs::Transaction(Vec::new()),
            ParquetTypeEnum::TableItem => ParquetTypeStructs::TableItem(Vec::new()),
            ParquetTypeEnum::MoveModule => ParquetTypeStructs::MoveModule(Vec::new()),
        }
    }

    pub fn get_table_name(&self) -> &'static str {
        match self {
            ParquetTypeStructs::MoveResource(_) => "move_resources",
            ParquetTypeStructs::WriteSetChange(_) => "write_set_changes",
            ParquetTypeStructs::Transaction(_) => "parquet_transactions",
            ParquetTypeStructs::TableItem(_) => "table_items",
            ParquetTypeStructs::MoveModule(_) => "move_modules",
        }
    }
}

pub trait ParquetStruct {}

impl ParquetStruct for MoveResource {}
impl ParquetStruct for WriteSetChangeModel {}
impl ParquetStruct for ParquetTransaction {}
impl ParquetStruct for TableItem {}
impl ParquetStruct for MoveModule {}

impl ParquetTypeStructs {
    pub fn get_type(&self) -> ParquetTypeEnum {
        match self {
            ParquetTypeStructs::MoveResource(_) => ParquetTypeEnum::MoveResource,
            ParquetTypeStructs::WriteSetChange(_) => ParquetTypeEnum::WriteSetChange,
            ParquetTypeStructs::Transaction(_) => ParquetTypeEnum::Transaction,
            ParquetTypeStructs::TableItem(_) => ParquetTypeEnum::TableItem,
            ParquetTypeStructs::MoveModule(_) => ParquetTypeEnum::MoveModule,
        }
    }

    /// Get a vector of trait object references to the inner structs
    pub fn get_structs(&self) -> Vec<&dyn ParquetStruct> {
        match self {
            ParquetTypeStructs::MoveResource(v) => {
                v.iter().map(|s| s as &dyn ParquetStruct).collect()
            },
            ParquetTypeStructs::WriteSetChange(v) => {
                v.iter().map(|s| s as &dyn ParquetStruct).collect()
            },
            ParquetTypeStructs::Transaction(v) => {
                v.iter().map(|s| s as &dyn ParquetStruct).collect()
            },
            ParquetTypeStructs::TableItem(v) => v.iter().map(|s| s as &dyn ParquetStruct).collect(),
            ParquetTypeStructs::MoveModule(v) => {
                v.iter().map(|s| s as &dyn ParquetStruct).collect()
            },
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
